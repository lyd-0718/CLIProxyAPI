// Package trace implements the built-in durable Session/Turn recorder.
// Trace events are append-only NDJSON records with a small SQLite index.
package trace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	clipsession "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/session"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

// ContextKey is the Gin context key used by the middleware and usage hooks.
const ContextKey = "__cpa_trace_state__"

const schemaVersion = 1

// Event is the stable wire format for one NDJSON line.
type Event struct {
	SchemaVersion int             `json:"schema_version"`
	EventID       string          `json:"event_id"`
	Timestamp     time.Time       `json:"timestamp"`
	SessionID     string          `json:"session_id"`
	SessionSource string          `json:"session_id_source"`
	TurnID        string          `json:"turn_id"`
	RequestID     string          `json:"request_id,omitempty"`
	TraceID       string          `json:"trace_id,omitempty"`
	Sequence      int64           `json:"sequence"`
	Kind          string          `json:"event"`
	Incomplete    bool            `json:"incomplete,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

// UpstreamRequest is the raw outbound provider request captured by the executor helpers.
type UpstreamRequest struct {
	URL       string      `json:"url"`
	Method    string      `json:"method"`
	Headers   http.Header `json:"headers,omitempty"`
	Body      []byte      `json:"body,omitempty"`
	Provider  string      `json:"provider,omitempty"`
	AuthID    string      `json:"auth_id,omitempty"`
	AuthLabel string      `json:"auth_label,omitempty"`
	AuthType  string      `json:"auth_type,omitempty"`
	AuthValue string      `json:"auth_value,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// UpstreamResponse describes one provider response observation.
type UpstreamResponse struct {
	Status  int         `json:"status"`
	Headers http.Header `json:"headers,omitempty"`
	Body    []byte      `json:"body,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type contextGetter interface {
	Get(string) (any, bool)
}

// State returns the active trace state carried by a CPA execution context.
func StateFromContext(ctx context.Context) *State {
	if ctx == nil {
		return nil
	}
	if state, ok := ctx.Value(ContextKey).(*State); ok && state != nil {
		return state
	}
	if carrier, ok := ctx.Value("gin").(contextGetter); ok && carrier != nil {
		if value, exists := carrier.Get(ContextKey); exists {
			if state, ok := value.(*State); ok {
				return state
			}
		}
	}
	return nil
}

// State tracks one downstream request and its Turn. It is safe for concurrent
// upstream and response goroutines.
type State struct {
	recorder      *Recorder
	mu            sync.Mutex
	sequence      int64
	finished      bool
	incomplete    bool
	requestID     string
	traceID       string
	sessionID     string
	sessionSource string
	turnID        string
	startedAt     time.Time
	requestMethod string
	requestPath   string
	requestHeader http.Header
	requestBody   []byte
	response      bytes.Buffer
	status        int
	responseHeads http.Header
	firstByte     time.Time
	usage         *coreusage.Record
}

// NewState starts a trace for a downstream request.
func NewState(recorder *Recorder, req *http.Request, requestBody []byte) *State {
	if recorder == nil || req == nil {
		return nil
	}
	requestID := logging.GetRequestID(req.Context())
	if requestID == "" {
		requestID = logging.GenerateRequestID()
	}
	sessionID, source := resolveSessionID(req, requestBody, requestID)
	state := &State{
		recorder:      recorder,
		requestID:     requestID,
		sessionID:     sessionID,
		sessionSource: source,
		turnID:        uuid.NewString(),
		startedAt:     time.Now(),
		requestMethod: req.Method,
		requestPath:   req.URL.Path,
		requestHeader: cloneHeader(req.Header),
		requestBody:   bytes.Clone(requestBody),
		responseHeads: make(http.Header),
	}
	return state
}

// IDs exposes the identifiers for management and helper integrations.
func (s *State) IDs() (sessionID, turnID, requestID, traceID string) {
	if s == nil {
		return "", "", "", ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionID, s.turnID, s.requestID, s.traceID
}

// SetTraceID stores the CPA credential-selection trace ID once it becomes available.
func (s *State) SetTraceID(traceID string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.traceID = strings.TrimSpace(traceID)
	s.mu.Unlock()
}

func (s *State) append(kind string, payload any) error {
	if s == nil || s.recorder == nil {
		return nil
	}
	raw, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		return errMarshal
	}
	s.mu.Lock()
	s.sequence++
	event := Event{
		SchemaVersion: schemaVersion,
		EventID:       uuid.NewString(),
		Timestamp:     time.Now(),
		SessionID:     s.sessionID,
		SessionSource: s.sessionSource,
		TurnID:        s.turnID,
		RequestID:     s.requestID,
		TraceID:       s.traceID,
		Sequence:      s.sequence,
		Kind:          strings.TrimSpace(kind),
		Incomplete:    s.incomplete,
		Payload:       raw,
	}
	s.mu.Unlock()
	if errAppend := s.recorder.Append(event); errAppend != nil {
		s.mu.Lock()
		s.incomplete = true
		s.mu.Unlock()
		return errAppend
	}
	return nil
}

// markIncomplete records that the request could not produce a complete trace.
// The API request is still allowed to finish; management views expose this
// state so operators do not mistake a partial capture for a complete one.
func (s *State) markIncomplete() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.incomplete = true
	s.mu.Unlock()
}

// Begin writes the downstream request event.
func (s *State) Begin() error {
	if s == nil {
		return nil
	}
	if err := s.append("request.input", map[string]any{
		"method":  s.requestMethod,
		"path":    s.requestPath,
		"headers": s.requestHeader,
		"body":    rawPayload(s.requestBody),
	}); err != nil {
		return err
	}
	// Tool calls, tool results, and reasoning can be present in the incoming
	// conversation as well as in the model response, so inspect both sides.
	emitSemanticEvents(s, s.requestBody)
	return nil
}

// AppendDownstreamChunk stores a response chunk and tracks the first byte.
func (s *State) AppendDownstreamChunk(data []byte) {
	if s == nil || len(data) == 0 {
		return
	}
	s.mu.Lock()
	if s.firstByte.IsZero() {
		s.firstByte = time.Now()
	}
	_, _ = s.response.Write(data)
	s.mu.Unlock()
	if s.recorder != nil && s.recorder.recordStreamChunks {
		_ = s.append("stream.chunk", map[string]any{"raw": rawPayload(data)})
	}
}

// SetResponseMetadata records downstream status and headers.
func (s *State) SetResponseMetadata(status int, headers http.Header) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if status > 0 {
		s.status = status
	}
	s.responseHeads = cloneHeader(headers)
	s.mu.Unlock()
}

// Finish writes response, extracted reasoning/tool events, and completion.
func (s *State) Finish(status int, headers http.Header) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.finished {
		s.mu.Unlock()
		return
	}
	s.finished = true
	if status > 0 {
		s.status = status
	}
	s.responseHeads = cloneHeader(headers)
	body := bytes.Clone(s.response.Bytes())
	incomplete := s.incomplete
	started := s.startedAt
	firstByte := s.firstByte
	statusCode := s.status
	responseHeaders := cloneHeader(s.responseHeads)
	s.mu.Unlock()

	if statusCode >= http.StatusBadRequest {
		_ = s.append("turn.error", map[string]any{
			"status":  statusCode,
			"headers": responseHeaders,
			"body":    rawPayload(body),
		})
	}
	_ = s.append("model.output", map[string]any{
		"status":           statusCode,
		"headers":          responseHeaders,
		"body":             rawPayload(body),
		"first_byte_at":    firstByte,
		"latency_ms":       time.Since(started).Milliseconds(),
		"failed":           statusCode >= http.StatusBadRequest,
		"trace_incomplete": incomplete,
	})
	emitSemanticEvents(s, body)
	_ = s.append("turn.completed", map[string]any{
		"status":     statusCode,
		"failed":     statusCode >= http.StatusBadRequest,
		"incomplete": incomplete,
		"latency_ms": time.Since(started).Milliseconds(),
		"ttft_ms":    durationMillis(started, firstByte),
	})
}

// RecordUsage attaches the canonical CPA usage record to this Turn.
func (s *State) RecordUsage(record coreusage.Record) {
	if s == nil {
		return
	}
	s.mu.Lock()
	copyRecord := record
	s.usage = &copyRecord
	s.mu.Unlock()
	_ = s.append("usage", usagePayload(record))
}

func usagePayload(record coreusage.Record) map[string]any {
	return map[string]any{
		"provider":              record.Provider,
		"executor_type":         record.ExecutorType,
		"model":                 record.Model,
		"alias":                 record.Alias,
		"api_key":               record.APIKey,
		"auth_id":               record.AuthID,
		"auth_index":            record.AuthIndex,
		"auth_type":             record.AuthType,
		"source":                record.Source,
		"reasoning_effort":      record.ReasoningEffort,
		"requested_at":          record.RequestedAt,
		"latency_ms":            record.Latency.Milliseconds(),
		"ttft_ms":               record.TTFT.Milliseconds(),
		"failed":                record.Failed,
		"failure":               record.Fail,
		"input_tokens":          record.Detail.InputTokens,
		"output_tokens":         record.Detail.OutputTokens,
		"reasoning_tokens":      record.Detail.ReasoningTokens,
		"cached_tokens":         record.Detail.CachedTokens,
		"cache_read_tokens":     record.Detail.CacheReadTokens,
		"cache_creation_tokens": record.Detail.CacheCreationTokens,
		"total_tokens":          record.Detail.TotalTokens,
	}
}

// RecordUsageForContext is called by the canonical UsageReporter.
func RecordUsageForContext(ctx context.Context, record coreusage.Record) {
	if state := StateFromContext(ctx); state != nil {
		if traceID := traceIDFromContext(ctx); traceID != "" {
			state.mu.Lock()
			state.traceID = traceID
			state.mu.Unlock()
		}
		state.RecordUsage(record)
	}
}

func traceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if carrier, ok := ctx.Value("gin").(*gin.Context); ok && carrier != nil {
		return strings.TrimSpace(logging.GetGinCPATraceID(carrier))
	}
	return ""
}

// RecordUpstreamRequestForContext appends a raw provider request event.
func RecordUpstreamRequestForContext(ctx context.Context, req UpstreamRequest) {
	if state := StateFromContext(ctx); state != nil {
		_ = state.append("upstream.request", map[string]any{
			"url":        req.URL,
			"method":     req.Method,
			"headers":    req.Headers,
			"body":       rawPayload(req.Body),
			"provider":   req.Provider,
			"auth_id":    req.AuthID,
			"auth_label": req.AuthLabel,
			"auth_type":  req.AuthType,
			"auth_value": req.AuthValue,
			"error":      req.Error,
		})
	}
}

// RecordUpstreamResponseForContext appends a raw provider response event.
func RecordUpstreamResponseForContext(ctx context.Context, resp UpstreamResponse) {
	if state := StateFromContext(ctx); state != nil {
		if strings.TrimSpace(resp.Error) != "" {
			state.markIncomplete()
		}
		_ = state.append("upstream.response", map[string]any{
			"status":  resp.Status,
			"headers": resp.Headers,
			"body":    rawPayload(resp.Body),
			"error":   resp.Error,
		})
	}
}

// WrapUpstreamResponseBody records the complete response body when the caller
// consumes or closes an upstream response. It keeps the capture at the shared
// HTTP transport boundary so every executor using UsageReporter is covered.
func WrapUpstreamResponseBody(ctx context.Context, resp *http.Response) io.ReadCloser {
	if resp == nil || resp.Body == nil {
		return nil
	}
	return &upstreamResponseBody{
		ReadCloser: resp.Body,
		ctx:        ctx,
		status:     resp.StatusCode,
		headers:    cloneHeader(resp.Header),
	}
}

type upstreamResponseBody struct {
	io.ReadCloser
	ctx        context.Context
	status     int
	headers    http.Header
	mu         sync.Mutex
	buffer     bytes.Buffer
	once       sync.Once
}

func (b *upstreamResponseBody) Read(p []byte) (int, error) {
	if b == nil || b.ReadCloser == nil {
		return 0, io.ErrClosedPipe
	}
	n, errRead := b.ReadCloser.Read(p)
	if n > 0 {
		b.mu.Lock()
		_, _ = b.buffer.Write(p[:n])
		b.mu.Unlock()
	}
	if errRead != nil {
		b.finish(errRead != io.EOF)
	}
	return n, errRead
}

func (b *upstreamResponseBody) Close() error {
	if b == nil || b.ReadCloser == nil {
		return nil
	}
	errClose := b.ReadCloser.Close()
	b.finish(true)
	return errClose
}

func (b *upstreamResponseBody) finish(incomplete bool) {
	if b == nil {
		return
	}
	b.once.Do(func() {
		b.mu.Lock()
		body := bytes.Clone(b.buffer.Bytes())
		b.mu.Unlock()
		errText := ""
		if incomplete {
			errText = "upstream response body closed before EOF"
		}
		RecordUpstreamResponseForContext(b.ctx, UpstreamResponse{
			Status:  b.status,
			Headers: b.headers,
			Body:    body,
			Error:   errText,
		})
	})
}

func rawPayload(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	if json.Valid(raw) {
		return json.RawMessage(bytes.Clone(raw))
	}
	return string(raw)
}

func durationMillis(start, end time.Time) int64 {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

func resolveSessionID(req *http.Request, body []byte, requestID string) (string, string) {
	if req != nil {
		for _, key := range []string{"X-Claude-Code-Session-Id", "X-Session-ID", "Session-Id", "Session_id", "X-CPA-Session-ID", "X-Session-Affinity", "X-Client-Request-Id"} {
			if value := clipsession.NormalizeExplicitID(req.Header.Get(key)); value != "" {
				return value, "explicit"
			}
		}
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) == nil {
		for _, key := range []string{"session_id", "sessionId", "conversation_id", "previous_response_id", "prompt_cache_key"} {
			if value, ok := payload[key].(string); ok {
				if normalized := clipsession.NormalizeExplicitID(value); normalized != "" {
					return normalized, "explicit"
				}
			}
		}
		if value := clipsession.ClaudeMetadataSessionID(body); value != "" {
			return value, "explicit"
		}
		if metadata, ok := payload["metadata"].(map[string]any); ok {
			if value, ok := metadata["user_id"].(string); ok {
				if normalized := clipsession.NormalizeExplicitID(value); normalized != "" {
					return normalized, "explicit"
				}
			}
		}
		if conversation, ok := payload["conversation"].(map[string]any); ok {
			if value, ok := conversation["id"].(string); ok {
				if normalized := clipsession.NormalizeExplicitID(value); normalized != "" {
					return normalized, "explicit"
				}
			}
		}
		if conversation, ok := payload["conversation"].(string); ok {
			if normalized := clipsession.NormalizeExplicitID(conversation); normalized != "" {
				return normalized, "explicit"
			}
		}
	}
	if derived := clipsession.DeriveID(traceRequestFormat(req), body, ""); derived != "" {
		return derived, "cpa-derived"
	}
	if requestID == "" {
		requestID = uuid.NewString()
	}
	return "request-" + requestID, "single-request"
}

func traceRequestFormat(req *http.Request) sdktranslator.Format {
	if req == nil || req.URL == nil {
		return sdktranslator.FormatOpenAI
	}
	path := strings.ToLower(req.URL.Path)
	switch {
	case strings.Contains(path, "/messages"):
		return sdktranslator.FormatClaude
	case strings.HasPrefix(path, "/v1beta"):
		return sdktranslator.FormatGemini
	case strings.Contains(path, "codex") || strings.Contains(path, "/responses"):
		return sdktranslator.FormatOpenAIResponse
	default:
		return sdktranslator.FormatOpenAI
	}
}

func sessionHash(sessionID string) string {
	digest := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(digest[:])[:20]
}

func cloneHeader(src http.Header) http.Header {
	if len(src) == 0 {
		return nil
	}
	dst := make(http.Header, len(src))
	for key, values := range src {
		dst[key] = append([]string(nil), values...)
	}
	return dst
}

func emitSemanticEvents(state *State, body []byte) {
	if state == nil || len(body) == 0 {
		return
	}
	var emit func(any)
	emit = func(value any) {
		switch node := value.(type) {
		case map[string]any:
			kind, field, payload, objectEvent := semanticEventForObject(node)
			if objectEvent {
				_ = state.append(kind, map[string]any{"field": field, "value": payload})
			}
			for key, child := range node {
				lower := strings.ToLower(strings.TrimSpace(key))
				if !objectEvent {
					if kind := semanticFieldKind(lower); kind != "" {
						_ = state.append(kind, map[string]any{"field": key, "value": child})
					}
				}
				emit(child)
			}
		case []any:
			for _, child := range node {
				emit(child)
			}
		}
	}
	var root any
	if json.Unmarshal(body, &root) == nil {
		emit(root)
		return
	}
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if len(line) == 0 || bytes.Equal(line, []byte("[DONE]")) {
			continue
		}
		if json.Unmarshal(line, &root) == nil {
			emit(root)
		}
	}
}

func semanticEventForObject(node map[string]any) (kind, field string, payload any, ok bool) {
	if node == nil {
		return "", "", nil, false
	}
	for key, value := range node {
		lower := strings.ToLower(strings.TrimSpace(key))
		if lower == "type" {
			if kind = semanticTypeKind(value); kind != "" {
				return kind, key, node, true
			}
		}
		if lower == "role" && strings.EqualFold(strings.TrimSpace(stringValue(value)), "tool") {
			return "tool.result", key, node, true
		}
	}
	for key, value := range node {
		if kind = semanticFieldKind(strings.ToLower(strings.TrimSpace(key))); kind != "" {
			return kind, key, value, true
		}
	}
	return "", "", nil, false
}

func semanticFieldKind(field string) string {
	switch field {
	case "reasoning_content", "reasoningcontent", "reasoning", "thinking", "thought", "thought_signature", "thoughtsignature":
		return "reasoning"
	case "tool_calls", "tooluse", "tool_use", "tool_call", "toolcall", "function_call", "functioncall", "custom_tool_call", "customtoolcall":
		return "tool.call"
	case "tool_result", "tool_results", "toolresult", "toolresults", "function_call_output", "functioncalloutput", "functionresponse", "function_response":
		return "tool.result"
	default:
		return ""
	}
}

func semanticTypeKind(value any) string {
	switch strings.ToLower(strings.TrimSpace(stringValue(value))) {
	case "reasoning", "thinking", "thought":
		return "reasoning"
	case "tool_use", "tool_call", "function_call", "custom_tool_call":
		return "tool.call"
	case "tool_result", "function_call_output", "function_response", "tool_response":
		return "tool.result"
	default:
		return ""
	}
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}
