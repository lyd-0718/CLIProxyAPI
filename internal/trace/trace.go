// Package trace implements the built-in durable Session/Turn recorder.
// Trace events are append-only NDJSON records with a small SQLite index.
package trace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	URL       string
	Method    string
	Headers   http.Header
	Body      []byte
	Provider  string
	AuthID    string
	AuthLabel string
	AuthType  string
	AuthValue string
}

// UpstreamResponse describes one provider response observation.
type UpstreamResponse struct {
	Status  int
	Headers http.Header
	Body    []byte
	Error   string
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
		_ = state.append("upstream.request", req)
	}
}

// RecordUpstreamResponseForContext appends a raw provider response event.
func RecordUpstreamResponseForContext(ctx context.Context, resp UpstreamResponse) {
	if state := StateFromContext(ctx); state != nil {
		_ = state.append("upstream.response", resp)
	}
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
	var emit func(any, string)
	emit = func(value any, parent string) {
		switch node := value.(type) {
		case map[string]any:
			for key, child := range node {
				lower := strings.ToLower(strings.TrimSpace(key))
				switch lower {
				case "reasoning_content", "reasoning", "thinking", "thought", "thoughtsignature":
					_ = state.append("reasoning", map[string]any{"field": key, "value": child})
				case "tool_calls", "tool_use", "tool_call":
					_ = state.append("tool.call", map[string]any{"field": key, "value": child})
				case "tool_result", "tool_results":
					_ = state.append("tool.result", map[string]any{"field": key, "value": child})
				}
				emit(child, key)
			}
		case []any:
			for _, child := range node {
				emit(child, parent)
			}
		}
	}
	var root any
	if json.Unmarshal(body, &root) == nil {
		emit(root, "")
		return
	}
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if len(line) == 0 || bytes.Equal(line, []byte("[DONE]")) {
			continue
		}
		if json.Unmarshal(line, &root) == nil {
			emit(root, "")
		}
	}
}
