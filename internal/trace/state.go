package trace

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

// ContextKey is the Gin context key used by the middleware and usage hooks.
const ContextKey = "__cpa_trace_state__"

type contextGetter interface {
	Get(string) (any, bool)
}

// StateFromContext returns the active trace state carried by a CPA execution context.
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
	recorder               *Recorder
	mu                     sync.Mutex
	sequence               int64
	finished               bool
	incomplete             bool
	requestID              string
	traceID                string
	sessionID              string
	sessionSource          string
	turnID                 string
	startedAt              time.Time
	requestMethod          string
	requestPath            string
	requestHeader          http.Header
	requestBody            []byte
	response               bytes.Buffer
	status                 int
	responseHeads          http.Header
	firstByte              time.Time
	usage                  *coreusage.Record
	lastUpstreamRequestKey string
	upstreamRequestOpen    bool
	semanticSeen           map[string]bool
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
	return &State{
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
		semanticSeen:  map[string]bool{},
	}
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
	return s.append("request.input", map[string]any{
		"method":  s.requestMethod,
		"path":    s.requestPath,
		"headers": s.requestHeader,
		"body":    rawPayload(s.requestBody),
	})
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
	s.mu.Lock()
	incomplete = s.incomplete
	s.mu.Unlock()
	_ = s.append("model.output", map[string]any{
		"status":           statusCode,
		"headers":          responseHeaders,
		"body":             rawPayload(body),
		"first_byte_at":    firstByte,
		"latency_ms":       time.Since(started).Milliseconds(),
		"failed":           statusCode >= http.StatusBadRequest,
		"trace_incomplete": incomplete,
	})
	emitSemanticEventsFrom(s, body, "model.output")
	s.mu.Lock()
	incomplete = s.incomplete
	s.mu.Unlock()
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
		"access_token_sha256":   record.AccessTokenSHA256,
		"source":                record.Source,
		"reasoning_effort":      record.ReasoningEffort,
		"service_tier":          record.ServiceTier,
		"response_service_tier": record.ResponseServiceTier,
		"generate":              coreusage.GenerateEnabled(record.Generate),
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
		"token_breakdown":       record.Detail.TokenBreakdown,
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
