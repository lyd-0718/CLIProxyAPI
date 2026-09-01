package trace

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
)

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

// RecordUpstreamRequestForContext appends a raw provider request event.
func RecordUpstreamRequestForContext(ctx context.Context, req UpstreamRequest) {
	state := StateFromContext(ctx)
	if state == nil {
		return
	}
	key := req.Method + "\n" + req.URL + "\n" + string(req.Body) + "\n" + req.Error
	state.mu.Lock()
	duplicate := state.upstreamRequestOpen && state.lastUpstreamRequestKey == key
	if !duplicate {
		state.lastUpstreamRequestKey = key
		state.upstreamRequestOpen = true
	}
	state.mu.Unlock()
	if duplicate {
		return
	}
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
	emitSemanticEventsFrom(state, req.Body, "upstream.request")
}

// RecordUpstreamResponseForContext appends a raw provider response event.
func RecordUpstreamResponseForContext(ctx context.Context, resp UpstreamResponse) {
	state := StateFromContext(ctx)
	if state == nil {
		return
	}
	if isUpstreamChunk(resp) {
		if state.recorder != nil && state.recorder.recordStreamChunks {
			_ = state.append("stream.chunk", map[string]any{"raw": rawPayload(resp.Body)})
		}
		return
	}
	state.mu.Lock()
	state.upstreamRequestOpen = false
	state.mu.Unlock()
	if strings.TrimSpace(resp.Error) != "" {
		state.markIncomplete()
	}
	_ = state.append("upstream.response", map[string]any{
		"status":  resp.Status,
		"headers": resp.Headers,
		"body":    rawPayload(resp.Body),
		"error":   resp.Error,
	})
	emitSemanticEventsFrom(state, resp.Body, "upstream.response")
}

func isUpstreamChunk(resp UpstreamResponse) bool {
	return resp.Status == 0 && strings.TrimSpace(resp.Error) == "" && len(resp.Headers) == 0 && len(resp.Body) > 0
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
	ctx     context.Context
	status  int
	headers http.Header
	mu      sync.Mutex
	buffer  bytes.Buffer
	once    sync.Once
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
