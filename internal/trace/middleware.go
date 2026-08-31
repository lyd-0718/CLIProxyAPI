package trace

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	log "github.com/sirupsen/logrus"
)

// Middleware starts a Trace for each public model API request. It sits before
// the existing request logger so both systems observe the same bytes.
func Middleware(recorder *Recorder) gin.HandlerFunc {
	return func(c *gin.Context) {
		if recorder == nil || !recorder.Enabled() || c == nil || c.Request == nil || !isTraceablePath(c.Request.URL.Path) {
			c.Next()
			return
		}
		body, errRead := readAndRestoreBody(c.Request)
		if errRead != nil {
			c.Next()
			return
		}
		state := NewState(recorder, c.Request, body)
		if state == nil {
			c.Next()
			return
		}
		c.Set(ContextKey, state)
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ContextKey, state))
		if errBegin := state.Begin(); errBegin != nil {
			log.WithError(errBegin).Warn("trace: failed to record request")
		}
		wrapper := &ResponseWriter{ResponseWriter: c.Writer, state: state}
		c.Writer = wrapper
		c.Next()
		state.SetTraceID(logging.GetGinCPATraceID(c))
		state.Finish(wrapper.statusCode(), wrapper.Header())
	}
}

func readAndRestoreBody(req *http.Request) ([]byte, error) {
	if req == nil || req.Body == nil || req.Body == http.NoBody {
		return nil, nil
	}
	body, errRead := io.ReadAll(req.Body)
	_ = req.Body.Close()
	req.Body = io.NopCloser(bytes.NewReader(body))
	return body, errRead
}

func isTraceablePath(path string) bool {
	for _, prefix := range []string{"/v1", "/v1beta", "/openai/v1", "/backend-api/codex"} {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

// ResponseWriter captures every downstream response byte after it has been
// written to the client. It deliberately preserves Gin's writer embedding.
type ResponseWriter struct {
	gin.ResponseWriter
	state  *State
	status int
}

func (w *ResponseWriter) WriteHeader(statusCode int) {
	if w == nil {
		return
	}
	w.status = statusCode
	if w.state != nil {
		w.state.SetResponseMetadata(statusCode, w.Header())
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *ResponseWriter) Write(data []byte) (int, error) {
	if w == nil || w.ResponseWriter == nil {
		return 0, io.ErrClosedPipe
	}
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, errWrite := w.ResponseWriter.Write(data)
	if w.state != nil && n > 0 {
		w.state.AppendDownstreamChunk(data[:n])
	}
	return n, errWrite
}

func (w *ResponseWriter) WriteString(data string) (int, error) {
	if w == nil || w.ResponseWriter == nil {
		return 0, io.ErrClosedPipe
	}
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, errWrite := w.ResponseWriter.WriteString(data)
	if w.state != nil && n > 0 {
		w.state.AppendDownstreamChunk([]byte(data[:n]))
	}
	return n, errWrite
}

func (w *ResponseWriter) Flush() {
	if w == nil || w.ResponseWriter == nil {
		return
	}
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	w.ResponseWriter.Flush()
}

func (w *ResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hijacker.Hijack()
}

func (w *ResponseWriter) CloseNotify() <-chan bool {
	if notifier, ok := w.ResponseWriter.(http.CloseNotifier); ok {
		return notifier.CloseNotify()
	}
	ch := make(chan bool)
	return ch
}

func (w *ResponseWriter) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := w.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (w *ResponseWriter) Unwrap() http.ResponseWriter {
	if w == nil || w.ResponseWriter == nil {
		return nil
	}
	return w.ResponseWriter
}

func (w *ResponseWriter) statusCode() int {
	if w == nil {
		return http.StatusOK
	}
	if w.status > 0 {
		return w.status
	}
	if statusWriter, ok := w.ResponseWriter.(interface{ Status() int }); ok && statusWriter.Status() > 0 {
		return statusWriter.Status()
	}
	return http.StatusOK
}
