package trace

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"
)

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
