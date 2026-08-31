package trace

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SessionSummary is the management API representation of a Session index row.
type SessionSummary struct {
	SessionID     string    `json:"session_id"`
	SessionSource string    `json:"session_id_source"`
	FirstAt       time.Time `json:"first_at"`
	LastAt        time.Time `json:"last_at"`
	TurnCount     int64     `json:"turn_count"`
	EventCount    int64     `json:"event_count"`
	Bytes         int64     `json:"bytes"`
	Incomplete    bool      `json:"incomplete"`
	FilePath      string    `json:"file_path,omitempty"`
}

// SessionDetail combines a summary and its ordered events.
type SessionDetail struct {
	SessionSummary
	Events []Event `json:"events"`
}

// ListSessions lists recent sessions with optional text filtering.
func (r *Recorder) ListSessions(query string, limit, offset int) ([]SessionSummary, error) {
	return r.ListSessionsFiltered(Filter{Query: query}, limit, offset)
}

// ListSessionsFiltered lists sessions using the same identity and time
// filters as the overview and request views.
func (r *Recorder) ListSessionsFiltered(filter Filter, limit, offset int) ([]SessionSummary, error) {
	if !r.Enabled() {
		return []SessionSummary{}, nil
	}
	limit, offset = normalizePage(limit, offset, 100)
	query := strings.TrimSpace(filter.Query)
	conditions := []string{"(? = '' OR s.session_id LIKE ? OR s.session_source LIKE ?)"}
	args := []any{query, "%" + query + "%", "%" + query + "%"}
	model := strings.TrimSpace(filter.Model)
	conditions = append(conditions, "(? = '' OR EXISTS (SELECT 1 FROM turns t WHERE t.session_id=s.session_id AND t.model LIKE ?))")
	args = append(args, model, "%"+model+"%")
	apiKey := strings.TrimSpace(filter.APIKey)
	conditions = append(conditions, "(? = '' OR EXISTS (SELECT 1 FROM turns t WHERE t.session_id=s.session_id AND t.api_key LIKE ?))")
	args = append(args, apiKey, "%"+apiKey+"%")
	source := strings.TrimSpace(filter.Source)
	conditions = append(conditions, "(? = '' OR s.session_source LIKE ?)")
	args = append(args, source, "%"+source+"%")
	if !filter.From.IsZero() {
		conditions = append(conditions, "s.last_at >= ?")
		args = append(args, filter.From.UTC().Format(time.RFC3339Nano))
	}
	if !filter.To.IsZero() {
		conditions = append(conditions, "s.last_at <= ?")
		args = append(args, filter.To.UTC().Format(time.RFC3339Nano))
	}
	rows, errQuery := r.db.Query(`SELECT s.session_id,s.session_source,s.first_at,s.last_at,s.turn_count,s.event_count,s.bytes,s.incomplete,s.file_path
		FROM sessions s WHERE `+strings.Join(conditions, " AND ")+`
		ORDER BY s.last_at DESC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if errQuery != nil {
		return nil, errQuery
	}
	defer rows.Close()
	out := make([]SessionSummary, 0)
	for rows.Next() {
		var item SessionSummary
		var first, last string
		var incomplete int
		if errScan := rows.Scan(&item.SessionID, &item.SessionSource, &first, &last, &item.TurnCount, &item.EventCount, &item.Bytes, &incomplete, &item.FilePath); errScan != nil {
			return nil, errScan
		}
		item.FirstAt, _ = time.Parse(time.RFC3339Nano, first)
		item.LastAt, _ = time.Parse(time.RFC3339Nano, last)
		item.Incomplete = incomplete != 0
		out = append(out, item)
	}
	return out, rows.Err()
}

// GetSession returns a Session summary and all available ordered events.
func (r *Recorder) GetSession(sessionID string) (SessionDetail, error) {
	if !r.Enabled() {
		return SessionDetail{}, errors.New("trace recorder is disabled")
	}
	var summary SessionSummary
	var first, last string
	var incomplete int
	err := r.db.QueryRow(`SELECT session_id,session_source,first_at,last_at,turn_count,event_count,bytes,incomplete,file_path FROM sessions WHERE session_id = ?`, sessionID).
		Scan(&summary.SessionID, &summary.SessionSource, &first, &last, &summary.TurnCount, &summary.EventCount, &summary.Bytes, &incomplete, &summary.FilePath)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SessionDetail{}, os.ErrNotExist
		}
		return SessionDetail{}, err
	}
	summary.FirstAt, _ = time.Parse(time.RFC3339Nano, first)
	summary.LastAt, _ = time.Parse(time.RFC3339Nano, last)
	summary.Incomplete = incomplete != 0
	events, errEvents := r.ReadEvents(sessionID)
	if errEvents != nil {
		return SessionDetail{}, errEvents
	}
	return SessionDetail{SessionSummary: summary, Events: events}, nil
}

// ReadEvents scans the session's NDJSON files. Payloads remain outside SQLite.
func (r *Recorder) ReadEvents(sessionID string) ([]Event, error) {
	if !r.Enabled() {
		return []Event{}, nil
	}
	hash := sessionHash(sessionID)
	events := make([]Event, 0)
	errWalk := filepath.WalkDir(r.rootDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Base(path) == "traces.db" || !strings.Contains(filepath.Base(path), "session-"+hash) {
			return nil
		}
		if !strings.HasSuffix(path, ".ndjson") && !strings.HasSuffix(path, ".ndjson.partial") {
			return nil
		}
		file, errOpen := os.Open(path)
		if errOpen != nil {
			return errOpen
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
		for scanner.Scan() {
			var event Event
			if errDecode := json.Unmarshal(scanner.Bytes(), &event); errDecode != nil {
				continue
			}
			if event.SessionID == sessionID {
				events = append(events, event)
			}
		}
		errScan := scanner.Err()
		_ = file.Close()
		return errScan
	})
	if errWalk != nil {
		return nil, errWalk
	}
	sortEvents(events)
	return events, nil
}

// ReadTurn returns events for a single Turn.
func (r *Recorder) ReadTurn(turnID string) ([]Event, error) {
	if !r.Enabled() {
		return []Event{}, nil
	}
	var sessionID string
	if err := r.db.QueryRow("SELECT session_id FROM turns WHERE turn_id = ?", turnID).Scan(&sessionID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []Event{}, os.ErrNotExist
		}
		return nil, err
	}
	events, errRead := r.ReadEvents(sessionID)
	if errRead != nil {
		return nil, errRead
	}
	filtered := events[:0]
	for _, event := range events {
		if event.TurnID == turnID {
			filtered = append(filtered, event)
		}
	}
	return filtered, nil
}

// DeleteSession removes indexed metadata and all corresponding trace files.
func (r *Recorder) DeleteSession(sessionID string) error {
	if !r.Enabled() {
		return nil
	}
	events, errEvents := r.ReadEvents(sessionID)
	if errEvents != nil {
		return errEvents
	}
	hash := sessionHash(sessionID)
	var paths []string
	errWalk := filepath.WalkDir(r.rootDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.Contains(filepath.Base(path), "session-"+hash) && (strings.HasSuffix(path, ".ndjson") || strings.HasSuffix(path, ".ndjson.partial")) {
			paths = append(paths, path)
		}
		return nil
	})
	if errWalk != nil {
		return errWalk
	}
	for _, path := range paths {
		if errRemove := os.Remove(path); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
			return errRemove
		}
	}
	tx, errTx := r.db.Begin()
	if errTx != nil {
		return errTx
	}
	if _, err := tx.Exec("DELETE FROM events WHERE session_id = ?", sessionID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.Exec("DELETE FROM turns WHERE session_id = ?", sessionID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.Exec("DELETE FROM sessions WHERE session_id = ?", sessionID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if errCommit := tx.Commit(); errCommit != nil {
		return errCommit
	}
	_ = events
	return nil
}

// ExportSession returns a stable NDJSON export of a session.
func (r *Recorder) ExportSession(sessionID string) ([]byte, error) {
	events, errEvents := r.ReadEvents(sessionID)
	if errEvents != nil {
		return nil, errEvents
	}
	var out strings.Builder
	for _, event := range events {
		line, errMarshal := json.Marshal(event)
		if errMarshal != nil {
			return nil, errMarshal
		}
		out.Write(line)
		out.WriteByte('\n')
	}
	return []byte(out.String()), nil
}

// Stats returns lightweight recorder health data.
func (r *Recorder) Stats() (map[string]any, error) {
	if !r.Enabled() {
		return map[string]any{"enabled": false}, nil
	}
	var sessions, events int64
	if err := r.db.QueryRow("SELECT COUNT(*) FROM sessions").Scan(&sessions); err != nil {
		return nil, err
	}
	if err := r.db.QueryRow("SELECT COUNT(*) FROM events").Scan(&events); err != nil {
		return nil, err
	}
	r.errorMu.RLock()
	lastError := r.lastError
	lastErrorAt := r.lastErrorAt
	writeErrors := r.writeErrors
	r.errorMu.RUnlock()
	stats := map[string]any{
		"enabled":        true,
		"root_dir":       r.rootDir,
		"sessions":       sessions,
		"events":         events,
		"max_file_bytes": r.maxFileBytes,
		"retention_days": r.retentionDays,
		"metadata_days":  r.metadataDays,
		"max_bytes":      r.maxBytes,
		"write_errors":   writeErrors,
	}
	if lastError != "" {
		stats["last_write_error"] = lastError
		stats["last_write_error_at"] = lastErrorAt
	}
	return stats, nil
}

func (r *Recorder) updateTurnFromEvent(tx *sql.Tx, event Event) error {
	if event.Kind != "usage" && event.Kind != "turn.completed" {
		return nil
	}
	var payload map[string]any
	if len(event.Payload) > 0 {
		_ = json.Unmarshal(event.Payload, &payload)
	}
	getString := func(key string) string {
		if value, ok := payload[key].(string); ok {
			return strings.TrimSpace(value)
		}
		return ""
	}
	getInt := func(key string) int64 {
		switch value := payload[key].(type) {
		case float64:
			return int64(value)
		case json.Number:
			v, _ := value.Int64()
			return v
		}
		return 0
	}
	_, err := tx.Exec(`UPDATE turns SET model=CASE WHEN ? <> '' THEN ? ELSE model END,
		provider=CASE WHEN ? <> '' THEN ? ELSE provider END,
		api_key=CASE WHEN ? <> '' THEN ? ELSE api_key END,
		status=CASE WHEN ? > 0 THEN ? ELSE status END,
		input_tokens=CASE WHEN ? <> 0 THEN ? ELSE input_tokens END,
		output_tokens=CASE WHEN ? <> 0 THEN ? ELSE output_tokens END,
		reasoning_tokens=CASE WHEN ? <> 0 THEN ? ELSE reasoning_tokens END,
		cached_tokens=CASE WHEN ? <> 0 THEN ? ELSE cached_tokens END,
		total_tokens=CASE WHEN ? <> 0 THEN ? ELSE total_tokens END,
		latency_ms=CASE WHEN ? <> 0 THEN ? ELSE latency_ms END,
		ttft_ms=CASE WHEN ? <> 0 THEN ? ELSE ttft_ms END,
		failed=CASE WHEN ? THEN 1 ELSE failed END,
		incomplete=CASE WHEN ? THEN 1 ELSE incomplete END WHERE turn_id = ?`,
		getString("model"), getString("model"), getString("provider"), getString("provider"), getString("api_key"), getString("api_key"),
		getInt("status"), getInt("status"), getInt("input_tokens"), getInt("input_tokens"), getInt("output_tokens"), getInt("output_tokens"),
		getInt("reasoning_tokens"), getInt("reasoning_tokens"), getInt("cached_tokens"), getInt("cached_tokens"), getInt("total_tokens"), getInt("total_tokens"),
		getInt("latency_ms"), getInt("latency_ms"), getInt("ttft_ms"), getInt("ttft_ms"), payloadBool(payload, "failed"), payloadBool(payload, "incomplete"), event.TurnID)
	return err
}

func payloadBool(payload map[string]any, key string) bool {
	value, _ := payload[key].(bool)
	return value
}
