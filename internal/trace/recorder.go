package trace

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

const (
	defaultMaxFileBytes  int64 = 256 << 20
	defaultRetentionDays       = 30
	defaultMetadataDays        = 180
	defaultMaxBytes      int64 = 10 << 30
	queueSize                  = 2048
)

// Trace files are partitioned by the operator-facing local date. The service
// is deployed in Asia/Shanghai, but keeping the location explicit prevents a
// host or container timezone change from silently moving events between days.
var traceLocation = time.FixedZone("Asia/Shanghai", 8*60*60)

// Config controls recorder storage and cleanup.
type Config struct {
	Enabled            bool
	RootDir            string
	MaxFileBytes       int64
	RetentionDays      int
	MetadataDays       int
	MaxBytes           int64
	RecordStreamChunks bool
}

// Recorder persists trace events and maintains their query index.
type Recorder struct {
	rootDir            string
	db                 *sql.DB
	maxFileBytes       int64
	retentionDays      int
	metadataDays       int
	maxBytes           int64
	recordStreamChunks bool

	queue       chan queuedEvent
	stop        chan struct{}
	done        chan struct{}
	cleanupDone chan struct{}
	close       sync.Once
	queueMu     sync.RWMutex
	filesMu     sync.Mutex
	files       map[string]*traceFile
	errorMu     sync.RWMutex
	lastError   string
	lastErrorAt time.Time
	writeErrors int64
}

type traceFile struct {
	key       string
	sessionID string
	date      string
	hash      string
	part      int
	path      string
	bytes     int64
	file      *os.File
}

type queuedEvent struct {
	event Event
	done  chan error
}

// NewRecorder creates a recorder rooted at a durable directory.
func NewRecorder(cfg Config) (*Recorder, error) {
	if !cfg.Enabled {
		return &Recorder{}, nil
	}
	if strings.TrimSpace(cfg.RootDir) == "" {
		return nil, errors.New("trace root directory is empty")
	}
	if cfg.MaxFileBytes <= 0 {
		cfg.MaxFileBytes = defaultMaxFileBytes
	}
	if cfg.RetentionDays <= 0 {
		cfg.RetentionDays = defaultRetentionDays
	}
	if cfg.MetadataDays <= 0 {
		cfg.MetadataDays = defaultMetadataDays
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = defaultMaxBytes
	}
	if err := os.MkdirAll(cfg.RootDir, 0o700); err != nil {
		return nil, fmt.Errorf("create trace directory: %w", err)
	}
	db, errOpen := sql.Open("sqlite3", filepath.Join(cfg.RootDir, "traces.db"))
	if errOpen != nil {
		return nil, fmt.Errorf("open trace index: %w", errOpen)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		`CREATE TABLE IF NOT EXISTS sessions (
			session_id TEXT PRIMARY KEY,
			first_at TEXT NOT NULL,
			last_at TEXT NOT NULL,
			session_source TEXT NOT NULL,
			turn_count INTEGER NOT NULL DEFAULT 0,
			event_count INTEGER NOT NULL DEFAULT 0,
			bytes INTEGER NOT NULL DEFAULT 0,
			incomplete INTEGER NOT NULL DEFAULT 0,
			file_path TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS turns (
			turn_id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			request_id TEXT NOT NULL DEFAULT '',
			trace_id TEXT NOT NULL DEFAULT '',
			started_at TEXT NOT NULL,
			last_at TEXT NOT NULL,
			status INTEGER NOT NULL DEFAULT 0,
			model TEXT NOT NULL DEFAULT '',
			provider TEXT NOT NULL DEFAULT '',
			api_key TEXT NOT NULL DEFAULT '',
			input_tokens INTEGER NOT NULL DEFAULT 0,
			output_tokens INTEGER NOT NULL DEFAULT 0,
			reasoning_tokens INTEGER NOT NULL DEFAULT 0,
			cached_tokens INTEGER NOT NULL DEFAULT 0,
			total_tokens INTEGER NOT NULL DEFAULT 0,
			latency_ms INTEGER NOT NULL DEFAULT 0,
			ttft_ms INTEGER NOT NULL DEFAULT 0,
			failed INTEGER NOT NULL DEFAULT 0,
			incomplete INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id TEXT NOT NULL UNIQUE,
			session_id TEXT NOT NULL,
			turn_id TEXT NOT NULL,
			request_id TEXT NOT NULL DEFAULT '',
			trace_id TEXT NOT NULL DEFAULT '',
			sequence INTEGER NOT NULL,
			timestamp TEXT NOT NULL,
			event TEXT NOT NULL,
			incomplete INTEGER NOT NULL DEFAULT 0,
			file_path TEXT NOT NULL,
			file_offset INTEGER NOT NULL,
			file_size INTEGER NOT NULL
		)`,
		"CREATE INDEX IF NOT EXISTS idx_sessions_last_at ON sessions(last_at DESC)",
		"CREATE INDEX IF NOT EXISTS idx_turns_session_last ON turns(session_id, last_at DESC)",
		"CREATE INDEX IF NOT EXISTS idx_events_session_seq ON events(session_id, sequence)",
		"CREATE INDEX IF NOT EXISTS idx_events_turn_seq ON events(turn_id, sequence)",
	} {
		if _, errExec := db.Exec(stmt); errExec != nil {
			_ = db.Close()
			return nil, fmt.Errorf("initialize trace index: %w", errExec)
		}
	}
	r := &Recorder{
		rootDir:            cfg.RootDir,
		db:                 db,
		maxFileBytes:       cfg.MaxFileBytes,
		retentionDays:      cfg.RetentionDays,
		metadataDays:       cfg.MetadataDays,
		maxBytes:           cfg.MaxBytes,
		recordStreamChunks: cfg.RecordStreamChunks,
		queue:              make(chan queuedEvent, queueSize),
		stop:               make(chan struct{}),
		done:               make(chan struct{}),
		cleanupDone:        make(chan struct{}),
		files:              make(map[string]*traceFile),
	}
	go r.run()
	go r.cleanupLoop()
	return r, nil
}

// Enabled reports whether this recorder is active.
func (r *Recorder) Enabled() bool { return r != nil && r.db != nil }

// Append queues one event and waits until the writer has persisted it. It
// intentionally applies backpressure when the bounded writer queue is full so
// trace chunks are not silently dropped and write failures can mark the Turn
// incomplete before the API request finishes.
func (r *Recorder) Append(event Event) error {
	if !r.Enabled() {
		return nil
	}
	if strings.TrimSpace(event.EventID) == "" {
		event.EventID = fmt.Sprintf("event-%d", time.Now().UnixNano())
	}
	item := queuedEvent{event: event, done: make(chan error, 1)}
	r.queueMu.RLock()
	defer r.queueMu.RUnlock()
	select {
	case <-r.stop:
		return errors.New("trace recorder is closed")
	default:
	}
	select {
	case r.queue <- item:
		return <-item.done
	case <-r.stop:
		return errors.New("trace recorder is closed")
	}
}

func (r *Recorder) run() {
	defer close(r.done)
	for item := range r.queue {
		errWrite := r.writeEvent(item.event)
		if errWrite != nil {
			r.recordWriteError(errWrite)
			log.WithError(errWrite).Warn("trace: failed to persist event")
		}
		item.done <- errWrite
		close(item.done)
	}
}

func (r *Recorder) recordWriteError(err error) {
	if r == nil || err == nil {
		return
	}
	r.errorMu.Lock()
	r.lastError = err.Error()
	r.lastErrorAt = time.Now().UTC()
	r.writeErrors++
	r.errorMu.Unlock()
}

func (r *Recorder) cleanupLoop() {
	defer close(r.cleanupDone)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if errCleanup := r.Cleanup(time.Now()); errCleanup != nil {
				log.WithError(errCleanup).Warn("trace: cleanup failed")
			}
		case <-r.stop:
			return
		}
	}
}

func (r *Recorder) writeEvent(event Event) error {
	line, errMarshal := json.Marshal(event)
	if errMarshal != nil {
		return errMarshal
	}
	line = append(line, '\n')
	r.filesMu.Lock()
	defer r.filesMu.Unlock()
	file, errFile := r.fileForLocked(event)
	if errFile != nil {
		return errFile
	}
	if file.bytes > 0 && file.bytes+int64(len(line)) > r.maxFileBytes {
		if errRotate := r.rotateLocked(file); errRotate != nil {
			return errRotate
		}
		file, errFile = r.fileForLocked(event)
		if errFile != nil {
			return errFile
		}
	}
	offset := file.bytes
	n, errWrite := file.file.Write(line)
	if errWrite != nil {
		return errWrite
	}
	if n != len(line) {
		return ioErrShortWrite
	}
	file.bytes += int64(n)
	if errIndex := r.indexEvent(event, filepath.ToSlash(relativePath(r.rootDir, file.path)), offset, int64(n)); errIndex != nil {
		return errIndex
	}
	if errLimit := r.enforceMaxBytesLocked(file.path); errLimit != nil {
		return errLimit
	}
	return nil
}

var ioErrShortWrite = errors.New("short trace write")

func (r *Recorder) fileForLocked(event Event) (*traceFile, error) {
	date := event.Timestamp.In(traceLocation).Format("2006-01-02")
	if date == "0001-01-01" {
		date = time.Now().In(traceLocation).Format("2006-01-02")
	}
	hash := sessionHash(event.SessionID)
	key := date + ":" + hash
	if existing := r.files[key]; existing != nil {
		return existing, nil
	}
	dir := filepath.Join(r.rootDir, date)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		return nil, errMkdir
	}
	part := 1
	lastPartial := 0
	for {
		name := fmt.Sprintf("session-%s", hash)
		if part > 1 {
			name += fmt.Sprintf(".part-%d", part)
		}
		partial := filepath.Join(dir, name+".ndjson.partial")
		final := strings.TrimSuffix(partial, ".partial")
		if _, errStat := os.Stat(partial); errStat == nil {
			lastPartial = part
			part++
			continue
		}
		if _, errStat := os.Stat(final); errStat == nil {
			part++
			continue
		}
		break
	}
	if lastPartial > 0 {
		// Continue the active partial file rather than creating an unnecessary
		// new part after a process restart.
		part = lastPartial
	}
	name := fmt.Sprintf("session-%s", hash)
	if part > 1 {
		name += fmt.Sprintf(".part-%d", part)
	}
	path := filepath.Join(dir, name+".ndjson.partial")
	f, errOpen := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if errOpen != nil {
		return nil, errOpen
	}
	stat, errStat := f.Stat()
	if errStat != nil {
		_ = f.Close()
		return nil, errStat
	}
	file := &traceFile{key: key, sessionID: event.SessionID, date: date, hash: hash, part: part, path: path, bytes: stat.Size(), file: f}
	r.files[key] = file
	return file, nil
}

func (r *Recorder) rotateLocked(file *traceFile) error {
	if file == nil {
		return nil
	}
	if errClose := file.file.Close(); errClose != nil {
		return errClose
	}
	finalPath := strings.TrimSuffix(file.path, ".partial")
	if errRename := os.Rename(file.path, finalPath); errRename != nil && !errors.Is(errRename, os.ErrNotExist) {
		return errRename
	}
	delete(r.files, file.key)
	return nil
}

func (r *Recorder) indexEvent(event Event, relativeFile string, offset, size int64) error {
	if r.db == nil {
		return nil
	}
	tx, errTx := r.db.Begin()
	if errTx != nil {
		return errTx
	}
	rollback := func(err error) error {
		_ = tx.Rollback()
		return err
	}
	now := event.Timestamp.UTC().Format(time.RFC3339Nano)
	if _, err := tx.Exec(`INSERT INTO sessions(session_id,first_at,last_at,session_source,event_count,bytes,incomplete,file_path)
		VALUES(?,?,?,?,1,?,?,?) ON CONFLICT(session_id) DO UPDATE SET last_at=excluded.last_at,event_count=sessions.event_count+1,bytes=sessions.bytes+excluded.bytes,incomplete=MAX(sessions.incomplete,excluded.incomplete),file_path=excluded.file_path`,
		event.SessionID, now, now, event.SessionSource, size, size, boolInt(event.Incomplete), relativeFile); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(`INSERT INTO turns(turn_id,session_id,request_id,trace_id,started_at,last_at,incomplete)
		VALUES(?,?,?,?,?,?,?) ON CONFLICT(turn_id) DO UPDATE SET last_at=excluded.last_at,incomplete=MAX(turns.incomplete,excluded.incomplete)`,
		event.TurnID, event.SessionID, event.RequestID, event.TraceID, now, now, boolInt(event.Incomplete)); err != nil {
		return rollback(err)
	}
	if event.Kind == "request.input" {
		if _, err := tx.Exec("UPDATE sessions SET turn_count = turn_count + 1 WHERE session_id = ?", event.SessionID); err != nil {
			return rollback(err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO events(event_id,session_id,turn_id,request_id,trace_id,sequence,timestamp,event,incomplete,file_path,file_offset,file_size)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, event.EventID, event.SessionID, event.TurnID, event.RequestID, event.TraceID, event.Sequence, now, event.Kind, boolInt(event.Incomplete), relativeFile, offset, size); err != nil {
		return rollback(err)
	}
	if err := r.updateTurnFromEvent(tx, event); err != nil {
		return rollback(err)
	}
	if errCommit := tx.Commit(); errCommit != nil {
		return errCommit
	}
	return nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func relativePath(root, path string) string {
	rel, errRel := filepath.Rel(root, path)
	if errRel != nil {
		return path
	}
	return rel
}

// Close drains queued events and closes files/database. Active partial files
// are atomically renamed to their final .ndjson names.
func (r *Recorder) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	var errOut error
	r.close.Do(func() {
		r.queueMu.Lock()
		close(r.stop)
		close(r.queue)
		r.queueMu.Unlock()
		<-r.cleanupDone
		<-r.done
		r.filesMu.Lock()
		for key, file := range r.files {
			if file == nil || file.file == nil {
				continue
			}
			if errClose := file.file.Close(); errClose != nil && errOut == nil {
				errOut = errClose
			}
			finalPath := strings.TrimSuffix(file.path, ".partial")
			if errRename := os.Rename(file.path, finalPath); errRename != nil && !errors.Is(errRename, os.ErrNotExist) && errOut == nil {
				errOut = errRename
			}
			delete(r.files, key)
		}
		r.filesMu.Unlock()
		if errClose := r.db.Close(); errClose != nil && errOut == nil {
			errOut = errClose
		}
	})
	return errOut
}

// Cleanup removes body files older than retention and metadata older than the
// metadata retention period. It never deletes the SQLite index itself.
func (r *Recorder) Cleanup(now time.Time) error {
	if !r.Enabled() {
		return nil
	}
	r.filesMu.Lock()
	defer r.filesMu.Unlock()
	bodyCutoff := now.AddDate(0, 0, -r.retentionDays)
	if errWalk := filepath.WalkDir(r.rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d == nil || d.IsDir() || path == filepath.Join(r.rootDir, "traces.db") || strings.Contains(path, "traces.db-") {
			return nil
		}
		if _, active := r.activeFileByPathLocked(path); active {
			return nil
		}
		if !strings.HasSuffix(path, ".ndjson") && !strings.HasSuffix(path, ".ndjson.partial") {
			return nil
		}
		info, errStat := d.Info()
		if errStat == nil && info.ModTime().Before(bodyCutoff) {
			if errRemove := os.Remove(path); errRemove == nil && r.db != nil {
				relative := filepath.ToSlash(relativePath(r.rootDir, path))
				_, _ = r.db.Exec("UPDATE sessions SET incomplete = 1 WHERE session_id IN (SELECT session_id FROM events WHERE file_path = ?)", relative)
				_, _ = r.db.Exec("UPDATE turns SET incomplete = 1 WHERE session_id IN (SELECT session_id FROM events WHERE file_path = ?)", relative)
			}
		}
		return nil
	}); errWalk != nil {
		return errWalk
	}
	metadataCutoff := now.AddDate(0, 0, -r.metadataDays).UTC().Format(time.RFC3339Nano)
	_, errDelete := r.db.Exec("DELETE FROM events WHERE timestamp < ?", metadataCutoff)
	if errDelete != nil {
		return errDelete
	}
	_, errDelete = r.db.Exec("DELETE FROM turns WHERE last_at < ?", metadataCutoff)
	if errDelete != nil {
		return errDelete
	}
	_, errDelete = r.db.Exec("DELETE FROM sessions WHERE last_at < ?", metadataCutoff)
	if errDelete != nil {
		return errDelete
	}
	return r.rebuildAggregates()
}

// enforceMaxBytesLocked removes the oldest finalized body files until the
// configured storage ceiling is met. The active file is never removed.
// Callers must hold filesMu.
func (r *Recorder) enforceMaxBytesLocked(activePath string) error {
	if r == nil || r.maxBytes <= 0 {
		return nil
	}
	type bodyFile struct {
		path string
		size int64
		mod  time.Time
	}
	var candidates []bodyFile
	var total int64
	errWalk := filepath.WalkDir(r.rootDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry == nil || entry.IsDir() || path == activePath || strings.HasSuffix(path, ".partial") || filepath.Base(path) == "traces.db" || strings.Contains(path, "traces.db-") {
			return nil
		}
		if !strings.HasSuffix(path, ".ndjson") {
			return nil
		}
		info, errInfo := entry.Info()
		if errInfo != nil {
			return errInfo
		}
		candidates = append(candidates, bodyFile{path: path, size: info.Size(), mod: info.ModTime()})
		total += info.Size()
		return nil
	})
	if errWalk != nil {
		return errWalk
	}
	for _, active := range r.files {
		if active != nil {
			total += active.bytes
		}
	}
	if total <= r.maxBytes {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].mod.Before(candidates[j].mod) })
	for _, candidate := range candidates {
		if total <= r.maxBytes {
			break
		}
		if errRemove := os.Remove(candidate.path); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
			return errRemove
		}
		total -= candidate.size
		if r.db == nil {
			continue
		}
		relative := filepath.ToSlash(relativePath(r.rootDir, candidate.path))
		if _, err := r.db.Exec("DELETE FROM events WHERE file_path = ?", relative); err != nil {
			return err
		}
		if _, err := r.db.Exec("DELETE FROM turns WHERE session_id NOT IN (SELECT DISTINCT session_id FROM events)"); err != nil {
			return err
		}
		if _, err := r.db.Exec("DELETE FROM sessions WHERE session_id NOT IN (SELECT DISTINCT session_id FROM events)"); err != nil {
			return err
		}
	}
	return r.rebuildAggregates()
}

func (r *Recorder) activeFileByPathLocked(path string) (*traceFile, bool) {
	for _, file := range r.files {
		if file != nil && file.path == path {
			return file, true
		}
	}
	return nil, false
}

func (r *Recorder) rebuildAggregates() error {
	if r == nil || r.db == nil {
		return nil
	}
	if _, err := r.db.Exec(`UPDATE sessions SET
		event_count = (SELECT COUNT(*) FROM events WHERE events.session_id = sessions.session_id),
		bytes = COALESCE((SELECT SUM(file_size) FROM events WHERE events.session_id = sessions.session_id), 0),
		first_at = COALESCE((SELECT MIN(timestamp) FROM events WHERE events.session_id = sessions.session_id), first_at),
		last_at = COALESCE((SELECT MAX(timestamp) FROM events WHERE events.session_id = sessions.session_id), last_at),
		turn_count = (SELECT COUNT(DISTINCT turn_id) FROM events WHERE events.session_id = sessions.session_id),
		file_path = COALESCE((SELECT file_path FROM events WHERE events.session_id = sessions.session_id ORDER BY timestamp DESC, sequence DESC LIMIT 1), file_path)`); err != nil {
		return err
	}
	if _, err := r.db.Exec("DELETE FROM turns WHERE turn_id NOT IN (SELECT DISTINCT turn_id FROM events)"); err != nil {
		return err
	}
	_, err := r.db.Exec("DELETE FROM sessions WHERE session_id NOT IN (SELECT DISTINCT session_id FROM events)")
	return err
}

// RootDir returns the recorder's durable root.
func (r *Recorder) RootDir() string {
	if r == nil {
		return ""
	}
	return r.rootDir
}

// MaxBytes returns the configured body storage ceiling.
func (r *Recorder) MaxBytes() int64 {
	if r == nil {
		return 0
	}
	return r.maxBytes
}

func sortEvents(events []Event) {
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Timestamp.Equal(events[j].Timestamp) {
			return events[i].Sequence < events[j].Sequence
		}
		return events[i].Timestamp.Before(events[j].Timestamp)
	})
}
