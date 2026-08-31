package trace

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

func TestRecorderPersistsSessionEventsAndUsage(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir(), MaxFileBytes: 1 << 20})
	if errNew != nil {
		t.Fatal(errNew)
	}
	req, errReq := http.NewRequest(http.MethodPost, "http://example.test/v1/chat/completions", bytes.NewBufferString(`{"model":"gpt-test","messages":[{"role":"user","content":"hello"}]}`))
	if errReq != nil {
		t.Fatal(errReq)
	}
	req.Header.Set("X-Session-ID", "session-test")
	state := NewState(recorder, req, []byte(`{"model":"gpt-test"}`))
	if state == nil {
		t.Fatal("expected state")
	}
	if err := state.Begin(); err != nil {
		t.Fatal(err)
	}
	RecordUpstreamRequestForContext(contextWithState(state), UpstreamRequest{URL: "https://upstream.test", Method: http.MethodPost, Body: []byte(`{"x":1}`), Headers: http.Header{"Authorization": {"Bearer secret"}}})
	RecordUpstreamResponseForContext(contextWithState(state), UpstreamResponse{Status: 200, Body: []byte(`{"delta":"ok"}`)})
	state.AppendDownstreamChunk([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	state.RecordUsage(coreusage.Record{Provider: "test", Model: "gpt-test", APIKey: "sk-test", Detail: coreusage.Detail{InputTokens: 2, OutputTokens: 3, TotalTokens: 5}})
	state.Finish(200, http.Header{"Content-Type": {"application/json"}})

	waitForEvents(t, recorder, "session-test", 5)
	detail, errGet := recorder.GetSession("session-test")
	if errGet != nil {
		t.Fatal(errGet)
	}
	if detail.SessionSource != "explicit" {
		t.Fatalf("session source = %q", detail.SessionSource)
	}
	if len(detail.Events) < 5 {
		t.Fatalf("events = %d, want at least 5", len(detail.Events))
	}
	var sawUsage, sawOutput, sawUpstream bool
	for _, event := range detail.Events {
		sawUsage = sawUsage || event.Kind == "usage"
		sawOutput = sawOutput || event.Kind == "model.output"
		sawUpstream = sawUpstream || event.Kind == "upstream.request"
	}
	if !sawUsage || !sawOutput || !sawUpstream {
		t.Fatalf("missing expected event kinds: usage=%v output=%v upstream=%v", sawUsage, sawOutput, sawUpstream)
	}
	if errClose := recorder.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	files, _ := filepath.Glob(filepath.Join(recorder.RootDir(), "*", "*.ndjson"))
	if len(files) == 0 {
		t.Fatal("expected finalized ndjson file")
	}
}

func TestRecorderRotatesTraceFiles(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir(), MaxFileBytes: 512})
	if errNew != nil {
		t.Fatal(errNew)
	}
	for i := 0; i < 20; i++ {
		if err := recorder.Append(Event{SchemaVersion: 1, EventID: time.Now().Format("150405.000000000") + string(rune(i)), Timestamp: time.Now(), SessionID: "rotate", SessionSource: "single-request", TurnID: "turn", Kind: "stream.chunk", Payload: []byte(`{"raw":"long trace payload"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(150 * time.Millisecond)
	if errClose := recorder.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	files, errGlob := filepath.Glob(filepath.Join(recorder.RootDir(), "*", "session-*.ndjson"))
	if errGlob != nil || len(files) < 2 {
		t.Fatalf("rotated files = %d, err=%v", len(files), errGlob)
	}
}

func contextWithState(state *State) context.Context {
	return context.WithValue(context.Background(), ContextKey, state)
}

func waitForEvents(t *testing.T, recorder *Recorder, sessionID string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		items, err := recorder.ListSessions(sessionID, 10, 0)
		if err == nil && len(items) == 1 && items[0].EventCount >= int64(want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	items, _ := recorder.ListSessions(sessionID, 10, 0)
	if len(items) == 0 {
		t.Fatal("session was not indexed")
	}
	t.Fatalf("event count = %d, want at least %d", items[0].EventCount, want)
}

func TestRawHeaderPayloadIsWritten(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	state := &State{recorder: recorder, sessionID: "raw", sessionSource: "explicit", turnID: "turn", requestID: "request"}
	if err := state.append("request.input", map[string]any{"headers": http.Header{"Authorization": {"Bearer keep-me"}}}); err != nil {
		t.Fatal(err)
	}
	waitForEvents(t, recorder, "raw", 1)
	detail, errGet := recorder.GetSession("raw")
	if errGet != nil {
		t.Fatal(errGet)
	}
	data, _ := recorder.ExportSession("raw")
	if !strings.Contains(string(data), "keep-me") || len(detail.Events) != 1 {
		t.Fatalf("raw payload not preserved: %s", data)
	}
	_ = recorder.Close()
}

func TestRequestSemanticEventsAreRecorded(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://example.test/v1/messages", bytes.NewBufferString(`{"messages":[{"role":"assistant","thinking":"reason"},{"role":"user","tool_result":{"content":"done"}}]}`))
	state := NewState(recorder, req, []byte(`{"messages":[{"role":"assistant","thinking":"reason"},{"role":"user","tool_result":{"content":"done"}}]}`))
	if err := state.Begin(); err != nil {
		t.Fatal(err)
	}
	waitForEvents(t, recorder, state.sessionID, 3)
	detail, errGet := recorder.GetSession(state.sessionID)
	if errGet != nil {
		t.Fatal(errGet)
	}
	var reasoning, toolResult bool
	for _, event := range detail.Events {
		reasoning = reasoning || event.Kind == "reasoning"
		toolResult = toolResult || event.Kind == "tool.result"
	}
	if !reasoning || !toolResult {
		t.Fatalf("semantic events missing: reasoning=%v tool_result=%v", reasoning, toolResult)
	}
	_ = recorder.Close()
}

func TestRecorderEnforcesMaxBytesForFinalizedFiles(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir(), MaxFileBytes: 256, MaxBytes: 900})
	if errNew != nil {
		t.Fatal(errNew)
	}
	for i := 0; i < 24; i++ {
		session := "capacity-a"
		if i%2 == 1 {
			session = "capacity-b"
		}
		if err := recorder.Append(Event{SchemaVersion: 1, EventID: fmt.Sprintf("capacity-%d", i), Timestamp: time.Now(), SessionID: session, SessionSource: "single-request", TurnID: fmt.Sprintf("turn-%d", i), Kind: "stream.chunk", Payload: []byte(`{"raw":"012345678901234567890123456789"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	var total int64
	_ = filepath.WalkDir(recorder.RootDir(), func(path string, entry fs.DirEntry, err error) error {
		if err == nil && entry != nil && !entry.IsDir() && strings.HasSuffix(path, ".ndjson") {
			if info, statErr := entry.Info(); statErr == nil {
				total += info.Size()
			}
		}
		return nil
	})
	if total > recorder.MaxBytes() {
		t.Fatalf("trace body bytes = %d, max = %d", total, recorder.MaxBytes())
	}
	_ = recorder.Close()
}

func TestSessionIDFallsBackToCPAIdentity(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "http://example.test/v1/chat/completions", bytes.NewBufferString(`{"model":"gpt-test","messages":[{"role":"user","content":"same root"}]}`))
	first := NewState(&Recorder{}, req, []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"same root"}]}`))
	if first == nil {
		t.Fatal("expected state")
	}
	if first.sessionSource != "cpa-derived" || first.sessionID == "" {
		t.Fatalf("session fallback = (%q, %q), want cpa-derived identity", first.sessionID, first.sessionSource)
	}
}
