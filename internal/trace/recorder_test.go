package trace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
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
	if detail.Incomplete || detail.FilePath == "" || detail.Bytes <= 0 {
		t.Fatalf("session aggregate = incomplete:%v file:%q bytes:%d, want complete indexed session", detail.Incomplete, detail.FilePath, detail.Bytes)
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
	exported, errExport := recorder.ExportSession("session-test")
	if errExport != nil || !strings.Contains(string(exported), `"token_breakdown"`) {
		t.Fatalf("usage token breakdown was not preserved: err=%v trace=%s", errExport, exported)
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

func TestRecorderUsesAsiaShanghaiDateBoundary(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	// 16:30 UTC is already the next calendar day in Asia/Shanghai.
	eventTime := time.Date(2026, time.August, 30, 16, 30, 0, 0, time.UTC)
	if errAppend := recorder.Append(Event{
		SchemaVersion: 1,
		EventID:       "timezone-boundary",
		Timestamp:     eventTime,
		SessionID:     "timezone-boundary",
		SessionSource: "single-request",
		TurnID:        "timezone-turn",
		Kind:          "request.input",
	}); errAppend != nil {
		t.Fatal(errAppend)
	}
	waitForEvents(t, recorder, "timezone-boundary", 1)
	if errClose := recorder.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	files, errGlob := filepath.Glob(filepath.Join(recorder.RootDir(), "2026-08-31", "*.ndjson"))
	if errGlob != nil || len(files) != 1 {
		t.Fatalf("trace files = %v, err=%v; want one file under 2026-08-31", files, errGlob)
	}
}

func TestRecorderReportsWriteFailureToCallerAndHealth(t *testing.T) {
	root := t.TempDir()
	eventTime := time.Date(2026, time.August, 31, 1, 0, 0, 0, time.UTC)
	datePath := filepath.Join(root, eventTime.In(traceLocation).Format("2006-01-02"))
	if errWrite := os.WriteFile(datePath, []byte("directory placeholder"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: root})
	if errNew != nil {
		t.Fatal(errNew)
	}
	errAppend := recorder.Append(Event{
		SchemaVersion: 1,
		EventID:       "write-failure",
		Timestamp:     eventTime,
		SessionID:     "write-failure",
		SessionSource: "single-request",
		TurnID:        "write-failure-turn",
		Kind:          "request.input",
	})
	if errAppend == nil {
		t.Fatal("Append should report a trace write failure")
	}
	stats, errStats := recorder.Stats()
	if errStats != nil {
		t.Fatal(errStats)
	}
	if got, _ := stats["write_errors"].(int64); got == 0 {
		t.Fatalf("health stats did not record write error: %#v", stats)
	}
	if _, ok := stats["last_write_error"].(string); !ok {
		t.Fatalf("health stats missing last_write_error: %#v", stats)
	}
	if errClose := recorder.Close(); errClose != nil {
		t.Fatal(errClose)
	}
}

func contextWithState(state *State) context.Context {
	return context.WithValue(context.Background(), ContextKey, state)
}

func waitForEvents(t *testing.T, recorder *Recorder, sessionID string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		detail, err := recorder.GetSession(sessionID)
		if err == nil && detail.EventCount >= int64(want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	detail, err := recorder.GetSession(sessionID)
	if err != nil {
		t.Fatal("session was not indexed")
	}
	t.Fatalf("event count = %d, want at least %d", detail.EventCount, want)
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

func TestUpstreamResponseBodyIsCapturedAsRawPayload(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://example.test/v1/chat/completions", nil)
	state := NewState(recorder, req, nil)
	ctx := contextWithState(state)
	response := &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"choices":[{"delta":{"content":"ok"}}]}`)),
	}
	wrapped := WrapUpstreamResponseBody(ctx, response)
	if _, errRead := io.ReadAll(wrapped); errRead != nil {
		t.Fatal(errRead)
	}
	if errClose := wrapped.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	waitForEvents(t, recorder, state.sessionID, 1)
	exported, errExport := recorder.ExportSession(state.sessionID)
	if errExport != nil {
		t.Fatal(errExport)
	}
	if !strings.Contains(string(exported), `"status":200`) || !strings.Contains(string(exported), `"content":"ok"`) {
		t.Fatalf("upstream response was not recorded as raw JSON: %s", exported)
	}
	_ = recorder.Close()
}

func TestUpstreamSemanticEventsIncludeAllFieldsAndSource(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://example.test/v1/chat/completions", nil)
	state := NewState(recorder, req, nil)
	ctx := contextWithState(state)
	RecordUpstreamRequestForContext(ctx, UpstreamRequest{
		Body: []byte(`{"reasoning_content":"reason","tool_calls":[{"id":"call-1"}]}`),
	})
	RecordUpstreamResponseForContext(ctx, UpstreamResponse{
		Status: 200,
		Body:   []byte(`{"type":"function_call_output","call_id":"call-1","output":"done"}`),
	})
	waitForEvents(t, recorder, state.sessionID, 5)
	detail, errGet := recorder.GetSession(state.sessionID)
	if errGet != nil {
		t.Fatal(errGet)
	}
	var reasoning, toolCall, toolResult bool
	for _, event := range detail.Events {
		if event.Kind == "reasoning" {
			reasoning = true
		}
		if event.Kind == "tool.call" {
			toolCall = true
		}
		if event.Kind == "tool.result" {
			toolResult = true
		}
		if event.Kind == "reasoning" || event.Kind == "tool.call" || event.Kind == "tool.result" {
			var payload map[string]any
			if errDecode := json.Unmarshal(event.Payload, &payload); errDecode != nil {
				t.Fatal(errDecode)
			}
			wantSource := "upstream.request"
			if event.Kind == "tool.result" {
				wantSource = "upstream.response"
			}
			if payload["source"] != wantSource {
				t.Fatalf("semantic event source = %v for %s, want %s", payload["source"], event.Kind, wantSource)
			}
		}
	}
	if !reasoning || !toolCall || !toolResult {
		t.Fatalf("upstream semantic events missing: reasoning=%v tool_call=%v tool_result=%v", reasoning, toolCall, toolResult)
	}
	_ = recorder.Close()
}

func TestInterruptedUpstreamResponseMarksTraceIncomplete(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://example.test/v1/chat/completions", nil)
	state := NewState(recorder, req, nil)
	ctx := contextWithState(state)
	RecordUpstreamResponseForContext(ctx, UpstreamResponse{Status: 200, Error: "connection reset"})
	state.Finish(http.StatusBadGateway, nil)
	waitForEvents(t, recorder, state.sessionID, 4)
	detail, errGet := recorder.GetSession(state.sessionID)
	if errGet != nil {
		t.Fatal(errGet)
	}
	if !detail.Incomplete {
		t.Fatal("session should be marked incomplete after upstream interruption")
	}
	sawCompleted := false
	for _, event := range detail.Events {
		if event.Kind == "upstream.response" && !event.Incomplete {
			t.Fatal("interrupted upstream response event should be marked incomplete")
		}
		if event.Kind == "turn.completed" {
			sawCompleted = true
			var payload map[string]any
			if errDecode := json.Unmarshal(event.Payload, &payload); errDecode != nil {
				t.Fatal(errDecode)
			}
			if incomplete, _ := payload["incomplete"].(bool); !incomplete {
				t.Fatalf("turn.completed payload incomplete = %v, want true", payload["incomplete"])
			}
		}
	}
	if !sawCompleted {
		t.Fatal("missing turn.completed event")
	}
	_ = recorder.Close()
}

func TestRecorderPersistsEventsAcrossRestart(t *testing.T) {
	root := t.TempDir()
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: root})
	if errNew != nil {
		t.Fatal(errNew)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://example.test/v1/chat/completions", nil)
	state := NewState(recorder, req, nil)
	if errAppend := state.append("request.input", map[string]any{"body": map[string]any{"model": "test"}}); errAppend != nil {
		t.Fatal(errAppend)
	}
	waitForEvents(t, recorder, state.sessionID, 1)
	if errClose := recorder.Close(); errClose != nil {
		t.Fatal(errClose)
	}

	restarted, errRestart := NewRecorder(Config{Enabled: true, RootDir: root})
	if errRestart != nil {
		t.Fatal(errRestart)
	}
	detail, errGet := restarted.GetSession(state.sessionID)
	if errGet != nil {
		t.Fatal(errGet)
	}
	if len(detail.Events) != 1 || detail.Events[0].Kind != "request.input" {
		t.Fatalf("persisted events = %#v", detail.Events)
	}
	_ = restarted.Close()
}

func TestRecorderRepairsAggregateRowsOnRestart(t *testing.T) {
	root := t.TempDir()
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: root})
	if errNew != nil {
		t.Fatal(errNew)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://example.test/v1/chat/completions", nil)
	state := NewState(recorder, req, nil)
	if errAppend := state.append("request.input", map[string]any{"body": map[string]any{"model": "repair-test"}}); errAppend != nil {
		t.Fatal(errAppend)
	}
	state.RecordUsage(coreusage.Record{Model: "repair-test"})
	waitForEvents(t, recorder, state.sessionID, 2)
	if _, errCorrupt := recorder.db.Exec("UPDATE sessions SET incomplete = 999, file_path = '0' WHERE session_id = ?", state.sessionID); errCorrupt != nil {
		t.Fatal(errCorrupt)
	}
	if _, errCorrupt := recorder.db.Exec("UPDATE turns SET incomplete = 999 WHERE turn_id = ?", state.turnID); errCorrupt != nil {
		t.Fatal(errCorrupt)
	}
	if errClose := recorder.Close(); errClose != nil {
		t.Fatal(errClose)
	}

	restarted, errRestart := NewRecorder(Config{Enabled: true, RootDir: root})
	if errRestart != nil {
		t.Fatal(errRestart)
	}
	detail, errGet := restarted.GetSession(state.sessionID)
	if errGet != nil {
		t.Fatal(errGet)
	}
	if detail.Incomplete || detail.FilePath == "" || detail.FilePath == "0" {
		t.Fatalf("restarted aggregate = incomplete:%v file:%q, want rebuilt values", detail.Incomplete, detail.FilePath)
	}
	turns, errTurns := restarted.ListTurns(Filter{Model: "repair-test"}, 10, 0)
	if errTurns != nil || len(turns) != 1 || turns[0].Incomplete {
		t.Fatalf("restarted turn aggregate = err:%v turns:%#v", errTurns, turns)
	}
	_ = restarted.Close()
}

func TestTraceAnalyticsListRequestsAndOverview(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	for index, sessionID := range []string{"analytics-a", "analytics-b"} {
		turnID := fmt.Sprintf("turn-%d", index)
		if err := recorder.Append(Event{SchemaVersion: 1, EventID: fmt.Sprintf("analytics-request-%d", index), Timestamp: now.Add(time.Duration(index) * time.Minute), SessionID: sessionID, SessionSource: "explicit", TurnID: turnID, RequestID: fmt.Sprintf("request-%d", index), Kind: "request.input", Payload: []byte(`{"model":"gpt-test"}`)}); err != nil {
			t.Fatal(err)
		}
		if err := recorder.Append(Event{SchemaVersion: 1, EventID: fmt.Sprintf("analytics-usage-%d", index), Timestamp: now.Add(time.Duration(index)*time.Minute + time.Second), SessionID: sessionID, SessionSource: "explicit", TurnID: turnID, Kind: "usage", Payload: []byte(fmt.Sprintf(`{"model":"gpt-test","provider":"openai","api_key":"key-%d","input_tokens":10,"output_tokens":4,"reasoning_tokens":2,"cached_tokens":3,"total_tokens":14,"latency_ms":100,"ttft_ms":20}`, index))}); err != nil {
			t.Fatal(err)
		}
		if err := recorder.Append(Event{SchemaVersion: 1, EventID: fmt.Sprintf("analytics-complete-%d", index), Timestamp: now.Add(time.Duration(index)*time.Minute + 2*time.Second), SessionID: sessionID, SessionSource: "explicit", TurnID: turnID, Kind: "turn.completed", Payload: []byte(`{"status":200,"failed":false,"incomplete":false}`)}); err != nil {
			t.Fatal(err)
		}
	}
	waitForEvents(t, recorder, "analytics-a", 3)
	waitForEvents(t, recorder, "analytics-b", 3)
	requests, errRequests := recorder.ListTurns(Filter{Model: "gpt-test"}, 10, 0)
	if errRequests != nil {
		t.Fatal(errRequests)
	}
	if len(requests) != 2 || requests[0].InputTokens != 10 || requests[0].CachedTokens != 3 || !requests[0].TraceAvailable {
		t.Fatalf("requests = %#v", requests)
	}
	totalRequests, errCountRequests := recorder.CountTurns(Filter{Model: "gpt-test"})
	if errCountRequests != nil || totalRequests != 2 {
		t.Fatalf("request count = %d, err = %v", totalRequests, errCountRequests)
	}
	overview, errOverview := recorder.GetOverview(Filter{From: now.Add(-time.Second), To: now.Add(3 * time.Minute)})
	if errOverview != nil {
		t.Fatal(errOverview)
	}
	if overview.Sessions != 2 || overview.Turns != 2 || overview.Events != 6 || overview.InputTokens != 20 || overview.CachedTokens != 6 {
		t.Fatalf("overview = %#v", overview)
	}
	if len(overview.Models) != 1 || overview.Models[0].Name != "gpt-test" || len(overview.APIKeys) != 2 || len(overview.Providers) != 1 || len(overview.Timeline) == 0 {
		t.Fatalf("overview breakdowns = %#v", overview)
	}
	_ = recorder.Close()
}

func TestLegacyKeeperRequestsStayOutOfSessionTraces(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	events := []Event{
		{SchemaVersion: 1, EventID: "legacy-request", Timestamp: now, SessionID: "legacy-keeper-request-1", SessionSource: legacyKeeperSource, TurnID: "legacy-turn", RequestID: "legacy-request", Kind: "request.input", Payload: []byte(`{"model":"gpt-legacy"}`)},
		{SchemaVersion: 1, EventID: "legacy-usage", Timestamp: now.Add(time.Second), SessionID: "legacy-keeper-request-1", SessionSource: legacyKeeperSource, TurnID: "legacy-turn", RequestID: "legacy-request", Kind: "usage", Payload: []byte(`{"model":"gpt-legacy","provider":"openai","input_tokens":12,"output_tokens":3,"total_tokens":15}`)},
		{SchemaVersion: 1, EventID: "legacy-complete", Timestamp: now.Add(2 * time.Second), SessionID: "legacy-keeper-request-1", SessionSource: legacyKeeperSource, TurnID: "legacy-turn", RequestID: "legacy-request", Kind: "turn.completed", Payload: []byte(`{"status":200}`)},
	}
	for _, event := range events {
		if err := recorder.Append(event); err != nil {
			t.Fatal(err)
		}
	}
	waitForEvents(t, recorder, "legacy-keeper-request-1", len(events))

	requests, errRequests := recorder.ListTurns(Filter{}, 10, 0)
	if errRequests != nil || len(requests) != 1 || requests[0].TraceAvailable {
		t.Fatalf("legacy requests = %#v, err = %v", requests, errRequests)
	}
	requestCount, errRequestCount := recorder.CountTurns(Filter{})
	if errRequestCount != nil || requestCount != 1 {
		t.Fatalf("legacy request count = %d, err = %v", requestCount, errRequestCount)
	}
	sessions, errSessions := recorder.ListSessionsFiltered(Filter{}, 10, 0)
	if errSessions != nil || len(sessions) != 0 {
		t.Fatalf("legacy sessions = %#v, err = %v", sessions, errSessions)
	}
	sessionCount, errSessionCount := recorder.CountSessionsFiltered(Filter{})
	if errSessionCount != nil || sessionCount != 0 {
		t.Fatalf("legacy session count = %d, err = %v", sessionCount, errSessionCount)
	}
	overview, errOverview := recorder.GetOverview(Filter{})
	if errOverview != nil {
		t.Fatal(errOverview)
	}
	if overview.Turns != 1 || overview.TotalTokens != 15 || overview.Sessions != 0 || overview.Events != 0 {
		t.Fatalf("legacy overview = %#v", overview)
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

func TestStandardSemanticObjectEventsAreRecorded(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://example.test/v1/messages", nil)
	state := NewState(recorder, req, []byte(`{"content":[{"type":"thinking","thinking":"reason"},{"type":"tool_use","id":"call-1","name":"search"}],"messages":[{"role":"tool","tool_call_id":"call-1","content":"done"}]}`))
	if err := state.Begin(); err != nil {
		t.Fatal(err)
	}
	waitForEvents(t, recorder, state.sessionID, 4)
	detail, errGet := recorder.GetSession(state.sessionID)
	if errGet != nil {
		t.Fatal(errGet)
	}
	var reasoning, toolCall, toolResult bool
	for _, event := range detail.Events {
		switch event.Kind {
		case "reasoning":
			reasoning = true
		case "tool.call":
			toolCall = true
		case "tool.result":
			toolResult = true
		}
	}
	if !reasoning || !toolCall || !toolResult {
		t.Fatalf("standard semantic events missing: reasoning=%v tool_call=%v tool_result=%v", reasoning, toolCall, toolResult)
	}
	_ = recorder.Close()
}

func TestStreamingSemanticEventsAreRecordedFromSSE(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://example.test/v1/messages", nil)
	state := NewState(recorder, req, []byte(`{"model":"stream-model","messages":[{"role":"user","content":"inspect"}]}`))
	if errBegin := state.Begin(); errBegin != nil {
		t.Fatal(errBegin)
	}
	// This combines Anthropic content blocks and OpenAI-compatible delta fields
	// in the same SSE payload shape used by the protocol handlers.
	stream := strings.Join([]string{
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"reason"}}`,
		``,
		`data: {"type":"content_block_start","content_block":{"type":"tool_use","id":"call-1","name":"search"}}`,
		``,
		`data: {"choices":[{"delta":{"reasoning_content":"more","tool_calls":[{"id":"call-1","function":{"name":"search","arguments":"{}"}}]}}]}`,
		``,
		`data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"tool_result":{"tool_call_id":"call-1","content":"done"}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")
	state.AppendDownstreamChunk([]byte(stream))
	state.Finish(http.StatusOK, http.Header{"Content-Type": {"text/event-stream"}})
	waitForEvents(t, recorder, state.sessionID, 7)
	detail, errGet := recorder.GetSession(state.sessionID)
	if errGet != nil {
		t.Fatal(errGet)
	}
	var reasoning, toolCall, toolResult bool
	for _, event := range detail.Events {
		switch event.Kind {
		case "reasoning":
			reasoning = true
		case "tool.call":
			toolCall = true
		case "tool.result":
			toolResult = true
		}
	}
	if !reasoning || !toolCall || !toolResult {
		t.Fatalf("stream semantic events missing: reasoning=%v tool_call=%v tool_result=%v", reasoning, toolCall, toolResult)
	}
	_ = recorder.Close()
}

func TestOpenAIAndAnthropicProtocolTraces(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		model       string
		requestBody string
		outputBody  string
		wantOutput  string
	}{
		{
			name:        "openai non-stream",
			path:        "/v1/chat/completions",
			model:       "gpt-trace-nonstream",
			requestBody: `{"model":"gpt-trace-nonstream","messages":[{"role":"user","content":"lookup"},{"role":"tool","tool_call_id":"call-openai","content":"result"}]}`,
			outputBody:  `{"id":"chatcmpl-trace","object":"chat.completion","model":"gpt-trace-nonstream","choices":[{"index":0,"message":{"role":"assistant","content":"answer","reasoning_content":"think","tool_calls":[{"id":"call-openai","type":"function","function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
			wantOutput:  "answer",
		},
		{
			name:        "openai stream",
			path:        "/v1/chat/completions",
			model:       "gpt-trace-stream",
			requestBody: `{"model":"gpt-trace-stream","messages":[{"role":"user","content":"lookup"},{"role":"tool","tool_call_id":"call-openai-stream","content":"result"}]}`,
			outputBody: strings.Join([]string{
				`data: {"choices":[{"delta":{"reasoning_content":"think"}}]}`,
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-openai-stream","function":{"name":"lookup","arguments":"{}"}}]}}]}`,
				`data: {"choices":[{"delta":{"content":"answer"}}]}`,
				`data: [DONE]`,
			}, "\n"),
			wantOutput: "answer",
		},
		{
			name:        "anthropic non-stream",
			path:        "/v1/messages",
			model:       "claude-trace-nonstream",
			requestBody: `{"model":"claude-trace-nonstream","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-anthropic","content":"result"}]}]}`,
			outputBody:  `{"id":"msg-trace","type":"message","role":"assistant","model":"claude-trace-nonstream","content":[{"type":"thinking","thinking":"think"},{"type":"tool_use","id":"call-anthropic","name":"lookup","input":{}}],"stop_reason":"tool_use"}`,
			wantOutput:  "call-anthropic",
		},
		{
			name:        "anthropic stream",
			path:        "/v1/messages",
			model:       "claude-trace-stream",
			requestBody: `{"model":"claude-trace-stream","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-anthropic-stream","content":"result"}]}]}`,
			outputBody: strings.Join([]string{
				`event: content_block_start`,
				`data: {"type":"content_block_start","content_block":{"type":"thinking","thinking":"think"}}`,
				`data: {"type":"content_block_start","content_block":{"type":"tool_use","id":"call-anthropic-stream","name":"lookup","input":{}}}`,
				`data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"more"}}`,
				`data: {"type":"content_block_delta","delta":{"type":"input_json_delta","partial_json":"{}"}}`,
				`data: {"type":"message_delta","delta":{"stop_reason":"tool_use"}}`,
				`data: [DONE]`,
			}, "\n"),
			wantOutput: "call-anthropic-stream",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
			if errNew != nil {
				t.Fatal(errNew)
			}
			req, errReq := http.NewRequest(http.MethodPost, "http://example.test"+test.path, strings.NewReader(test.requestBody))
			if errReq != nil {
				t.Fatal(errReq)
			}
			state := NewState(recorder, req, []byte(test.requestBody))
			if errBegin := state.Begin(); errBegin != nil {
				t.Fatal(errBegin)
			}
			state.AppendDownstreamChunk([]byte(test.outputBody))
			state.RecordUsage(coreusage.Record{
				Provider: "protocol-test",
				Model:    test.model,
				APIKey:   "trace-test-key",
				Latency:  120 * time.Millisecond,
				TTFT:     30 * time.Millisecond,
				Detail: coreusage.Detail{
					InputTokens:         11,
					OutputTokens:        7,
					ReasoningTokens:     2,
					CachedTokens:        3,
					CacheReadTokens:     3,
					CacheCreationTokens: 1,
					TotalTokens:         18,
				},
			})
			state.Finish(http.StatusOK, http.Header{"Content-Type": {"application/json"}})

			waitForEvents(t, recorder, state.sessionID, 7)
			detail, errGet := recorder.GetSession(state.sessionID)
			if errGet != nil {
				t.Fatal(errGet)
			}
			seen := make(map[string]bool)
			for _, event := range detail.Events {
				seen[event.Kind] = true
			}
			for _, kind := range []string{"request.input", "tool.result", "reasoning", "tool.call", "usage", "model.output", "turn.completed"} {
				if !seen[kind] {
					t.Fatalf("missing %s event in %s trace", kind, test.name)
				}
			}
			exported, errExport := recorder.ExportSession(state.sessionID)
			if errExport != nil || !strings.Contains(string(exported), test.wantOutput) {
				t.Fatalf("model output was not preserved: err=%v trace=%s", errExport, exported)
			}
			turns, errTurns := recorder.ListTurns(Filter{Model: test.model}, 10, 0)
			if errTurns != nil || len(turns) != 1 || turns[0].InputTokens != 11 || turns[0].CachedTokens != 3 || turns[0].TotalTokens != 18 {
				t.Fatalf("usage index was not preserved: err=%v turns=%#v", errTurns, turns)
			}
			_ = recorder.Close()
		})
	}
}

func TestRecorderEnforcesMaxBytesForFinalizedFiles(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir(), MaxFileBytes: 256, MaxBytes: 900, CleanupEnabled: true})
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

func TestRecorderSkipsMaxBytesWhenCleanupDisabled(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir(), MaxFileBytes: 256, MaxBytes: 900})
	if errNew != nil {
		t.Fatal(errNew)
	}
	for i := 0; i < 24; i++ {
		if err := recorder.Append(Event{SchemaVersion: 1, EventID: fmt.Sprintf("keep-%d", i), Timestamp: time.Now(), SessionID: "keep", SessionSource: "single-request", TurnID: fmt.Sprintf("turn-%d", i), Kind: "stream.chunk", Payload: []byte(`{"raw":"012345678901234567890123456789"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	root := recorder.RootDir()
	maxBytes := recorder.MaxBytes()
	if errClose := recorder.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	var total int64
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && entry != nil && !entry.IsDir() && strings.HasSuffix(path, ".ndjson") {
			if info, statErr := entry.Info(); statErr == nil {
				total += info.Size()
			}
		}
		return nil
	})
	if total <= maxBytes {
		t.Fatalf("cleanup disabled should keep bodies over max bytes: total=%d max=%d", total, maxBytes)
	}
}

func TestTurnIndexUsesLastOutcomeAndReasoning(t *testing.T) {
	recorder, errNew := NewRecorder(Config{Enabled: true, RootDir: t.TempDir()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	events := []Event{
		{SchemaVersion: 1, EventID: "req", Timestamp: now, SessionID: "session-retry", SessionSource: "explicit", TurnID: "turn-retry", RequestID: "req-retry", Kind: "request.input", Payload: []byte(`{"model":"kimi-k3"}`)},
		{SchemaVersion: 1, EventID: "usage-fail", Timestamp: now.Add(time.Second), SessionID: "session-retry", SessionSource: "explicit", TurnID: "turn-retry", RequestID: "req-retry", Kind: "usage", Payload: []byte(`{"model":"kimi-k3","provider":"claude","reasoning_effort":"max","failed":true,"failure":{"StatusCode":403,"Body":"{\"error\":{\"type\":\"permission_error\",\"message\":\"weekly usage limit\"}}"}}`)},
		{SchemaVersion: 1, EventID: "usage-ok", Timestamp: now.Add(2 * time.Second), SessionID: "session-retry", SessionSource: "explicit", TurnID: "turn-retry", RequestID: "req-retry", Kind: "usage", Payload: []byte(`{"model":"kimi-k3","provider":"kimi","reasoning_effort":"max","failed":false,"input_tokens":1528,"output_tokens":119,"cached_tokens":39424,"total_tokens":41071}`)},
		{SchemaVersion: 1, EventID: "done", Timestamp: now.Add(3 * time.Second), SessionID: "session-retry", SessionSource: "explicit", TurnID: "turn-retry", RequestID: "req-retry", Kind: "turn.completed", Payload: []byte(`{"status":200,"failed":false,"incomplete":false}`)},
	}
	for _, event := range events {
		if err := recorder.Append(event); err != nil {
			t.Fatal(err)
		}
	}
	waitForEvents(t, recorder, "session-retry", len(events))
	turns, errTurns := recorder.ListTurns(Filter{}, 10, 0)
	if errTurns != nil || len(turns) != 1 {
		t.Fatalf("turns = %#v, err = %v", turns, errTurns)
	}
	item := turns[0]
	if item.Failed || item.Failure != "" {
		t.Fatalf("last successful outcome should clear failed latch: %#v", item)
	}
	if item.Status != 200 || item.ReasoningEffort != "max" || item.Provider != "kimi" {
		t.Fatalf("indexed request = %#v", item)
	}
	wantShare := cachedShare(1528, 39424)
	if item.CachedShare != wantShare || wantShare < 96 {
		t.Fatalf("cached share = %v, want %v", item.CachedShare, wantShare)
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
