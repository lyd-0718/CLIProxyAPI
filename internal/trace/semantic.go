package trace

import (
	"bytes"
	"encoding/json"
	"strings"
)

func emitSemanticEventsFrom(state *State, body []byte, source string) {
	if state == nil || len(body) == 0 {
		return
	}
	skipHistory := source == "request.input" || source == "upstream.request"
	var emit func(any)
	emit = func(value any) {
		switch node := value.(type) {
		case map[string]any:
			semanticEvents, objectKinds := semanticEventsForObject(node)
			for _, item := range semanticEvents {
				state.appendSemantic(item.kind, item.field, item.payload, source)
			}
			for key, child := range node {
				lower := strings.ToLower(strings.TrimSpace(key))
				if skipHistory && isConversationHistoryKey(lower) {
					continue
				}
				if kind := semanticFieldKind(lower); kind != "" && !objectKinds[kind] {
					state.appendSemantic(kind, key, child, source)
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

func (s *State) appendSemantic(kind, field string, value any, source string) {
	if s == nil || kind == "" {
		return
	}
	key := kind + "\x00" + source + "\x00" + semanticIdentity(kind, value)
	s.mu.Lock()
	if s.semanticSeen == nil {
		s.semanticSeen = map[string]bool{}
	}
	if s.semanticSeen[key] {
		s.mu.Unlock()
		return
	}
	s.semanticSeen[key] = true
	s.mu.Unlock()
	_ = s.append(kind, semanticPayload(field, value, source))
}

func isConversationHistoryKey(key string) bool {
	switch key {
	case "messages", "contents", "input", "conversation", "system":
		return true
	default:
		return false
	}
}

func semanticIdentity(kind string, value any) string {
	if kind == "reasoning" {
		return "reasoning"
	}
	if id := toolIdentity(value); id != "" {
		return id
	}
	return kind
}

func toolIdentity(value any) string {
	switch node := value.(type) {
	case map[string]any:
		for _, key := range []string{"id", "tool_use_id", "tool_call_id", "call_id", "toolCallId", "toolUseId"} {
			if text := strings.TrimSpace(stringValue(node[key])); text != "" {
				return text
			}
		}
		for _, child := range node {
			if id := toolIdentity(child); id != "" {
				return id
			}
		}
	case []any:
		for _, child := range node {
			if id := toolIdentity(child); id != "" {
				return id
			}
		}
	}
	return ""
}

func semanticPayload(field string, value any, source string) map[string]any {
	payload := map[string]any{"field": field, "value": value}
	if strings.TrimSpace(source) != "" {
		payload["source"] = source
	}
	return payload
}

type semanticEvent struct {
	kind    string
	field   string
	payload any
}

func semanticEventsForObject(node map[string]any) ([]semanticEvent, map[string]bool) {
	if node == nil {
		return nil, nil
	}
	events := make([]semanticEvent, 0, 2)
	objectKinds := make(map[string]bool)
	for key, value := range node {
		lower := strings.ToLower(strings.TrimSpace(key))
		if lower != "type" {
			continue
		}
		if kind := semanticTypeKind(value); kind != "" {
			events = append(events, semanticEvent{kind: kind, field: key, payload: node})
			objectKinds[kind] = true
		}
	}
	for key, value := range node {
		lower := strings.ToLower(strings.TrimSpace(key))
		if lower != "role" || !strings.EqualFold(strings.TrimSpace(stringValue(value)), "tool") {
			continue
		}
		events = append(events, semanticEvent{kind: "tool.result", field: key, payload: node})
		objectKinds["tool.result"] = true
	}
	return events, objectKinds
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
	case "reasoning", "thinking", "thought", "reasoning_content":
		return "reasoning"
	case "tool_use", "tool_call", "function_call", "custom_tool_call":
		return "tool.call"
	case "tool_result", "function_call_output", "function_response", "tool_response", "custom_tool_call_output":
		return "tool.result"
	default:
		return ""
	}
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}
