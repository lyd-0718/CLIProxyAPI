package trace

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
	clipsession "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/session"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

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
