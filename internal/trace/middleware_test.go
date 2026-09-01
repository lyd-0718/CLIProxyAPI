package trace

import (
	"net/http"
	"testing"
)

func TestIsTraceableRequestAllowsModelInvocationsOnly(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   bool
	}{
		{method: http.MethodPost, path: "/v1/chat/completions", want: true},
		{method: http.MethodPost, path: "/v1/completions", want: true},
		{method: http.MethodPost, path: "/v1/messages", want: true},
		{method: http.MethodPost, path: "/v1/responses", want: true},
		{method: http.MethodGet, path: "/v1/responses", want: true},
		{method: http.MethodPost, path: "/v1/responses/compact", want: true},
		{method: http.MethodPost, path: "/v1/images/generations", want: true},
		{method: http.MethodPost, path: "/v1/videos/generations", want: true},
		{method: http.MethodPost, path: "/backend-api/codex/responses", want: true},
		{method: http.MethodGet, path: "/backend-api/codex/responses", want: true},
		{method: http.MethodPost, path: "/v1beta/interactions", want: true},
		{method: http.MethodPost, path: "/v1beta/models/gemini-pro:generateContent", want: true},
		{method: http.MethodPost, path: "/v1beta/models/gemini-pro:streamGenerateContent", want: true},
		{method: http.MethodPost, path: "/v1/realtime", want: true},
		{method: http.MethodGet, path: "/v1/realtime", want: true},

		{method: http.MethodGet, path: "/v1/models", want: false},
		{method: http.MethodGet, path: "/v1/models/", want: false},
		{method: http.MethodGet, path: "/v1/models/gpt-4", want: false},
		{method: http.MethodGet, path: "/openai/v1/models", want: false},
		{method: http.MethodGet, path: "/v1beta/models", want: false},
		{method: http.MethodGet, path: "/v1beta/models/gemini-pro", want: false},
		{method: http.MethodPost, path: "/v1/messages/count_tokens", want: false},
		{method: http.MethodPost, path: "/v1/alpha/search", want: false},
		{method: http.MethodGet, path: "/v1/videos/abc", want: false},
		{method: http.MethodGet, path: "/openai/v1/videos/abc/content", want: false},
		{method: http.MethodPost, path: "/v1/realtime/client_secrets", want: false},
		{method: http.MethodPost, path: "/v1/realtime/sessions", want: false},
		{method: http.MethodGet, path: "/v1/realtime/calls/abc", want: false},
		{method: http.MethodPost, path: "/v1/realtime/calls/abc/hangup", want: false},
		{method: http.MethodGet, path: "/healthz", want: false},
		{method: http.MethodGet, path: "/v0/management/traces", want: false},
	}
	for _, test := range tests {
		if got := isTraceableRequest(test.method, test.path); got != test.want {
			t.Fatalf("isTraceableRequest(%q, %q) = %v, want %v", test.method, test.path, got, test.want)
		}
	}
}
