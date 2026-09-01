package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/access/clientkeys"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestMaskClientAPIKey(t *testing.T) {
	t.Parallel()
	if got := maskClientAPIKey("abcd"); got != "****" {
		t.Fatalf("short key mask = %q", got)
	}
	if got := maskClientAPIKey("sk-abcdefghijklmnop"); got != "sk-a••••mnop" {
		t.Fatalf("mask = %q", got)
	}
}

func TestGenerateClientAPIKey(t *testing.T) {
	t.Parallel()
	key, err := generateClientAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key, "sk-") || len(key) != 51 {
		t.Fatalf("generated key %q", key)
	}
}

func TestGetClientAPIKeysIncludesUsageAndStatus(t *testing.T) {
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	clientkeys.Reset("live-key")
	clientkeys.RecordFinish("live-key", false)
	clientkeys.AddTokens("live-key", 3, 7)

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				APIKeys: []string{"live-key", "expired-key"},
				APIKeyProfiles: []config.APIKeyProfile{
					{Name: "studio", Key: "live-key", Concurrency: 4, CreatedAt: "2026-01-01T00:00:00Z"},
					{Name: "old", Key: "expired-key", ExpiresAt: past},
				},
			},
		},
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/v0/management/client-api-keys", nil)
	h.GetClientAPIKeys(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Keys []clientAPIKeyView `json:"keys"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Keys) != 2 {
		t.Fatalf("keys len = %d", len(payload.Keys))
	}
	live := payload.Keys[0]
	if live.Key != "live-key" || live.MaskedKey == live.Key || live.Name != "studio" {
		t.Fatalf("live key view = %+v", live)
	}
	if live.Status != "active" || live.Success != 1 || live.InputTokens != 3 || live.OutputTokens != 7 {
		t.Fatalf("live usage/status = %+v", live)
	}
	if payload.Keys[1].Status != "expired" {
		t.Fatalf("expired status = %q", payload.Keys[1].Status)
	}
}

func TestCreateClientAPIKeyPersistsProfile(t *testing.T) {
	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/client-api-keys", strings.NewReader(`{"name":"bot","concurrency":2}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.CreateClientAPIKey(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Key clientAPIKeyView `json:"key"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Key.RawKey == "" || !strings.HasPrefix(payload.Key.RawKey, "sk-") {
		t.Fatalf("raw key missing: %+v", payload.Key)
	}
	if len(h.cfg.APIKeys) != 1 || h.cfg.APIKeys[0] != payload.Key.RawKey {
		t.Fatalf("api keys = %#v", h.cfg.APIKeys)
	}
	if len(h.cfg.APIKeyProfiles) != 1 || h.cfg.APIKeyProfiles[0].Name != "bot" || h.cfg.APIKeyProfiles[0].Concurrency != 2 {
		t.Fatalf("profiles = %#v", h.cfg.APIKeyProfiles)
	}
}

func TestPatchClientAPIKeyResetUsage(t *testing.T) {
	clientkeys.Reset("patch-key")
	clientkeys.RecordFinish("patch-key", false)
	h := &Handler{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				APIKeys: []string{"patch-key"},
			},
		},
		configFilePath: writeTestConfigFile(t),
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/client-api-keys", strings.NewReader(`{"key":"patch-key","reset_usage":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.PatchClientAPIKey(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if stats := clientkeys.Snapshot("patch-key"); stats.Requests != 0 {
		t.Fatalf("requests after reset = %d", stats.Requests)
	}
}

func TestDeleteClientAPIKeyRemovesProfile(t *testing.T) {
	h := &Handler{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				APIKeys:        []string{"keep", "drop"},
				APIKeyProfiles: []config.APIKeyProfile{{Name: "gone", Key: "drop"}, {Name: "stay", Key: "keep"}},
			},
		},
		configFilePath: writeTestConfigFile(t),
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/client-api-keys?key=drop", nil)
	h.DeleteClientAPIKey(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(h.cfg.APIKeys) != 1 || h.cfg.APIKeys[0] != "keep" {
		t.Fatalf("api keys = %#v", h.cfg.APIKeys)
	}
	if len(h.cfg.APIKeyProfiles) != 1 || h.cfg.APIKeyProfiles[0].Key != "keep" {
		t.Fatalf("profiles = %#v", h.cfg.APIKeyProfiles)
	}
}

func TestPruneAPIKeyProfiles(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{
		SDKConfig: config.SDKConfig{
			APIKeys: []string{"keep"},
			APIKeyProfiles: []config.APIKeyProfile{
				{Key: "keep"},
				{Key: "orphan"},
			},
		},
	}
	pruneAPIKeyProfiles(cfg)
	if len(cfg.APIKeyProfiles) != 1 || cfg.APIKeyProfiles[0].Key != "keep" {
		t.Fatalf("profiles = %#v", cfg.APIKeyProfiles)
	}
}
