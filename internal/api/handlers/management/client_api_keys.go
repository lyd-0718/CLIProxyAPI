package management

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/access/clientkeys"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

type clientAPIKeyView struct {
	Name         string `json:"name"`
	Key          string `json:"key"`
	MaskedKey    string `json:"masked_key"`
	RawKey       string `json:"raw_key,omitempty"`
	Concurrency  int    `json:"concurrency"`
	InFlight     int    `json:"in_flight"`
	Requests     int64  `json:"requests"`
	Success      int64  `json:"success"`
	Failed       int64  `json:"failed"`
	InputTokens  int64  `json:"input_tokens"`
	OutputTokens int64  `json:"output_tokens"`
	ExpiresAt    string `json:"expires_at,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
	Disabled     bool   `json:"disabled"`
	Status       string `json:"status"`
}

func (h *Handler) GetClientAPIKeys(c *gin.Context) {
	h.mu.Lock()
	cfg := h.cfg
	h.mu.Unlock()
	if cfg == nil {
		c.JSON(http.StatusOK, gin.H{"keys": []clientAPIKeyView{}})
		return
	}
	c.JSON(http.StatusOK, gin.H{"keys": buildClientAPIKeyViews(cfg, false)})
}

func (h *Handler) CreateClientAPIKey(c *gin.Context) {
	var body struct {
		Name          string `json:"name"`
		Key           string `json:"key"`
		Concurrency   int    `json:"concurrency"`
		ExpiresInDays *int   `json:"expires_in_days"`
		ExpiresAt     string `json:"expires_at"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	name := strings.TrimSpace(body.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	key := strings.TrimSpace(body.Key)
	if key == "" {
		generated, errGen := generateClientAPIKey()
		if errGen != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate key"})
			return
		}
		key = generated
	}
	if body.Concurrency < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "concurrency must be >= 0"})
		return
	}
	expiresAt := strings.TrimSpace(body.ExpiresAt)
	if body.ExpiresInDays != nil && *body.ExpiresInDays > 0 {
		expiresAt = time.Now().UTC().Add(time.Duration(*body.ExpiresInDays) * 24 * time.Hour).Format(time.RFC3339)
	}
	h.mu.Lock()
	if h.cfg == nil {
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config unavailable"})
		return
	}
	for _, existing := range h.cfg.APIKeys {
		if strings.TrimSpace(existing) == key {
			h.mu.Unlock()
			c.JSON(http.StatusConflict, gin.H{"error": "api key already exists"})
			return
		}
	}
	h.cfg.APIKeys = append(h.cfg.APIKeys, key)
	created := config.APIKeyProfile{
		Name:        name,
		Key:         key,
		Concurrency: body.Concurrency,
		ExpiresAt:   expiresAt,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}
	h.cfg.APIKeyProfiles = append(h.cfg.APIKeyProfiles, created)
	snapshot, ok := h.saveConfigAndSnapshotLocked(c)
	h.mu.Unlock()
	if !ok {
		return
	}
	var reqCtx context.Context
	if c != nil && c.Request != nil {
		reqCtx = c.Request.Context()
	}
	h.reloadConfigAfterManagementSaveAsync(reqCtx, snapshot)
	c.JSON(http.StatusOK, gin.H{"key": clientAPIKeyViewFromProfile(created, true)})
}

func (h *Handler) PatchClientAPIKey(c *gin.Context) {
	var body struct {
		Key         string  `json:"key"`
		Name        *string `json:"name"`
		Concurrency *int    `json:"concurrency"`
		ExpiresAt   *string `json:"expires_at"`
		Disabled    *bool   `json:"disabled"`
		ResetUsage  *bool   `json:"reset_usage"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || strings.TrimSpace(body.Key) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}
	key := strings.TrimSpace(body.Key)
	h.mu.Lock()
	if h.cfg == nil {
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config unavailable"})
		return
	}
	if !clientKeyExists(h.cfg, key) {
		h.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"error": "api key not found"})
		return
	}
	if body.ResetUsage != nil && *body.ResetUsage {
		clientkeys.Reset(key)
	}
	configChanged := body.Name != nil || body.Concurrency != nil || body.ExpiresAt != nil || body.Disabled != nil
	if !configChanged {
		h.mu.Unlock()
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
		return
	}
	profile := findOrCreateProfileLocked(h.cfg, key)
	if body.Name != nil {
		profile.Name = strings.TrimSpace(*body.Name)
	}
	if body.Concurrency != nil {
		if *body.Concurrency < 0 {
			h.mu.Unlock()
			c.JSON(http.StatusBadRequest, gin.H{"error": "concurrency must be >= 0"})
			return
		}
		profile.Concurrency = *body.Concurrency
	}
	if body.ExpiresAt != nil {
		profile.ExpiresAt = strings.TrimSpace(*body.ExpiresAt)
	}
	if body.Disabled != nil {
		profile.Disabled = *body.Disabled
	}
	ok := h.persistLocked(c)
	h.mu.Unlock()
	if !ok {
		return
	}
}

func (h *Handler) DeleteClientAPIKey(c *gin.Context) {
	key := strings.TrimSpace(c.Query("key"))
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}
	h.mu.Lock()
	if h.cfg == nil {
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config unavailable"})
		return
	}
	if !clientKeyExists(h.cfg, key) {
		h.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"error": "api key not found"})
		return
	}
	nextKeys := h.cfg.APIKeys[:0]
	for _, existing := range h.cfg.APIKeys {
		if strings.TrimSpace(existing) != key {
			nextKeys = append(nextKeys, existing)
		}
	}
	h.cfg.APIKeys = nextKeys
	nextProfiles := h.cfg.APIKeyProfiles[:0]
	for _, profile := range h.cfg.APIKeyProfiles {
		if strings.TrimSpace(profile.Key) != key {
			nextProfiles = append(nextProfiles, profile)
		}
	}
	h.cfg.APIKeyProfiles = nextProfiles
	ok := h.persistLocked(c)
	h.mu.Unlock()
	if !ok {
		return
	}
}

func buildClientAPIKeyViews(cfg *config.Config, includeRaw bool) []clientAPIKeyView {
	profiles := map[string]config.APIKeyProfile{}
	for _, profile := range cfg.APIKeyProfiles {
		key := strings.TrimSpace(profile.Key)
		if key == "" {
			continue
		}
		profiles[key] = profile
	}
	views := make([]clientAPIKeyView, 0, len(cfg.APIKeys))
	seen := map[string]struct{}{}
	for _, raw := range cfg.APIKeys {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		profile := profiles[key]
		profile.Key = key
		views = append(views, clientAPIKeyViewFromProfile(profile, includeRaw))
	}
	return views
}

func clientAPIKeyViewFromProfile(profile config.APIKeyProfile, includeRaw bool) clientAPIKeyView {
	stats := clientkeys.Snapshot(profile.Key)
	status := "active"
	if profile.Disabled {
		status = "disabled"
	} else if clientkeys.ProfileExpired(profile.ExpiresAt, time.Now()) {
		status = "expired"
	}
	view := clientAPIKeyView{
		Name:         profile.Name,
		Key:          profile.Key,
		MaskedKey:    maskClientAPIKey(profile.Key),
		Concurrency:  profile.Concurrency,
		InFlight:     stats.InFlight,
		Requests:     stats.Requests,
		Success:      stats.Success,
		Failed:       stats.Failed,
		InputTokens:  stats.InputTokens,
		OutputTokens: stats.OutputTokens,
		ExpiresAt:    profile.ExpiresAt,
		CreatedAt:    profile.CreatedAt,
		Disabled:     profile.Disabled,
		Status:       status,
	}
	if includeRaw {
		view.RawKey = profile.Key
	}
	return view
}

func clientKeyExists(cfg *config.Config, key string) bool {
	for _, existing := range cfg.APIKeys {
		if strings.TrimSpace(existing) == key {
			return true
		}
	}
	return false
}

func findOrCreateProfileLocked(cfg *config.Config, key string) *config.APIKeyProfile {
	for i := range cfg.APIKeyProfiles {
		if strings.TrimSpace(cfg.APIKeyProfiles[i].Key) == key {
			return &cfg.APIKeyProfiles[i]
		}
	}
	cfg.APIKeyProfiles = append(cfg.APIKeyProfiles, config.APIKeyProfile{Key: key, CreatedAt: time.Now().UTC().Format(time.RFC3339)})
	return &cfg.APIKeyProfiles[len(cfg.APIKeyProfiles)-1]
}

func maskClientAPIKey(key string) string {
	if len(key) <= 8 {
		return strings.Repeat("*", len(key))
	}
	return key[:4] + "••••" + key[len(key)-4:]
}

func generateClientAPIKey() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "sk-" + hex.EncodeToString(buf), nil
}

func pruneAPIKeyProfiles(cfg *config.Config) {
	if cfg == nil {
		return
	}
	allowed := make(map[string]struct{}, len(cfg.APIKeys))
	for _, raw := range cfg.APIKeys {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		allowed[key] = struct{}{}
	}
	next := cfg.APIKeyProfiles[:0]
	for _, profile := range cfg.APIKeyProfiles {
		if _, ok := allowed[strings.TrimSpace(profile.Key)]; ok {
			next = append(next, profile)
		}
	}
	cfg.APIKeyProfiles = next
}
