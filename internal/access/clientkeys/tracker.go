package clientkeys

import (
	"strings"
	"sync"
	"time"

	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type Stats struct {
	InFlight     int   `json:"in_flight"`
	Requests     int64 `json:"requests"`
	Success      int64 `json:"success"`
	Failed       int64 `json:"failed"`
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

var (
	mu     sync.Mutex
	limits = map[string]int{}
	flight = map[string]int{}
	usage  = map[string]*Stats{}
)

func Configure(cfg *sdkconfig.SDKConfig) {
	next := map[string]int{}
	if cfg != nil {
		for _, profile := range cfg.APIKeyProfiles {
			key := strings.TrimSpace(profile.Key)
			if key == "" {
				continue
			}
			next[key] = profile.Concurrency
		}
	}
	mu.Lock()
	limits = next
	mu.Unlock()
}

func ProfileExpired(expiresAt string, now time.Time) bool {
	expiresAt = strings.TrimSpace(expiresAt)
	if expiresAt == "" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339, expiresAt)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339Nano, expiresAt)
	}
	if err != nil {
		return false
	}
	return now.After(parsed)
}

func Acquire(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return true
	}
	mu.Lock()
	defer mu.Unlock()
	limit := limits[key]
	if limit > 0 && flight[key] >= limit {
		return false
	}
	flight[key]++
	return true
}

func Release(key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	mu.Lock()
	if flight[key] > 0 {
		flight[key]--
	}
	if flight[key] == 0 {
		delete(flight, key)
	}
	mu.Unlock()
}

func RecordFinish(key string, failed bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	mu.Lock()
	stats := usageForLocked(key)
	stats.Requests++
	if failed {
		stats.Failed++
	} else {
		stats.Success++
	}
	mu.Unlock()
}

func AddTokens(key string, input, output int64) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	mu.Lock()
	stats := usageForLocked(key)
	stats.InputTokens += input
	stats.OutputTokens += output
	mu.Unlock()
}

func Snapshot(key string) Stats {
	key = strings.TrimSpace(key)
	mu.Lock()
	defer mu.Unlock()
	out := Stats{InFlight: flight[key]}
	if stats := usage[key]; stats != nil {
		out.Requests = stats.Requests
		out.Success = stats.Success
		out.Failed = stats.Failed
		out.InputTokens = stats.InputTokens
		out.OutputTokens = stats.OutputTokens
	}
	return out
}

func Reset(key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	mu.Lock()
	delete(usage, key)
	mu.Unlock()
}

func usageForLocked(key string) *Stats {
	stats := usage[key]
	if stats == nil {
		stats = &Stats{}
		usage[key] = stats
	}
	return stats
}
