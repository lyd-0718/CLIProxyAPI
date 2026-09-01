package clientkeys

import (
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func resetTrackerForTest() {
	mu.Lock()
	limits = map[string]int{}
	flight = map[string]int{}
	usage = map[string]*Stats{}
	mu.Unlock()
}

func TestProfileExpired(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if ProfileExpired("", now) {
		t.Fatal("empty expiry should not be expired")
	}
	if ProfileExpired(now.Add(time.Hour).Format(time.RFC3339), now) {
		t.Fatal("future expiry should not be expired")
	}
	if !ProfileExpired(now.Add(-time.Minute).Format(time.RFC3339), now) {
		t.Fatal("past expiry should be expired")
	}
}

func TestAcquireRespectsConcurrency(t *testing.T) {
	resetTrackerForTest()
	Configure(&sdkconfig.SDKConfig{
		APIKeyProfiles: []internalconfig.APIKeyProfile{
			{Key: "k-limit", Concurrency: 1},
		},
	})
	if !Acquire("k-limit") {
		t.Fatal("first acquire should succeed")
	}
	if Acquire("k-limit") {
		t.Fatal("second acquire should be rejected at limit 1")
	}
	Release("k-limit")
	if !Acquire("k-limit") {
		t.Fatal("acquire after release should succeed")
	}
	Release("k-limit")
}

func TestRecordFinishAndReset(t *testing.T) {
	resetTrackerForTest()
	RecordFinish("k-usage", false)
	RecordFinish("k-usage", true)
	AddTokens("k-usage", 10, 20)
	stats := Snapshot("k-usage")
	if stats.Requests != 2 || stats.Success != 1 || stats.Failed != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if stats.InputTokens != 10 || stats.OutputTokens != 20 {
		t.Fatalf("tokens = %+v", stats)
	}
	Reset("k-usage")
	stats = Snapshot("k-usage")
	if stats.Requests != 0 || stats.InputTokens != 0 || stats.OutputTokens != 0 {
		t.Fatalf("reset stats = %+v", stats)
	}
}
