package configaccess

import (
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestActiveAPIKeysSkipsDisabledAndExpired(t *testing.T) {
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	cfg := &sdkconfig.SDKConfig{
		APIKeys: []string{"keep", "disabled-key", "expired-key", "keep"},
		APIKeyProfiles: []internalconfig.APIKeyProfile{
			{Key: "disabled-key", Disabled: true},
			{Key: "expired-key", ExpiresAt: past},
		},
	}
	got := activeAPIKeys(cfg)
	if len(got) != 1 || got[0] != "keep" {
		t.Fatalf("activeAPIKeys = %#v, want [keep]", got)
	}
}

func TestActiveAPIKeysNilConfig(t *testing.T) {
	if got := activeAPIKeys(nil); got != nil {
		t.Fatalf("activeAPIKeys(nil) = %#v, want nil", got)
	}
}
