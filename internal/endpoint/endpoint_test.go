package endpoint_test

import (
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/endpoint"
)

func TestNewValid(t *testing.T) {
	createdAt := time.Date(2026, time.September, 6, 12, 30, 0, 123, time.FixedZone("caller", 19800))
	tests := []struct {
		name string
		url  string
	}{
		{"hostname", "https://example.com"},
		{"path and query", "https://example.com/hooks?topic=order&topic=invoice"},
		{"port", "https://example.com:8443/hooks"},
		{"preserved spelling", "HTTPS://Example.COM/hooks/%2f?value=%23"},
		{"IPv4", "https://192.0.2.1/hooks"},
		{"IPv6", "https://[2001:db8::1]:8443/hooks"},
		// Structural validation does not enforce network destination policy.
		{"localhost", "https://localhost/hooks"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := endpoint.ID(" ep_123 ")
			e, err := endpoint.New(id, tt.url, createdAt)
			if err != nil {
				t.Fatalf("New() error: %v", err)
			}
			if e.ID() != id || e.URL() != tt.url || e.CreatedAt() != createdAt {
				t.Fatalf("metadata changed: ID=%q URL=%q CreatedAt=%v", e.ID(), e.URL(), e.CreatedAt())
			}
			if !e.Fanout() {
				t.Fatal("new endpoint must be enabled")
			}
		})
	}
}

func TestNewInvalid(t *testing.T) {
	createdAt := time.Date(2026, time.September, 6, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		id        endpoint.ID
		url       string
		createdAt time.Time
	}{
		{"empty ID", "", "https://example.com", createdAt},
		{"whitespace ID", " \t\n\u2003", "https://example.com", createdAt},
		{"zero timestamp", "ep", "https://example.com", time.Time{}},
		{"empty URL", "ep", "", createdAt},
		{"whitespace URL", "ep", " \t\n", createdAt},
		{"relative path", "ep", "/hooks", createdAt},
		{"scheme relative", "ep", "//example.com/hooks", createdAt},
		{"missing scheme", "ep", "example.com/hooks", createdAt},
		{"HTTP", "ep", "http://example.com", createdAt},
		{"other scheme", "ep", "ftp://example.com", createdAt},
		{"missing host", "ep", "https:///hooks", createdAt},
		{"port without hostname", "ep", "https://:443/hooks", createdAt},
		{"opaque URL", "ep", "https:example.com", createdAt},
		{"credentials", "ep", "https://user:pass@example.com", createdAt},
		{"username only", "ep", "https://user@example.com", createdAt},
		{"empty credentials", "ep", "https://@example.com", createdAt},
		{"fragment", "ep", "https://example.com/hooks#section", createdAt},
		{"empty fragment", "ep", "https://example.com/hooks#", createdAt},
		{"invalid escape", "ep", "https://example.com/%zz", createdAt},
		{"invalid port", "ep", "https://example.com:abc", createdAt},
		{"malformed IPv6", "ep", "https://[::1/hooks", createdAt},
		{"space in hostname", "ep", "https://exa mple.com", createdAt},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := endpoint.New(tt.id, tt.url, tt.createdAt); err == nil {
				t.Fatal("New() succeeded for invalid input")
			}
		})
	}
}

func TestEnableDisable(t *testing.T) {
	tests := []struct {
		name   string
		states []bool
	}{
		{"repeated enable", []bool{true, true}},
		{"repeated disable", []bool{false, false}},
		{"re-enable", []bool{false, false, true, true}},
		{"disable again", []bool{false, true, false, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := endpoint.ID("ep_123")
			const destination = "https://example.com/hooks"
			createdAt := time.Date(2026, time.September, 6, 0, 0, 0, 0, time.UTC)
			e, err := endpoint.New(id, destination, createdAt)
			if err != nil {
				t.Fatalf("New() error: %v", err)
			}
			for i, enabled := range tt.states {
				if enabled {
					e.FanoutEnable()
				} else {
					e.FanoutDisable()
				}
				if e.Fanout() != enabled {
					t.Fatalf("step %d: Enabled() = %v, want %v", i, e.Fanout(), enabled)
				}
				if e.ID() != id || e.URL() != destination || e.CreatedAt() != createdAt {
					t.Fatalf("step %d: lifecycle operation changed immutable metadata", i)
				}
			}
		})
	}
}
