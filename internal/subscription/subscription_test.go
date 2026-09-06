package subscription_test

import (
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/endpoint"
	"github.com/xTwo56/iris/internal/event"
	"github.com/xTwo56/iris/internal/subscription"
)

func TestNewValid(t *testing.T) {
	createdAt := time.Date(2026, time.September, 6, 12, 30, 0, 123, time.FixedZone("caller", 19800))
	tests := []struct {
		name       string
		id         subscription.ID
		endpointID endpoint.ID
		eventType  event.Type
	}{
		{"ordinary values", "sub_123", "ep_123", "order.created"},
		{"preserved whitespace", " sub_123 ", " ep_123 ", " order.created "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := subscription.New(tt.id, tt.endpointID, tt.eventType, createdAt)
			if err != nil {
				t.Fatalf("New() error: %v", err)
			}
			if s.ID() != tt.id || s.EndpointID() != tt.endpointID || s.EventType() != tt.eventType || s.CreatedAt() != createdAt {
				t.Fatalf("metadata changed: ID=%q EndpointID=%q EventType=%q CreatedAt=%v", s.ID(), s.EndpointID(), s.EventType(), s.CreatedAt())
			}
			if !s.Enabled() {
				t.Fatal("new subscription must be enabled")
			}
		})
	}
}

func TestNewInvalid(t *testing.T) {
	createdAt := time.Date(2026, time.September, 6, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		id         subscription.ID
		endpointID endpoint.ID
		eventType  event.Type
		createdAt  time.Time
	}{
		{"empty ID", "", "ep", "order.created", createdAt},
		{"whitespace ID", " \t\n\u2003", "ep", "order.created", createdAt},
		{"empty endpoint ID", "sub", "", "order.created", createdAt},
		{"whitespace endpoint ID", "sub", " \t\n\u2003", "order.created", createdAt},
		{"empty event type", "sub", "ep", "", createdAt},
		{"whitespace event type", "sub", "ep", " \t\n\u2003", createdAt},
		{"zero timestamp", "sub", "ep", "order.created", time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := subscription.New(tt.id, tt.endpointID, tt.eventType, tt.createdAt); err == nil {
				t.Fatal("New() succeeded for invalid input")
			}
		})
	}
}

func TestMatches(t *testing.T) {
	tests := []struct {
		name            string
		rule, candidate event.Type
		disabled, want  bool
	}{
		{"exact", "order.created", "order.created", false, true},
		{"different", "order.created", "order.deleted", false, false},
		{"case sensitive", "order.created", "Order.Created", false, false},
		{"prefix", "order.created", "order", false, false},
		{"suffix", "order.created", "order.created.extra", false, false},
		{"no trimming", "order.created", " order.created ", false, false},
		{"literal whitespace", " order.created ", " order.created ", false, true},
		{"no wildcard expansion", "order.*", "order.created", false, false},
		{"literal wildcard", "order.*", "order.*", false, true},
		{"empty candidate", "order.created", "", false, false},
		{"disabled exact", "order.created", "order.created", true, false},
		{"disabled different", "order.created", "order.deleted", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := subscription.New("sub", "ep", tt.rule, time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatalf("New() error: %v", err)
			}
			if tt.disabled {
				s.Disable()
			}
			if got := s.Matches(tt.candidate); got != tt.want {
				t.Fatalf("Matches(%q) = %v, want %v", tt.candidate, got, tt.want)
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
			id, endpointID, eventType := subscription.ID("sub"), endpoint.ID("ep"), event.Type("order.created")
			createdAt := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
			s, err := subscription.New(id, endpointID, eventType, createdAt)
			if err != nil {
				t.Fatalf("New() error: %v", err)
			}
			for i, enabled := range tt.states {
				if enabled {
					s.Enable()
				} else {
					s.Disable()
				}
				if s.Enabled() != enabled || s.Matches(eventType) != enabled {
					t.Fatalf("step %d: Enabled()=%v Matches()=%v, want %v", i, s.Enabled(), s.Matches(eventType), enabled)
				}
				if s.ID() != id || s.EndpointID() != endpointID || s.EventType() != eventType || s.CreatedAt() != createdAt {
					t.Fatalf("step %d: lifecycle operation changed immutable metadata", i)
				}
			}
		})
	}
}
