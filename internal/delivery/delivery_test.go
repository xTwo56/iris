package delivery_test

import (
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/endpoint"
	"github.com/xTwo56/iris/internal/event"
)

func TestNewValid(t *testing.T) {
	createdAt := time.Date(2026, time.September, 6, 12, 30, 0, 123, time.FixedZone("caller", 19800))
	tests := []struct {
		name       string
		id         delivery.ID
		eventID    event.ID
		endpointID endpoint.ID
	}{
		{"ordinary values", "del_123", "evt_123", "ep_123"},
		{"preserved whitespace", " del_123 ", " evt_123 ", " ep_123 "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := delivery.New(tt.id, tt.eventID, tt.endpointID, createdAt)
			if err != nil {
				t.Fatalf("New() error: %v", err)
			}
			if d.ID() != tt.id || d.EventID() != tt.eventID || d.EndpointID() != tt.endpointID || d.CreatedAt() != createdAt {
				t.Fatalf("metadata changed: ID=%q EventID=%q EndpointID=%q CreatedAt=%v", d.ID(), d.EventID(), d.EndpointID(), d.CreatedAt())
			}
		})
	}
}

func TestNewInvalid(t *testing.T) {
	createdAt := time.Date(2026, time.September, 6, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		id         delivery.ID
		eventID    event.ID
		endpointID endpoint.ID
		createdAt  time.Time
	}{
		{"empty delivery ID", "", "evt", "ep", createdAt},
		{"whitespace delivery ID", " \t\n\u2003", "evt", "ep", createdAt},
		{"empty event ID", "del", "", "ep", createdAt},
		{"whitespace event ID", "del", " \t\n\u2003", "ep", createdAt},
		{"empty endpoint ID", "del", "evt", "", createdAt},
		{"whitespace endpoint ID", "del", "evt", " \t\n\u2003", createdAt},
		{"zero timestamp", "del", "evt", "ep", time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := delivery.New(tt.id, tt.eventID, tt.endpointID, tt.createdAt); err == nil {
				t.Fatal("New() succeeded for invalid input")
			}
		})
	}
}
