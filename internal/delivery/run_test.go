package delivery_test

import (
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
)

func TestNewRunValid(t *testing.T) {
	createdAt := time.Date(2026, time.September, 6, 12, 30, 0, 123, time.FixedZone("caller", 19800))
	tests := []struct {
		name       string
		id         delivery.RunID
		deliveryID delivery.ID
		trigger    delivery.Trigger
	}{
		{"initial", "run_123", "del_123", delivery.TriggerInitial},
		{"manual redelivery", "run_456", "del_123", delivery.TriggerManualRedelivery},
		{"preserved whitespace", " run_123 ", " del_123 ", delivery.TriggerInitial},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := delivery.NewRun(tt.id, tt.deliveryID, createdAt, tt.trigger)
			if err != nil {
				t.Fatalf("NewRun() error: %v", err)
			}
			if r.ID() != tt.id || r.DeliveryID() != tt.deliveryID || r.CreatedAt() != createdAt || r.Trigger() != tt.trigger {
				t.Fatalf("metadata changed: ID=%q DeliveryID=%q CreatedAt=%v Trigger=%q", r.ID(), r.DeliveryID(), r.CreatedAt(), r.Trigger())
			}
		})
	}
}

func TestNewRunInvalid(t *testing.T) {
	createdAt := time.Date(2026, time.September, 6, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		id         delivery.RunID
		deliveryID delivery.ID
		createdAt  time.Time
		trigger    delivery.Trigger
	}{
		{"empty run ID", "", "del", createdAt, delivery.TriggerInitial},
		{"whitespace run ID", " \t\n\u2003", "del", createdAt, delivery.TriggerInitial},
		{"empty delivery ID", "run", "", createdAt, delivery.TriggerInitial},
		{"whitespace delivery ID", "run", " \t\n\u2003", createdAt, delivery.TriggerInitial},
		{"zero timestamp", "run", "del", time.Time{}, delivery.TriggerInitial},
		{"empty trigger", "run", "del", createdAt, ""},
		{"whitespace trigger", "run", "del", createdAt, " \t\n\u2003"},
		{"unknown trigger", "run", "del", createdAt, "automatic_retry"},
		{"case variant", "run", "del", createdAt, "INITIAL"},
		{"padded trigger", "run", "del", createdAt, " manual_redelivery "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := delivery.NewRun(tt.id, tt.deliveryID, tt.createdAt, tt.trigger); err == nil {
				t.Fatal("NewRun() succeeded for invalid input")
			}
		})
	}
}
