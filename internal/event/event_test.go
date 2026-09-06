package event_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/event"
)

func TestNewValid(t *testing.T) {
	createdAt := time.Date(2026, time.September, 6, 12, 30, 0, 123, time.FixedZone("caller", 19800))
	tests := []struct {
		name    string
		payload string
	}{
		{"object with original formatting", " \n{\"z\": 1.00, \"a\": \"\\u0061\", \"z\": 2}\t"},
		{"array", `[1, true, null]`},
		{"string", `"hello"`},
		{"number", `1e3`},
		{"boolean", `false`},
		{"null", `null`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Nonblank identifiers are accepted verbatim, including whitespace.
			id, eventType := event.ID(" evt_123 "), event.Type(" order.created ")
			e, err := event.New(id, eventType, createdAt, []byte(tt.payload))
			if err != nil {
				t.Fatalf("New() error: %v", err)
			}
			if e.ID() != id || e.Type() != eventType || e.CreatedAt() != createdAt {
				t.Fatalf("metadata changed: ID=%q Type=%q CreatedAt=%v", e.ID(), e.Type(), e.CreatedAt())
			}
			if !bytes.Equal(e.Payload(), []byte(tt.payload)) {
				t.Fatalf("Payload() = %q, want exact bytes %q", e.Payload(), tt.payload)
			}
		})
	}
}

func TestNewInvalid(t *testing.T) {
	createdAt := time.Date(2026, time.September, 6, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		id        event.ID
		eventType event.Type
		createdAt time.Time
		payload   []byte
	}{
		{"empty ID", "", "created", createdAt, []byte(`{}`)},
		{"whitespace ID", " \t\n\u2003", "created", createdAt, []byte(`{}`)},
		{"empty type", "evt", "", createdAt, []byte(`{}`)},
		{"whitespace type", "evt", " \t\n\u2003", createdAt, []byte(`{}`)},
		{"zero timestamp", "evt", "created", time.Time{}, []byte(`{}`)},
		{"nil payload", "evt", "created", createdAt, nil},
		{"empty payload", "evt", "created", createdAt, []byte{}},
		{"whitespace payload", "evt", "created", createdAt, []byte(" \n\t")},
		{"malformed JSON", "evt", "created", createdAt, []byte(`{"a":}`)},
		{"trailing comma", "evt", "created", createdAt, []byte(`[1,]`)},
		{"multiple JSON values", "evt", "created", createdAt, []byte(`{} {}`)},
		{"trailing garbage", "evt", "created", createdAt, []byte(`{}x`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := event.New(tt.id, tt.eventType, tt.createdAt, tt.payload)
			if err == nil {
				t.Fatal("New() succeeded for invalid input")
			}
		})
	}
}

func TestPayloadIsolation(t *testing.T) {
	for _, source := range []string{"input", "retrieval"} {
		t.Run(source, func(t *testing.T) {
			const original = `{"value":1}`
			input := []byte(original)
			e, err := event.New("evt", "created", time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC), input)
			if err != nil {
				t.Fatalf("New() error: %v", err)
			}
			previousRead := e.Payload()
			mutable := input
			if source == "retrieval" {
				mutable = e.Payload()
			}
			mutable[0] = '['
			if got := string(e.Payload()); got != original {
				t.Fatalf("mutation through %s changed event payload to %q", source, got)
			}
			if string(previousRead) != original {
				t.Fatalf("mutation through %s changed a previous retrieval to %q", source, previousRead)
			}
		})
	}
}
