package outbox_test

import (
	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/outbox"
	"testing"
	"time"
)

func TestValidation(t *testing.T) {
	at := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		id  delivery.RunID
		key string
		at  time.Time
	}{{"", "k", at}, {" \t", "k", at}, {"r", " ", at}, {"r", "k", time.Time{}}} {
		if _, err := outbox.New(tt.id, tt.key, tt.at); err == nil {
			t.Fatal("invalid accepted")
		}
	}
	e, err := outbox.New("r", " key ", at)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		job string
		at  time.Time
	}{{"", at}, {" \t", at}, {"j", time.Time{}}} {
		if _, err := e.WithAcknowledgment(tt.job, tt.at); err == nil {
			t.Fatal("invalid ack accepted")
		}
	}
	ack, err := e.WithAcknowledgment("j", at)
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := ack.WithAcknowledgment("j", at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	_, when, _ := repeat.Acknowledgment()
	if !when.Equal(at) {
		t.Fatal("timestamp changed")
	}
	if _, err := ack.WithAcknowledgment("other", at); err == nil {
		t.Fatal("conflict accepted")
	}
	if _, _, ok := e.Acknowledgment(); ok {
		t.Fatal("original mutated")
	}
}
