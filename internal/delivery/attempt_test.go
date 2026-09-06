package delivery_test

import (
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
)

func TestNewAttemptStart(t *testing.T) {
	now := time.Date(2026, 9, 7, 0, 0, 0, 123, time.FixedZone("caller", 19800))
	tests := []struct {
		name    string
		id      delivery.AttemptID
		runID   delivery.RunID
		at      time.Time
		invalid bool
	}{
		{"valid", "attempt", "run", now, false},
		{"preserved whitespace", " attempt ", " run ", now, false},
		{"empty ID", "", "run", now, true},
		{"blank ID", " \t\n\u2003", "run", now, true},
		{"empty run ID", "attempt", "", now, true},
		{"blank run ID", "attempt", " \t\n\u2003", now, true},
		{"zero time", "attempt", "run", time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := delivery.NewAttemptStart(tt.id, tt.runID, tt.at)
			if tt.invalid {
				if err == nil {
					t.Fatal("NewAttemptStart() succeeded for invalid input")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewAttemptStart() error: %v", err)
			}
			if a.ID() != tt.id || a.RunID() != tt.runID || a.StartedAt() != tt.at {
				t.Fatalf("start metadata changed: %+v", a)
			}
		})
	}
}

func TestNewAttemptOutcome(t *testing.T) {
	now := time.Date(2026, 9, 7, 0, 0, 0, 123, time.FixedZone("caller", 19800))
	start, err := delivery.NewAttemptStart(" attempt ", "run", now)
	if err != nil {
		t.Fatal(err)
	}
	status := func(n int) *int { return &n }
	tests := []struct {
		name           string
		start          delivery.AttemptStart
		finish         time.Time
		status         *int
		classification delivery.Classification
		invalid        bool
	}{
		{"success lower boundary", start, now.Add(time.Second), status(200), delivery.ClassificationSucceeded, false},
		{"success upper boundary and equal time", start, now, status(299), delivery.ClassificationSucceeded, false},
		{"retryable with status", start, now, status(503), delivery.ClassificationRetryableFailure, false},
		{"permanent with status", start, now, status(400), delivery.ClassificationPermanentFailure, false},
		{"retryable without status", start, now, nil, delivery.ClassificationRetryableFailure, false},
		{"permanent without status", start, now, nil, delivery.ClassificationPermanentFailure, false},
		{"status lower boundary", start, now, status(100), delivery.ClassificationRetryableFailure, false},
		{"status upper boundary", start, now, status(599), delivery.ClassificationPermanentFailure, false},
		{"failure below 2xx", start, now, status(199), delivery.ClassificationRetryableFailure, false},
		{"failure above 2xx", start, now, status(300), delivery.ClassificationPermanentFailure, false},
		{"zero start", delivery.AttemptStart{}, now, nil, delivery.ClassificationRetryableFailure, true},
		{"zero finish", start, time.Time{}, nil, delivery.ClassificationRetryableFailure, true},
		{"finish before start", start, now.Add(-time.Nanosecond), nil, delivery.ClassificationRetryableFailure, true},
		{"empty classification", start, now, status(200), "", true},
		{"unknown classification", start, now, nil, "unknown", true},
		{"classification case", start, now, status(200), "SUCCEEDED", true},
		{"classification whitespace", start, now, status(200), " succeeded ", true},
		{"negative status", start, now, status(-1), delivery.ClassificationRetryableFailure, true},
		{"zero status is not absent", start, now, status(0), delivery.ClassificationRetryableFailure, true},
		{"status below range", start, now, status(99), delivery.ClassificationRetryableFailure, true},
		{"status above range", start, now, status(600), delivery.ClassificationPermanentFailure, true},
		{"success without status", start, now, nil, delivery.ClassificationSucceeded, true},
		{"success below 2xx", start, now, status(199), delivery.ClassificationSucceeded, true},
		{"success above 2xx", start, now, status(300), delivery.ClassificationSucceeded, true},
		{"retryable rejects 200", start, now, status(200), delivery.ClassificationRetryableFailure, true},
		{"retryable rejects 299", start, now, status(299), delivery.ClassificationRetryableFailure, true},
		{"permanent rejects 200", start, now, status(200), delivery.ClassificationPermanentFailure, true},
		{"permanent rejects 299", start, now, status(299), delivery.ClassificationPermanentFailure, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := tt.start
			o, err := delivery.NewAttemptOutcome(tt.start, tt.finish, tt.status, tt.classification)
			if tt.start != before {
				t.Fatal("outcome construction changed start record")
			}
			if tt.invalid {
				if err == nil {
					t.Fatal("NewAttemptOutcome() succeeded for invalid input")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewAttemptOutcome() error: %v", err)
			}
			if o.AttemptID() != tt.start.ID() || o.FinishedAt() != tt.finish || o.Classification() != tt.classification {
				t.Fatalf("outcome metadata changed: %+v", o)
			}
			wantStatus := 0
			if tt.status != nil {
				wantStatus = *tt.status
			}
			got, present := o.HTTPStatus()
			if got != wantStatus || present != (tt.status != nil) {
				t.Fatalf("HTTPStatus() = (%d, %v), want (%d, %v)", got, present, wantStatus, tt.status != nil)
			}
			if tt.status != nil {
				*tt.status = 418
			}
			got = 500 // Changing a retrieved value cannot change the record either.
			if again, _ := o.HTTPStatus(); again != wantStatus {
				t.Fatalf("status changed through external value: got %d, want %d", again, wantStatus)
			}
		})
	}
}
