package delivery_test

import (
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
)

func TestRunTerminal(t *testing.T) {
	at := time.Date(2026, 9, 21, 0, 0, 0, 123456000, time.UTC)
	for _, state := range []delivery.TerminalState{delivery.TerminalSucceeded, delivery.TerminalFailed} {
		v, err := delivery.NewRunTerminal("run", "job", state, at)
		if err != nil || v.RunID() != "run" || v.MercuryJobID() != "job" || v.State() != state || !v.ObservedAt().Equal(at) {
			t.Fatalf("terminal: %+v %v", v, err)
		}
	}
	for _, tt := range []struct {
		run   delivery.RunID
		job   string
		state delivery.TerminalState
		at    time.Time
	}{
		{" ", "job", delivery.TerminalFailed, at}, {"run", " ", delivery.TerminalFailed, at},
		{"run", "job", "running", at}, {"run", "job", "completed", at}, {"run", "job", delivery.TerminalSucceeded, time.Time{}},
	} {
		if _, err := delivery.NewRunTerminal(tt.run, tt.job, tt.state, tt.at); err == nil {
			t.Fatalf("accepted invalid observation: %+v", tt)
		}
	}
}
