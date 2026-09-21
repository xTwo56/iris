package delivery

import (
	"errors"
	"strings"
	"time"
)

// TerminalState uses Mercury's wire vocabulary: succeeded is successful
// completion, while failed is terminal execution failure, not proof that the
// receiver did not process a request. It does not classify an HTTP attempt.
type TerminalState string

const (
	TerminalSucceeded TerminalState = "succeeded"
	TerminalFailed    TerminalState = "failed"
)

// RunTerminal is an immutable observation alongside the original run. Its time
// is when Iris observed Mercury, not when Mercury finished execution. Missing
// records mean unreconciled; missing attempt outcomes remain unknown.
type RunTerminal struct {
	runID      RunID
	jobID      string
	state      TerminalState
	observedAt time.Time
}

// NewRunTerminal validates an observation without making network or storage
// calls. The repository separately verifies its acknowledged job association.
func NewRunTerminal(id RunID, job string, state TerminalState, at time.Time) (RunTerminal, error) {
	if strings.TrimSpace(string(id)) == "" || strings.TrimSpace(job) == "" || at.IsZero() {
		return RunTerminal{}, errors.New("terminal observation requires run, job and timestamp")
	}
	if state != TerminalSucceeded && state != TerminalFailed {
		return RunTerminal{}, errors.New("terminal state must be succeeded or failed")
	}
	return RunTerminal{id, job, state, at}, nil
}

func (t RunTerminal) RunID() RunID          { return t.runID }
func (t RunTerminal) MercuryJobID() string  { return t.jobID }
func (t RunTerminal) State() TerminalState  { return t.state }
func (t RunTerminal) ObservedAt() time.Time { return t.observedAt }

// SubmittedRun is a non-reserving snapshot of an acknowledged run awaiting
// reconciliation. The immutable run supplies the pagination and payload identity.
type SubmittedRun struct {
	Run          Run
	MercuryJobID string
}
