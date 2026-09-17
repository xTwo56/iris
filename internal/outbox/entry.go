// Package outbox represents durable intent to submit one delivery run as a job.
package outbox

import (
	"errors"
	"github.com/xTwo56/iris/internal/delivery"
	"strings"
	"time"
)

// Entry retains immutable submission identity and an optional acknowledgment.
// Submitted means Mercury accepted the job, not that webhook delivery succeeded.
// Private fields and value accessors prevent mutation of stored metadata.
type Entry struct {
	runID       delivery.RunID
	key         string
	createdAt   time.Time
	jobID       string
	submittedAt time.Time
	submitted   bool
}

// New validates caller-supplied identity and time and creates pending intent.
// Accepted strings are retained exactly; the zero value is invalid.
func New(runID delivery.RunID, key string, at time.Time) (Entry, error) {
	if strings.TrimSpace(string(runID)) == "" {
		return Entry{}, errors.New("outbox run ID must not be blank")
	}
	if strings.TrimSpace(key) == "" {
		return Entry{}, errors.New("outbox submission key must not be blank")
	}
	if at.IsZero() {
		return Entry{}, errors.New("outbox creation timestamp must not be zero")
	}
	return Entry{runID: runID, key: key, createdAt: at}, nil
}

// WithAcknowledgment returns a new value with an acknowledgment. Existing
// acknowledgments cannot change; repeating the job identity retains original time.
func (e Entry) WithAcknowledgment(jobID string, at time.Time) (Entry, error) {
	if _, err := New(e.runID, e.key, e.createdAt); err != nil {
		return Entry{}, err
	}
	if err := ValidateAcknowledgment(jobID, at); err != nil {
		return Entry{}, err
	}
	if e.submitted {
		if e.jobID != jobID {
			return Entry{}, errors.New("outbox job identity conflict")
		}
		return e, nil
	}
	e.jobID = jobID
	e.submittedAt = at
	e.submitted = true
	return e, nil
}

// ValidateAcknowledgment checks caller-supplied acknowledgment fields without I/O.
func ValidateAcknowledgment(jobID string, at time.Time) error {
	if strings.TrimSpace(jobID) == "" {
		return errors.New("outbox job ID must not be blank")
	}
	if at.IsZero() {
		return errors.New("outbox submission timestamp must not be zero")
	}
	return nil
}

// RunID identifies the run whose submission is required.
func (e Entry) RunID() delivery.RunID { return e.runID }

// SubmissionKey returns the stable key to reuse after uncertain submission results.
func (e Entry) SubmissionKey() string { return e.key }

// CreatedAt returns the caller-supplied intent creation time.
func (e Entry) CreatedAt() time.Time { return e.createdAt }

// Acknowledgment returns value copies; false means both fields are absent.
func (e Entry) Acknowledgment() (string, time.Time, bool) { return e.jobID, e.submittedAt, e.submitted }
