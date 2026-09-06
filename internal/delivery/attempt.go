package delivery

import (
	"errors"
	"strings"
	"time"
)

type AttemptID string

// Classification describes an observed outcome, as decided by the caller.
type Classification string

const (
	ClassificationSucceeded        Classification = "succeeded"
	ClassificationRetryableFailure Classification = "retryable_failure"
	ClassificationPermanentFailure Classification = "permanent_failure"
)

/*
 * immutable record to append before sending a request
 * start without an outcome means that result is unknown, not that delivery
 * definitely failed
 */
type AttemptStart struct {
	id        AttemptID
	runID     RunID
	startedAt time.Time
}

func NewAttemptStart(id AttemptID, runID RunID, startedAt time.Time) (AttemptStart, error) {
	if strings.TrimSpace(string(id)) == "" {
		return AttemptStart{}, errors.New("attempt ID must not be blank")
	}
	if strings.TrimSpace(string(runID)) == "" {
		return AttemptStart{}, errors.New("attempt run ID must not be blank")
	}
	if startedAt.IsZero() {
		return AttemptStart{}, errors.New("attempt start timestamp must not be zero")
	}
	return AttemptStart{id: id, runID: runID, startedAt: startedAt}, nil
}

func (a AttemptStart) ID() AttemptID { return a.id }

func (a AttemptStart) RunID() RunID { return a.runID }

func (a AttemptStart) StartedAt() time.Time { return a.startedAt }

// immutable observation appended after an attempt starts
// at most one outcome per attempt will be enforced later in persistence
type AttemptOutcome struct {
	attemptID      AttemptID
	finishedAt     time.Time
	httpStatus     int
	hasHTTPStatus  bool
	classification Classification
}

func NewAttemptOutcome(start AttemptStart, finishedAt time.Time, httpStatus *int, classification Classification) (AttemptOutcome, error) {
	if _, err := NewAttemptStart(start.id, start.runID, start.startedAt); err != nil {
		return AttemptOutcome{}, err
	}
	if finishedAt.IsZero() {
		return AttemptOutcome{}, errors.New("attempt finish timestamp must not be zero")
	}
	if finishedAt.Before(start.startedAt) {
		return AttemptOutcome{}, errors.New("attempt finish timestamp must not precede start")
	}
	if classification != ClassificationSucceeded && classification != ClassificationRetryableFailure && classification != ClassificationPermanentFailure {
		return AttemptOutcome{}, errors.New("attempt classification must be succeeded, retryable_failure or permanent_failure")
	}
	status := 0
	if httpStatus != nil {
		status = *httpStatus
		if status < 100 || status > 599 {
			return AttemptOutcome{}, errors.New("attempt HTTP status must be between 100 and 599")
		}
	}
	successStatus := status >= 200 && status <= 299
	if classification == ClassificationSucceeded && !successStatus {
		return AttemptOutcome{}, errors.New("successful attempt requires a 2xx HTTP status")
	}
	if classification != ClassificationSucceeded && successStatus {
		return AttemptOutcome{}, errors.New("failed attempt must not have a 2xx HTTP status")
	}
	return AttemptOutcome{
		attemptID: start.id, finishedAt: finishedAt, httpStatus: status,
		hasHTTPStatus: httpStatus != nil, classification: classification,
	}, nil
}

func (o AttemptOutcome) AttemptID() AttemptID { return o.attemptID }

func (o AttemptOutcome) FinishedAt() time.Time { return o.finishedAt }

func (o AttemptOutcome) HTTPStatus() (int, bool) { return o.httpStatus, o.hasHTTPStatus }

func (o AttemptOutcome) Classification() Classification { return o.classification }
