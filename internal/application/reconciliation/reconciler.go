// Package reconciliation observes Mercury results; it never executes or retries jobs.
package reconciliation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/mercury"
	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

var (
	ErrJobMissing     = errors.New("Mercury inspection job missing")
	ErrAuthentication = errors.New("Mercury inspection authentication rejected")
	ErrUnavailable    = errors.New("Mercury inspection unavailable")
	ErrTimeout        = errors.New("Mercury inspection timed out")
	ErrProtocol       = errors.New("Mercury inspection response invalid")
	ErrIdentity       = errors.New("Mercury inspection job identity mismatch")
	ErrPersistence    = errors.New("run reconciliation persistence failed")
)

// Store is used with a pool-backed repository so each operation finishes before
// network inspection. Neither pending outbox entries nor attempt history is written.
type Store interface {
	ListUnresolvedAfter(context.Context, int, time.Time, delivery.RunID) ([]delivery.SubmittedRun, error)
	Record(context.Context, delivery.RunTerminal) error
}

// Inspector is precisely the public SDK operation; it cannot claim, renew or
// report execution transitions. The SDK supplies authenticated bounded HTTP.
type Inspector interface {
	Inspect(context.Context, workerclient.JobID) (workerclient.Job, error)
}

type Config struct {
	BatchSize                      int
	PollInterval, OperationTimeout time.Duration
}

// Reconciler serially visits pages. Separate instances may overlap safely: the
// repository keeps the first compatible terminal observation atomically.
type Reconciler struct {
	store     Store
	inspector Inspector
	config    Config
	now       func() time.Time
	report    func(error)
	after     time.Time
	afterID   delivery.RunID
}

func New(store Store, inspector Inspector, config Config, now func() time.Time, report func(error)) (*Reconciler, error) {
	if store == nil || inspector == nil || now == nil || report == nil || config.BatchSize < 1 || config.BatchSize > deliverypg.MaxReconciliationBatch || config.PollInterval <= 0 || config.OperationTimeout <= 0 {
		return nil, errors.New("invalid reconciliation dependencies or limits")
	}
	return &Reconciler{store: store, inspector: inspector, config: config, now: now, report: report}, nil
}

// ReconcileBatch makes progress past every inspected run, even an unavailable
// job or a still-running job. An empty page wraps the cursor for a later sweep;
// newly acknowledged older runs are then included. Call serially per instance.
func (r *Reconciler) ReconcileBatch(ctx context.Context) error {
	read, cancel := context.WithTimeout(ctx, r.config.OperationTimeout)
	entries, err := r.store.ListUnresolvedAfter(read, r.config.BatchSize, r.after, r.afterID)
	cancel()
	if err != nil {
		err = errors.Join(ErrPersistence, err)
		r.report(err)
		return err
	}
	if len(entries) == 0 {
		r.after = time.Time{}
		r.afterID = ""
		return nil
	}
	var failures []error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.after, r.afterID = entry.Run.CreatedAt(), entry.Run.ID()
		operation, cancel := context.WithTimeout(ctx, r.config.OperationTimeout)
		err := r.reconcile(operation, entry)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			r.report(err)
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// reconcile records only positively observed terminal state. Identity and task
// payload checks prevent another run's job from terminating this run. No absence,
// HTTP error or missing attempt outcome is converted into confirmed failure.
func (r *Reconciler) reconcile(ctx context.Context, entry delivery.SubmittedRun) error {
	if entry.Run.ID() == "" || strings.TrimSpace(entry.MercuryJobID) == "" {
		return ErrIdentity
	}
	job, err := r.inspector.Inspect(ctx, workerclient.JobID(entry.MercuryJobID))
	if err != nil {
		return inspectionError(err)
	}
	if ctx.Err() != nil {
		return inspectionError(ctx.Err())
	}
	if job.ID != workerclient.JobID(entry.MercuryJobID) || job.TaskType != workerclient.TaskType(mercury.TaskType) {
		return ErrIdentity
	}
	var payload struct {
		DeliveryID delivery.ID    `json:"delivery_id"`
		RunID      delivery.RunID `json:"run_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(job.Payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || decoder.Decode(new(any)) != io.EOF {
		return ErrProtocol
	}
	if payload.RunID != entry.Run.ID() || payload.DeliveryID != entry.Run.DeliveryID() {
		return ErrIdentity
	}
	var state delivery.TerminalState
	switch job.State {
	case workerclient.StateQueued, workerclient.StateLeased, workerclient.StateRunning, workerclient.StateRetryScheduled:
		return nil
	case workerclient.StateSucceeded:
		if job.CompletedAt == nil || job.CompletedAt.IsZero() || job.FailedAt != nil || job.Lease != nil {
			return ErrProtocol
		}
		state = delivery.TerminalSucceeded
	case workerclient.StateFailed:
		if job.FailedAt == nil || job.FailedAt.IsZero() || job.CompletedAt != nil || job.Lease != nil {
			return ErrProtocol
		}
		state = delivery.TerminalFailed
	default:
		return ErrProtocol
	}
	// Observation time is ours; Mercury completion/failure timestamps and failure
	// text are not synthesized or copied into attempt history. A failed job can
	// still have reached a receiver before an acknowledgment was lost.
	terminal, err := delivery.NewRunTerminal(entry.Run.ID(), entry.MercuryJobID, state, r.now().UTC().Truncate(time.Microsecond))
	if err != nil {
		return errors.Join(ErrPersistence, err)
	}
	if err := r.store.Record(ctx, terminal); err != nil {
		return errors.Join(ErrPersistence, err)
	}
	return nil
}

// inspectionError keeps operational problems distinct. The SDK classifies HTTP
// 5xx as protocol errors, so inspect its status before the general protocol case.
// Callers should log ErrorClass only, never remote error messages or lease fields.
func inspectionError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	var timeout net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
		return ErrTimeout
	}
	var operation *workerclient.OperationError
	if errors.As(err, &operation) {
		switch {
		case operation.StatusCode == 401 || operation.StatusCode == 403:
			return ErrAuthentication
		case operation.StatusCode == 404:
			return ErrJobMissing
		case operation.StatusCode == 408:
			return ErrTimeout
		case operation.StatusCode >= 500 || operation.StatusCode == 429 || operation.StatusCode == 425:
			return ErrUnavailable
		}
	}
	switch {
	case errors.Is(err, workerclient.ErrAuthentication):
		return ErrAuthentication
	case errors.Is(err, workerclient.ErrJobNotFound):
		return ErrJobMissing
	case errors.Is(err, workerclient.ErrProtocol):
		return ErrProtocol
	default:
		return ErrUnavailable
	}
}

// Run waits after every bounded page, including error pages and empty sweeps.
// Rechecking observations is not scheduling a webhook retry; only Mercury does
// that. Cancellation interrupts the page, HTTP/database calls and polling timer.
func (r *Reconciler) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_ = r.ReconcileBatch(ctx) // each problem is surfaced through report
		if err := ctx.Err(); err != nil {
			return err
		}
		timer := time.NewTimer(r.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// ErrorClass is safe for operational logs even when a repository error contains
// sensitive SQL diagnostics. It does not include job payloads or bearer/lease data.
func ErrorClass(err error) string {
	for _, entry := range []struct {
		err   error
		label string
	}{
		{ErrAuthentication, "Mercury worker authentication rejected"},
		{ErrJobMissing, "Mercury job missing; run remains unreconciled"},
		{ErrTimeout, "Mercury inspection timed out"},
		{ErrUnavailable, "Mercury inspection unavailable"},
		{ErrIdentity, "Mercury job identity mismatch; investigate association"},
		{ErrProtocol, "invalid Mercury inspection response"},
		{deliverypg.ErrTerminalConflict, "terminal observation conflicts; investigate association or state"},
		{ErrPersistence, "run observation persistence failed"},
	} {
		if errors.Is(err, entry.err) {
			return entry.label
		}
	}
	return "reconciliation interrupted or failed"
}
