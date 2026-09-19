// Package dispatcher moves durable submission intent to Mercury without executing webhooks.
package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/outbox"
)

var (
	ErrAuthentication = errors.New("Mercury submission authentication failed")
	ErrConfiguration  = errors.New("Mercury submission configuration or request rejected")
	ErrConflict       = errors.New("Mercury submission idempotency conflict")
	ErrTransient      = errors.New("Mercury submission temporarily unavailable")
	ErrAcknowledgment = errors.New("invalid Mercury submission acknowledgment")
)

// Submission carries only immutable routing references and the stored producer key.
// The v1 wire policy is fixed by the adapter, never derived from polling settings.
type Submission struct {
	Key        string
	DeliveryID delivery.ID
	RunID      delivery.RunID
}

// Submitter hides Mercury transport details. A successful result identifies the
// durably accepted job; errors, including uncertain responses, must leave intent pending.
type Submitter interface {
	Submit(context.Context, Submission) (string, error)
}

// Outbox exposes non-reserving pending reads and atomic acknowledgment only.
type Outbox interface {
	ListPendingAfter(context.Context, int, time.Time, delivery.RunID) ([]outbox.Entry, error)
	MarkSubmitted(context.Context, delivery.RunID, string, time.Time) error
}

// Runs resolves immutable execution identity before submission.
type Runs interface {
	GetByID(context.Context, delivery.RunID) (delivery.Run, error)
}

// Deliveries confirms the run references an existing delivery obligation.
type Deliveries interface {
	GetByID(context.Context, delivery.ID) (delivery.Delivery, error)
}

// Config bounds a page and its operations. PollInterval is the minimum retry
// backoff; consecutive failing pages exponentially back off up to MaxBackoff.
type Config struct {
	BatchSize                                  int
	PollInterval, MaxBackoff, OperationTimeout time.Duration
}

// Dispatcher is a single polling loop. Multiple independent instances are safe:
// Mercury deduplicates submission and the outbox atomically fixes the acknowledged ID.
type Dispatcher struct {
	outbox     Outbox
	runs       Runs
	deliveries Deliveries
	submitter  Submitter
	config     Config
	now        func() time.Time
	report     func(error)
	after      time.Time
	afterID    delivery.RunID
}

// New connects caller-owned repositories; it never begins or completes transactions.
// report receives operation errors; callers must log only safe classifications,
// since wrapped storage diagnostics may contain sensitive values.
func New(o Outbox, r Runs, d Deliveries, s Submitter, c Config, now func() time.Time, report func(error)) (*Dispatcher, error) {
	if o == nil || r == nil || d == nil || s == nil || now == nil || report == nil {
		return nil, errors.New("dispatcher dependencies are required")
	}
	if c.BatchSize < 1 || c.BatchSize > 1000 || c.PollInterval <= 0 || c.MaxBackoff < c.PollInterval || c.OperationTimeout <= 0 {
		return nil, errors.New("invalid dispatcher limits")
	}
	return &Dispatcher{outbox: o, runs: r, deliveries: d, submitter: s, config: c, now: now, report: report}, nil
}

// DispatchBatch visits one bounded page, advancing even when a submission fails.
// A failing oldest entry cannot monopolize each SELECT LIMIT page. An empty page
// wraps the cursor; entries inserted behind it are included on the next sweep.
// Call serially per Dispatcher; independent instances need no shared cursor.
func (d *Dispatcher) DispatchBatch(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, d.config.OperationTimeout)
	entries, err := d.outbox.ListPendingAfter(queryCtx, d.config.BatchSize, d.after, d.afterID)
	cancel()
	if err != nil {
		err = fmt.Errorf("read pending outbox: %w", err)
		d.report(err)
		return err
	}
	if len(entries) == 0 {
		d.after = time.Time{}
		d.afterID = ""
		return nil
	}
	var failures []error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Advance independently of success; all intent remains in PostgreSQL until ack.
		d.after, d.afterID = entry.CreatedAt(), entry.RunID()
		operation, cancel := context.WithTimeout(ctx, d.config.OperationTimeout)
		err := d.dispatch(operation, entry)
		cancel()
		if err != nil {
			failures = append(failures, err)
			d.report(err)
		}
	}
	return errors.Join(failures...)
}

// dispatch resolves immutable relationships before making the HTTP request. No
// transaction spans that call. A crash after acceptance but before MarkSubmitted
// is recovered by replaying the exact same key and request on a later sweep.
func (d *Dispatcher) dispatch(ctx context.Context, entry outbox.Entry) error {
	run, err := d.runs.GetByID(ctx, entry.RunID())
	if err != nil {
		return fmt.Errorf("resolve outbox run: %w", err)
	}
	value, err := d.deliveries.GetByID(ctx, run.DeliveryID())
	if err != nil {
		return fmt.Errorf("resolve run delivery: %w", err)
	}
	if run.ID() != entry.RunID() || value.ID() != run.DeliveryID() {
		return ErrConfiguration
	}
	job, err := d.submitter.Submit(ctx, Submission{Key: entry.SubmissionKey(), DeliveryID: value.ID(), RunID: run.ID()})
	if err != nil {
		return err
	}
	if strings.TrimSpace(job) == "" {
		return ErrAcknowledgment
	}
	// Submitted means job acceptance, never webhook success. The existing atomic
	// acknowledgment retains its original timestamp and rejects a different job ID.
	if err := d.outbox.MarkSubmitted(ctx, entry.RunID(), job, d.now()); err != nil {
		return fmt.Errorf("record Mercury acknowledgment: %w", err)
	}
	return nil
}

// Run waits after every page, including errors and empty pages. Backoff concerns
// submission transport only; Mercury alone schedules webhook execution retries.
// Cancellation interrupts both the timer and all repository/HTTP operations.
func (d *Dispatcher) Run(ctx context.Context) error {
	delay := d.config.PollInterval
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := d.DispatchBatch(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			delay = d.config.PollInterval
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if err != nil {
			if delay > d.config.MaxBackoff/2 {
				delay = d.config.MaxBackoff
			} else {
				delay *= 2
			}
		}
	}
}
