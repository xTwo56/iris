package dispatcher

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/outbox"
	outboxpg "github.com/xTwo56/iris/internal/outbox/postgres"
)

type memoryOutbox struct {
	mu       sync.Mutex
	entries  []outbox.Entry
	ackError error
	reads    int
}

func (m *memoryOutbox) ListPendingAfter(ctx context.Context, limit int, after time.Time, id delivery.RunID) ([]outbox.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	result := []outbox.Entry{}
	for _, e := range m.entries {
		_, _, done := e.Acknowledgment()
		if done {
			continue
		}
		if !after.IsZero() && (e.CreatedAt().Before(after) || (e.CreatedAt().Equal(after) && e.RunID() <= id)) {
			continue
		}
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt().Equal(result[j].CreatedAt()) {
			return result[i].RunID() < result[j].RunID()
		}
		return result[i].CreatedAt().Before(result[j].CreatedAt())
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}
func (m *memoryOutbox) MarkSubmitted(ctx context.Context, id delivery.RunID, job string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ackError != nil {
		return m.ackError
	}
	for i, e := range m.entries {
		if e.RunID() != id {
			continue
		}
		if original, _, done := e.Acknowledgment(); done && original != job {
			return outboxpg.ErrSubmissionConflict
		}
		updated, err := e.WithAcknowledgment(job, at)
		if err != nil {
			return err
		}
		m.entries[i] = updated
		return nil
	}
	return outboxpg.ErrNotFound
}
func (m *memoryOutbox) entry(i int) outbox.Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries[i]
}

type runs map[delivery.RunID]delivery.Run

func (r runs) GetByID(ctx context.Context, id delivery.RunID) (delivery.Run, error) {
	v, ok := r[id]
	if !ok {
		return delivery.Run{}, errors.New("missing run")
	}
	return v, nil
}

type deliveries map[delivery.ID]delivery.Delivery

func (d deliveries) GetByID(ctx context.Context, id delivery.ID) (delivery.Delivery, error) {
	v, ok := d[id]
	if !ok {
		return delivery.Delivery{}, errors.New("missing delivery")
	}
	return v, nil
}

type submitFunc func(context.Context, Submission) (string, error)

func (f submitFunc) Submit(ctx context.Context, s Submission) (string, error) { return f(ctx, s) }

type remote struct {
	mu        sync.Mutex
	requests  []Submission
	jobs      map[string]string
	uncertain bool
}

func (r *remote) Submit(ctx context.Context, s Submission) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, s)
	if r.jobs == nil {
		r.jobs = map[string]string{}
	}
	id := r.jobs[s.Key]
	if id == "" {
		id = "job-" + string(s.RunID)
		r.jobs[s.Key] = id
	}
	if r.uncertain {
		r.uncertain = false
		return "", ErrTransient
	}
	return id, nil
}
func fixture(t *testing.T, n int) (*memoryOutbox, runs, deliveries) {
	t.Helper()
	m := &memoryOutbox{}
	r := runs{}
	d := deliveries{}
	at := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		id := delivery.RunID(string(rune('A' + i)))
		value, err := delivery.New(delivery.ID("delivery-"+string(id)), "event", "endpoint", at)
		if err != nil {
			t.Fatal(err)
		}
		run, err := delivery.NewRun(id, value.ID(), at, delivery.TriggerInitial)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := outbox.New(id, "iris:delivery-run:"+string(id), at)
		if err != nil {
			t.Fatal(err)
		}
		m.entries = append(m.entries, entry)
		r[id] = run
		d[value.ID()] = value
	}
	return m, r, d
}
func service(t *testing.T, m *memoryOutbox, r runs, d deliveries, s Submitter, batch int) *Dispatcher {
	t.Helper()
	v, err := New(m, r, d, s, Config{BatchSize: batch, PollInterval: time.Hour, MaxBackoff: 2 * time.Hour, OperationTimeout: time.Second}, func() time.Time { return time.Date(2026, 9, 20, 1, 0, 0, 0, time.UTC) }, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func TestSubmissionAndCrashRecovery(t *testing.T) {
	for _, mode := range []string{"success", "uncertain response", "acknowledgment failure"} {
		t.Run(mode, func(t *testing.T) {
			m, r, d := fixture(t, 1)
			remote := &remote{uncertain: mode == "uncertain response"}
			localFailure := errors.New("write unavailable")
			if mode == "acknowledgment failure" {
				m.ackError = localFailure
			}
			err := service(t, m, r, d, remote, 10).DispatchBatch(context.Background())
			if mode == "success" && err != nil {
				t.Fatal(err)
			}
			if mode != "success" && err == nil {
				t.Fatal("expected recoverable error")
			}
			if mode != "success" {
				if _, _, done := m.entry(0).Acknowledgment(); done {
					t.Fatal("uncertain submission acknowledged")
				}
				m.ackError = nil
				// New dispatcher models a restart, with no retained in-memory request state.
				if err := service(t, m, r, d, remote, 10).DispatchBatch(context.Background()); err != nil {
					t.Fatal(err)
				}
				if len(remote.requests) != 2 || !reflect.DeepEqual(remote.requests[0], remote.requests[1]) {
					t.Fatal("request changed across recovery")
				}
			}
			if id, _, done := m.entry(0).Acknowledgment(); !done || id != "job-A" || len(remote.jobs) != 1 {
				t.Fatal("missing or duplicate accepted job")
			}
		})
	}
}
func TestConcurrentDispatchers(t *testing.T) {
	m, r, d := fixture(t, 1)
	accepted := &remote{}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	submitter := submitFunc(func(ctx context.Context, s Submission) (string, error) {
		entered <- struct{}{}
		<-release
		return accepted.Submit(ctx, s)
	})
	one, two := service(t, m, r, d, submitter, 1), service(t, m, r, d, submitter, 1)
	results := make(chan error, 2)
	go func() { results <- one.DispatchBatch(context.Background()) }()
	go func() { results <- two.DispatchBatch(context.Background()) }()
	<-entered
	<-entered
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if len(accepted.jobs) != 1 || len(accepted.requests) != 2 {
		t.Fatal("concurrent submissions did not converge")
	}
	if id, _, done := m.entry(0).Acknowledgment(); !done || id != "job-A" {
		t.Fatal("acknowledgment missing")
	}
}
func TestConflictingAcknowledgment(t *testing.T) {
	m, r, d := fixture(t, 1)
	stale := m.entry(0)
	at := stale.CreatedAt().Add(time.Hour)
	if err := m.MarkSubmitted(context.Background(), stale.RunID(), "original", at); err != nil {
		t.Fatal(err)
	}
	s := service(t, m, r, d, submitFunc(func(context.Context, Submission) (string, error) { return "different", nil }), 1)
	if err := s.dispatch(context.Background(), stale); !errors.Is(err, outboxpg.ErrSubmissionConflict) {
		t.Fatalf("conflicting ack=%v", err)
	}
	id, when, _ := m.entry(0).Acknowledgment()
	if id != "original" || !when.Equal(at) {
		t.Fatal("conflict overwrote history")
	}
}
func TestFailuresDoNotStarveLaterEntries(t *testing.T) {
	for _, failure := range []error{ErrTransient, ErrAuthentication, ErrConfiguration, ErrConflict, ErrAcknowledgment} {
		t.Run(failure.Error(), func(t *testing.T) {
			m, r, d := fixture(t, 2)
			s := service(t, m, r, d, submitFunc(func(ctx context.Context, input Submission) (string, error) {
				if input.RunID == "A" {
					return "", failure
				}
				return "accepted-B", nil
			}), 1)
			if err := s.DispatchBatch(context.Background()); !errors.Is(err, failure) {
				t.Fatalf("classification=%v", err)
			}
			if err := s.DispatchBatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, _, done := m.entry(0).Acknowledgment(); done {
				t.Fatal("failed entry acknowledged")
			}
			if id, _, done := m.entry(1).Acknowledgment(); !done || id != "accepted-B" {
				t.Fatal("later entry starved")
			}
			if err := s.DispatchBatch(context.Background()); err != nil {
				t.Fatal(err)
			} // end resets cursor
			if err := s.DispatchBatch(context.Background()); !errors.Is(err, failure) {
				t.Fatal("failed entry was not revisited")
			}
		})
	}
}
func TestCancellationAndPollingBackoff(t *testing.T) {
	m, r, d := fixture(t, 1)
	called := make(chan struct{}, 1)
	s := service(t, m, r, d, submitFunc(func(ctx context.Context, _ Submission) (string, error) { called <- struct{}{}; return "", ErrTransient }), 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	<-called
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("polling did not cancel")
	}
	m.mu.Lock()
	reads := m.reads
	m.mu.Unlock()
	if reads != 1 {
		t.Fatalf("polled during backoff: %d", reads)
	}
	s = service(t, m, r, d, submitFunc(func(ctx context.Context, _ Submission) (string, error) { <-ctx.Done(); return "", ctx.Err() }), 1)
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := s.DispatchBatch(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled batch=%v", err)
	}
}

func TestOperationDeadlineLeavesIntentPending(t *testing.T) {
	m, r, d := fixture(t, 1)
	s := service(t, m, r, d, submitFunc(func(ctx context.Context, _ Submission) (string, error) { <-ctx.Done(); return "", ctx.Err() }), 1)
	s.config.OperationTimeout = time.Millisecond
	if err := s.DispatchBatch(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("operation deadline=%v", err)
	}
	if _, _, done := m.entry(0).Acknowledgment(); done {
		t.Fatal("timed-out operation acknowledged")
	}
}
