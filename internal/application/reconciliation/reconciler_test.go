package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

type inspectFunc func(context.Context, workerclient.JobID) (workerclient.Job, error)

func (f inspectFunc) Inspect(ctx context.Context, id workerclient.JobID) (workerclient.Job, error) {
	return f(ctx, id)
}

type memoryStore struct {
	mu        sync.Mutex
	entries   []delivery.SubmittedRun
	saved     map[delivery.RunID]delivery.RunTerminal
	recordErr error
}

func (m *memoryStore) ListUnresolvedAfter(ctx context.Context, limit int, after time.Time, id delivery.RunID) ([]delivery.SubmittedRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []delivery.SubmittedRun
	for _, e := range m.entries {
		if _, exists := m.saved[e.Run.ID()]; exists {
			continue
		}
		if !after.IsZero() && (e.Run.CreatedAt().Before(after) || (e.Run.CreatedAt().Equal(after) && e.Run.ID() <= id)) {
			continue
		}
		result = append(result, e)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}
func (m *memoryStore) Record(ctx context.Context, v delivery.RunTerminal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recordErr != nil {
		return m.recordErr
	}
	if old, ok := m.saved[v.RunID()]; ok {
		if old.MercuryJobID() != v.MercuryJobID() || old.State() != v.State() {
			return deliverypg.ErrTerminalConflict
		}
		return nil
	}
	m.saved[v.RunID()] = v
	return nil
}

var testTime = time.Date(2026, 9, 21, 0, 0, 0, 123456000, time.UTC)

func storeFor(t *testing.T, ids ...string) *memoryStore {
	t.Helper()
	m := &memoryStore{saved: make(map[delivery.RunID]delivery.RunTerminal)}
	sort.Strings(ids)
	for _, id := range ids {
		run, err := delivery.NewRun(delivery.RunID(id), "delivery", testTime, delivery.TriggerInitial)
		if err != nil {
			t.Fatal(err)
		}
		m.entries = append(m.entries, delivery.SubmittedRun{Run: run, MercuryJobID: "job-" + id})
	}
	return m
}
func jobFor(id string, state workerclient.State) workerclient.Job {
	payload, _ := json.Marshal(map[string]string{"delivery_id": "delivery", "run_id": id})
	j := workerclient.Job{ID: workerclient.JobID("job-" + id), TaskType: "webhook.deliver.v1", Payload: payload, State: state}
	at := testTime.Add(time.Second)
	if state == workerclient.StateSucceeded {
		j.CompletedAt = &at
	}
	if state == workerclient.StateFailed {
		j.FailedAt = &at
	}
	return j
}
func newTestReconciler(t *testing.T, store Store, inspector Inspector, batch int) *Reconciler {
	t.Helper()
	r, err := New(store, inspector, Config{BatchSize: batch, PollInterval: time.Second, OperationTimeout: time.Second}, func() time.Time { return testTime.Add(time.Hour) }, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestConfirmedStatesOnly(t *testing.T) {
	for _, state := range []workerclient.State{workerclient.StateSucceeded, workerclient.StateFailed, workerclient.StateQueued, workerclient.StateLeased, workerclient.StateRunning, workerclient.StateRetryScheduled} {
		t.Run(string(state), func(t *testing.T) {
			m := storeFor(t, "run")
			r := newTestReconciler(t, m, inspectFunc(func(context.Context, workerclient.JobID) (workerclient.Job, error) { return jobFor("run", state), nil }), 10)
			if err := r.ReconcileBatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			terminal := state == workerclient.StateSucceeded || state == workerclient.StateFailed
			v, ok := m.saved["run"]
			if ok != terminal || (terminal && (string(v.State()) != string(state) || !v.ObservedAt().Equal(testTime.Add(time.Hour)))) {
				t.Fatalf("bad observation: %+v", v)
			}
		})
	}
}

func TestRetryThenSuccessPreservesFirstObservation(t *testing.T) {
	m := storeFor(t, "run")
	failedAt := testTime
	job := jobFor("run", workerclient.StateRetryScheduled)
	job.FailedAt = &failedAt
	r := newTestReconciler(t, m, inspectFunc(func(context.Context, workerclient.JobID) (workerclient.Job, error) {
		return job, nil
	}), 1)
	entry := m.entries[0]
	if err := r.reconcile(context.Background(), entry); err != nil || len(m.saved) != 0 {
		t.Fatalf("retry scheduled must remain unresolved: err=%v records=%d", err, len(m.saved))
	}
	job = jobFor("run", workerclient.StateSucceeded)
	job.FailedAt = &failedAt
	if err := r.reconcile(context.Background(), entry); err != nil {
		t.Fatalf("success with historical failure: %v", err)
	}
	first, ok := m.saved[entry.Run.ID()]
	if !ok || first.State() != delivery.TerminalSucceeded || first.MercuryJobID() != entry.MercuryJobID || !first.ObservedAt().Equal(testTime.Add(time.Hour)) {
		t.Fatalf("incorrect terminal observation: %+v", first)
	}
	// Another reconciler may already hold this entry when the first record lands.
	// Reinspect that stale selection with a later clock; the first record must win.
	r.now = func() time.Time { return testTime.Add(2 * time.Hour) }
	if err := r.reconcile(context.Background(), entry); err != nil {
		t.Fatalf("repeated success observation: %v", err)
	}
	if len(m.saved) != 1 || m.saved[entry.Run.ID()] != first {
		t.Fatal("repeated observation changed terminal history")
	}
	if job.FailedAt == nil || !job.FailedAt.Equal(failedAt) {
		t.Fatal("reconciliation erased Mercury failure history")
	}
}

func TestInspectionProblemsNeverBecomeFailure(t *testing.T) {
	for _, tt := range []struct {
		name      string
		mutate    func(*workerclient.Job)
		err, want error
	}{
		{"missing", nil, workerclient.ErrJobNotFound, ErrJobMissing},
		{"authentication", nil, workerclient.ErrAuthentication, ErrAuthentication},
		{"unavailable", nil, workerclient.ErrTransport, ErrUnavailable},
		{"timeout", nil, context.DeadlineExceeded, ErrTimeout},
		{"malformed JSON", nil, workerclient.ErrProtocol, ErrProtocol},
		{"job identity", func(j *workerclient.Job) { j.ID = "other" }, nil, ErrIdentity},
		{"task identity", func(j *workerclient.Job) { j.TaskType = "sleep" }, nil, ErrIdentity},
		{"run identity", func(j *workerclient.Job) { j.Payload = json.RawMessage(`{"delivery_id":"delivery","run_id":"other"}`) }, nil, ErrIdentity},
		{"delivery identity", func(j *workerclient.Job) { j.Payload = json.RawMessage(`{"delivery_id":"other","run_id":"run"}`) }, nil, ErrIdentity},
		{"missing payload", func(j *workerclient.Job) { j.Payload = nil }, nil, ErrProtocol},
		{"unknown state", func(j *workerclient.Job) { j.State = "completed" }, nil, ErrProtocol},
		{"missing completion", func(j *workerclient.Job) { j.CompletedAt = nil }, nil, ErrProtocol},
		{"zero completion", func(j *workerclient.Job) { j.CompletedAt = new(time.Time) }, nil, ErrProtocol},
		{"failed state with completion", func(j *workerclient.Job) { j.State = workerclient.StateFailed; j.FailedAt = j.CompletedAt }, nil, ErrProtocol},
		{"failed state without failure time", func(j *workerclient.Job) { j.State = workerclient.StateFailed; j.CompletedAt = nil }, nil, ErrProtocol},
		{"terminal lease", func(j *workerclient.Job) { j.Lease = &workerclient.Lease{} }, nil, ErrProtocol},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := storeFor(t, "run")
			j := jobFor("run", workerclient.StateSucceeded)
			if tt.mutate != nil {
				tt.mutate(&j)
			}
			r := newTestReconciler(t, m, inspectFunc(func(context.Context, workerclient.JobID) (workerclient.Job, error) { return j, tt.err }), 1)
			if err := r.ReconcileBatch(context.Background()); !errors.Is(err, tt.want) || len(m.saved) != 0 {
				t.Fatalf("inspection: %v, saved=%d", err, len(m.saved))
			}
		})
	}
}

func TestSDKInspectionContract(t *testing.T) {
	for _, state := range []workerclient.State{workerclient.StateSucceeded, workerclient.StateFailed} {
		t.Run(string(state), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/v1/worker/jobs/job-run" || r.Header.Get("Authorization") != "Bearer fixture-token" {
					t.Error("wrong inspection contract")
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(jobFor("run", state))
			}))
			defer srv.Close()
			client, err := workerclient.New(workerclient.Config{ServerURL: srv.URL, BearerToken: "fixture-token", HTTPClient: srv.Client()})
			if err != nil {
				t.Fatal(err)
			}
			m := storeFor(t, "run")
			r := newTestReconciler(t, m, client, 1)
			if err := r.ReconcileBatch(context.Background()); err != nil || len(m.saved) != 1 || string(m.saved["run"].State()) != string(state) {
				t.Fatalf("confirmed SDK observation: %v", err)
			}
		})
	}
	for _, tt := range []struct {
		status int
		body   string
		want   error
	}{
		{404, `{"error":{"code":"job_not_found"}}`, ErrJobMissing},
		{401, `{"error":{"code":"unauthorized"}}`, ErrAuthentication},
		{403, `{}`, ErrAuthentication}, {503, `{}`, ErrUnavailable}, {429, `{}`, ErrUnavailable},
		{408, `{}`, ErrTimeout}, {200, `{`, ErrProtocol}, {200, `null`, ErrIdentity},
	} {
		t.Run(fmt.Sprint(tt.status, tt.body), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/v1/worker/jobs/job-run" || r.Header.Get("Authorization") != "Bearer fixture-token" {
					t.Error("wrong inspection contract")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()
			client, err := workerclient.New(workerclient.Config{ServerURL: srv.URL, BearerToken: "fixture-token", HTTPClient: srv.Client()})
			if err != nil {
				t.Fatal(err)
			}
			m := storeFor(t, "run")
			r := newTestReconciler(t, m, client, 1)
			if err := r.ReconcileBatch(context.Background()); !errors.Is(err, tt.want) || len(m.saved) != 0 {
				t.Fatalf("inspection: %v", err)
			}
		})
	}
}

func TestBatchProgressAndWrap(t *testing.T) {
	m := storeFor(t, "A", "b", "z")
	var seen []workerclient.JobID
	r := newTestReconciler(t, m, inspectFunc(func(_ context.Context, id workerclient.JobID) (workerclient.Job, error) {
		seen = append(seen, id)
		if id == "job-A" {
			return workerclient.Job{}, workerclient.ErrJobNotFound
		}
		if id == "job-b" {
			return jobFor("b", workerclient.StateRunning), nil
		}
		return jobFor("z", workerclient.StateSucceeded), nil
	}), 1)
	for range 5 {
		_ = r.ReconcileBatch(context.Background())
	}
	if fmt.Sprint(seen) != "[job-A job-b job-z job-A]" || len(m.saved) != 1 {
		t.Fatalf("starved or failed to wrap: %v", seen)
	}
}

func TestConcurrentReconcilersAndPersistenceFailure(t *testing.T) {
	m := storeFor(t, "run")
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	inspect := inspectFunc(func(context.Context, workerclient.JobID) (workerclient.Job, error) {
		entered <- struct{}{}
		<-release
		return jobFor("run", workerclient.StateSucceeded), nil
	})
	a, b := newTestReconciler(t, m, inspect, 1), newTestReconciler(t, m, inspect, 1)
	results := make(chan error, 2)
	go func() { results <- a.ReconcileBatch(context.Background()) }()
	go func() { results <- b.ReconcileBatch(context.Background()) }()
	<-entered
	<-entered
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if len(m.saved) != 1 {
		t.Fatal("duplicate observation")
	}
	m = storeFor(t, "run")
	m.recordErr = errors.New("private storage detail")
	r := newTestReconciler(t, m, inspectFunc(func(context.Context, workerclient.JobID) (workerclient.Job, error) {
		return jobFor("run", workerclient.StateSucceeded), nil
	}), 1)
	err := r.ReconcileBatch(context.Background())
	if !errors.Is(err, ErrPersistence) || len(m.saved) != 0 || strings.Contains(ErrorClass(err), "private") {
		t.Fatalf("storage error: %v", err)
	}
}

func TestCancellationDeadlineAndPolling(t *testing.T) {
	m := storeFor(t, "run")
	entered := make(chan struct{})
	inspect := inspectFunc(func(ctx context.Context, _ workerclient.JobID) (workerclient.Job, error) {
		close(entered)
		<-ctx.Done()
		return workerclient.Job{}, ctx.Err()
	})
	r := newTestReconciler(t, m, inspect, 1)
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- r.Run(ctx) }()
	<-entered
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation blocked")
	}
	if len(m.saved) != 0 {
		t.Fatal("fabricated canceled outcome")
	}
	r = newTestReconciler(t, m, inspectFunc(func(ctx context.Context, _ workerclient.JobID) (workerclient.Job, error) {
		<-ctx.Done()
		return workerclient.Job{}, ctx.Err()
	}), 1)
	r.config.OperationTimeout = 5 * time.Millisecond
	if err := r.ReconcileBatch(context.Background()); !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
	// A long poll cannot delay shutdown after the completed page.
	pageDone := make(chan struct{})
	r = newTestReconciler(t, m, inspectFunc(func(context.Context, workerclient.JobID) (workerclient.Job, error) {
		close(pageDone)
		return jobFor("run", workerclient.StateRunning), nil
	}), 1)
	r.config.PollInterval = time.Hour
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go func() { finished <- r.Run(ctx) }()
	<-pageDone
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("polling blocked shutdown")
	}
}
