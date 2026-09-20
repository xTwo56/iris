package webhookworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/delivery/transport"
	"github.com/xTwo56/iris/internal/endpoint"
	endpointpg "github.com/xTwo56/iris/internal/endpoint/postgres"
	"github.com/xTwo56/iris/internal/endpointsecret"
	secretpg "github.com/xTwo56/iris/internal/endpointsecret/postgres"
	"github.com/xTwo56/iris/internal/event"
	eventpg "github.com/xTwo56/iris/internal/event/postgres"
	"github.com/xtwo56/mercury/remoteworker"
	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

type getter[I ~string, V any] func(context.Context, I) (V, error)

func (g getter[I, V]) GetByID(ctx context.Context, id I) (V, error) { return g(ctx, id) }

type sendFunc func(context.Context, string, []byte, event.ID, delivery.ID, []byte) transport.Observation

func (f sendFunc) Send(ctx context.Context, u string, b []byte, e event.ID, d delivery.ID, s []byte) transport.Observation {
	return f(ctx, u, b, e, d, s)
}

type history struct {
	starts               []delivery.AttemptStart
	outcomes             []delivery.AttemptOutcome
	startErr, outcomeErr error
	afterStart           func()
	inspectOutcome       func(context.Context)
}

func (h *history) AppendStart(ctx context.Context, s delivery.AttemptStart) error {
	if h.startErr != nil {
		return h.startErr
	}
	h.starts = append(h.starts, s)
	if h.afterStart != nil {
		h.afterStart()
	}
	return nil
}
func (h *history) AppendOutcome(ctx context.Context, o delivery.AttemptOutcome) error {
	if h.inspectOutcome != nil {
		h.inspectOutcome(ctx)
	}
	if h.outcomeErr != nil {
		return h.outcomeErr
	}
	h.outcomes = append(h.outcomes, o)
	return nil
}

type fixture struct {
	deps           Dependencies
	history        *history
	sends          int
	event          event.Event
	endpoint       endpoint.Endpoint
	delivery       delivery.Delivery
	run            delivery.Run
	observation    transport.Observation
	sentEvents     []event.ID
	sentDeliveries []delivery.ID
}

func setup(t *testing.T) *fixture {
	t.Helper()
	at := time.Date(2026, 9, 21, 0, 0, 0, 123456000, time.UTC)
	e, err := event.New("event-1", "order.created", at, []byte(" {\n \"number\": 1.00e+02 }\n"))
	if err != nil {
		t.Fatal(err)
	}
	ep, err := endpoint.New("endpoint-1", "https://public.example/webhook", at)
	if err != nil {
		t.Fatal(err)
	}
	d, err := delivery.New("delivery-1", e.ID(), ep.ID(), at)
	if err != nil {
		t.Fatal(err)
	}
	run, err := delivery.NewRun("run-1", d.ID(), at, delivery.TriggerInitial)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{history: &history{}, event: e, endpoint: ep, delivery: d, run: run, observation: transport.Observation{HTTPStatus: 204, Classification: delivery.ClassificationSucceeded}}
	counter := 0
	f.deps = Dependencies{
		Runs:       getter[delivery.RunID, delivery.Run](func(ctx context.Context, id delivery.RunID) (delivery.Run, error) { return f.run, nil }),
		Deliveries: getter[delivery.ID, delivery.Delivery](func(ctx context.Context, id delivery.ID) (delivery.Delivery, error) { return f.delivery, nil }),
		Events:     getter[event.ID, event.Event](func(ctx context.Context, id event.ID) (event.Event, error) { return f.event, nil }),
		Endpoints:  getter[endpoint.ID, endpoint.Endpoint](func(ctx context.Context, id endpoint.ID) (endpoint.Endpoint, error) { return f.endpoint, nil }), Attempts: f.history,
		Credentials: func(ctx context.Context, id endpoint.ID) ([]byte, error) {
			if id != f.endpoint.ID() {
				t.Error("wrong credential reference")
			}
			return bytes.Repeat([]byte{7}, 32), nil
		},
		NewAttemptID: func() (delivery.AttemptID, error) {
			counter++
			return delivery.AttemptID(fmt.Sprintf("attempt-%d", counter)), nil
		}, Now: func() time.Time { return at.Add(time.Second) }, StorageTimeout: time.Second, CleanupTimeout: 100 * time.Millisecond,
	}
	f.deps.Sender = sendFunc(func(ctx context.Context, url string, body []byte, e event.ID, d delivery.ID, secret []byte) transport.Observation {
		f.sends++
		if len(f.history.starts) != f.sends {
			t.Error("send occurred before durable start")
		}
		if !bytes.Equal(body, f.event.Payload()) || url != f.endpoint.URL() || len(secret) != 32 {
			t.Error("send changed bytes/destination/credentials")
		}
		f.sentEvents = append(f.sentEvents, e)
		f.sentDeliveries = append(f.sentDeliveries, d)
		return f.observation
	})
	return f
}

const validPayload = `{"delivery_id":"delivery-1","run_id":"run-1"}`

func (f *fixture) execute(t *testing.T, ctx context.Context, raw string) (json.RawMessage, error) {
	t.Helper()
	h, err := New(f.deps)
	if err != nil {
		t.Fatal(err)
	}
	return h.Execute(ctx, json.RawMessage(raw))
}
func requireFailure(t *testing.T, err, want error, classification workerclient.FailureClassification) {
	t.Helper()
	var failure *remoteworker.Failure
	if !errors.Is(err, want) || !errors.As(err, &failure) || failure.Classification != classification {
		t.Fatalf("failure=%v want %v/%s", err, want, classification)
	}
}

func TestObservedOutcomesAndStableIdentity(t *testing.T) {
	for _, tt := range []struct {
		name           string
		observation    transport.Observation
		want           error
		classification workerclient.FailureClassification
	}{
		{name: "success", observation: transport.Observation{HTTPStatus: 204, Classification: delivery.ClassificationSucceeded}},
		{name: "observed success with drain timeout", observation: transport.Observation{HTTPStatus: 200, Classification: delivery.ClassificationSucceeded, Err: transport.ErrTimeout}},
		{name: "retryable HTTP", observation: transport.Observation{HTTPStatus: 503, Classification: delivery.ClassificationRetryableFailure}, want: ErrRetryableDelivery, classification: workerclient.FailureRetryable},
		{name: "permanent HTTP", observation: transport.Observation{HTTPStatus: 400, Classification: delivery.ClassificationPermanentFailure}, want: ErrPermanentDelivery, classification: workerclient.FailurePermanent},
		{name: "network timeout", observation: transport.Observation{Classification: delivery.ClassificationRetryableFailure, Err: transport.ErrTimeout}, want: ErrRetryableDelivery, classification: workerclient.FailureRetryable},
		{name: "blocked destination", observation: transport.Observation{Classification: delivery.ClassificationPermanentFailure, Err: transport.ErrBlockedDestination}, want: ErrPermanentDelivery, classification: workerclient.FailurePermanent},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := setup(t)
			f.observation = tt.observation
			for range 2 {
				result, err := f.execute(t, context.Background(), validPayload)
				if tt.want == nil {
					if err != nil || !json.Valid(result) {
						t.Fatalf("success=%s %v", result, err)
					}
				} else {
					requireFailure(t, err, tt.want, tt.classification)
				}
			}
			if f.sends != 2 || len(f.history.outcomes) != 2 || f.history.starts[0].ID() == f.history.starts[1].ID() {
				t.Fatal("attempt identities were reused")
			}
			for i, o := range f.history.outcomes {
				status, present := o.HTTPStatus()
				if present != (tt.observation.HTTPStatus != 0) || status != tt.observation.HTTPStatus || o.AttemptID() != f.history.starts[i].ID() {
					t.Fatal("outcome lost status/identity")
				}
				if f.sentEvents[i] != f.event.ID() || f.sentDeliveries[i] != f.delivery.ID() {
					t.Fatal("stable identities changed")
				}
			}
		})
	}
}
func TestPayloadValidation(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `[]`, `{"delivery_id":" ","run_id":"r"}`, validPayload + ` {}`, `{"delivery_id":"d","run_id":"r","version":2}`, `{"delivery_id":"d","run_id":"r","url":"https://example.com"}`} {
		f := setup(t)
		_, err := f.execute(t, context.Background(), raw)
		requireFailure(t, err, ErrPayload, workerclient.FailurePermanent)
		if f.sends != 0 || len(f.history.starts) != 0 {
			t.Fatal("invalid data caused attempt")
		}
	}
}
func TestReferencesAndCredentials(t *testing.T) {
	for _, tt := range []struct {
		name           string
		change         func(*fixture)
		want           error
		classification workerclient.FailureClassification
	}{
		{"missing run", func(f *fixture) {
			f.deps.Runs = getter[delivery.RunID, delivery.Run](func(context.Context, delivery.RunID) (delivery.Run, error) {
				return delivery.Run{}, deliverypg.ErrRunNotFound
			})
		}, ErrReferences, workerclient.FailurePermanent},
		{"missing delivery", func(f *fixture) {
			f.deps.Deliveries = getter[delivery.ID, delivery.Delivery](func(context.Context, delivery.ID) (delivery.Delivery, error) {
				return delivery.Delivery{}, deliverypg.ErrNotFound
			})
		}, ErrReferences, workerclient.FailurePermanent},
		{"missing event", func(f *fixture) {
			f.deps.Events = getter[event.ID, event.Event](func(context.Context, event.ID) (event.Event, error) { return event.Event{}, eventpg.ErrNotFound })
		}, ErrReferences, workerclient.FailurePermanent},
		{"missing endpoint", func(f *fixture) {
			f.deps.Endpoints = getter[endpoint.ID, endpoint.Endpoint](func(context.Context, endpoint.ID) (endpoint.Endpoint, error) {
				return endpoint.Endpoint{}, endpointpg.ErrNotFound
			})
		}, ErrReferences, workerclient.FailurePermanent},
		{"run delivery mismatch", func(f *fixture) {
			f.run, _ = delivery.NewRun("run-1", "other", f.run.CreatedAt(), delivery.TriggerInitial)
		}, ErrReferences, workerclient.FailurePermanent},
		{"event mismatch", func(f *fixture) { f.event, _ = event.New("other", "type", f.event.CreatedAt(), []byte(`{}`)) }, ErrReferences, workerclient.FailurePermanent},
		{"endpoint mismatch", func(f *fixture) {
			f.endpoint, _ = endpoint.New("other", "https://public.example", f.endpoint.CreatedAt())
		}, ErrReferences, workerclient.FailurePermanent},
		{"missing credentials", func(f *fixture) {
			f.deps.Credentials = func(context.Context, endpoint.ID) ([]byte, error) { return nil, secretpg.ErrNotFound }
		}, ErrCredentials, workerclient.FailurePermanent},
		{"corrupt credentials", func(f *fixture) {
			f.deps.Credentials = func(context.Context, endpoint.ID) ([]byte, error) { return nil, endpointsecret.ErrInvalid }
		}, ErrCredentials, workerclient.FailurePermanent},
		{"short secret", func(f *fixture) {
			f.deps.Credentials = func(context.Context, endpoint.ID) ([]byte, error) { return []byte{1}, nil }
		}, ErrCredentials, workerclient.FailurePermanent},
		{"credential storage", func(f *fixture) {
			f.deps.Credentials = func(context.Context, endpoint.ID) ([]byte, error) { return nil, errors.New("private database detail") }
		}, ErrStorage, workerclient.FailureRetryable},
		{"reference storage", func(f *fixture) {
			f.deps.Runs = getter[delivery.RunID, delivery.Run](func(context.Context, delivery.RunID) (delivery.Run, error) {
				return delivery.Run{}, errors.New("private database detail")
			})
		}, ErrStorage, workerclient.FailureRetryable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := setup(t)
			tt.change(f)
			_, err := f.execute(t, context.Background(), validPayload)
			requireFailure(t, err, tt.want, tt.classification)
			if f.sends != 0 || len(f.history.starts) != 0 {
				t.Fatal("invalid reference caused attempt")
			}
		})
	}
}
func TestPersistenceFailures(t *testing.T) {
	for _, startFailure := range []bool{true, false} {
		f := setup(t)
		want := ErrOutcome
		if startFailure {
			f.history.startErr = errors.New("storage unavailable")
			want = ErrStart
		} else {
			f.history.outcomeErr = errors.New("storage unavailable")
		}
		result, err := f.execute(t, context.Background(), validPayload)
		requireFailure(t, err, want, workerclient.FailureRetryable)
		if result != nil {
			t.Fatal("reported success on storage failure")
		}
		if startFailure && f.sends != 0 {
			t.Fatal("sent without start")
		}
		if !startFailure && (f.sends != 1 || len(f.history.outcomes) != 0) {
			t.Fatal("resent after outcome failure")
		}
	}
}
func TestCancellation(t *testing.T) {
	for _, observed := range []bool{false, true} {
		f := setup(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.deps.Sender = sendFunc(func(context.Context, string, []byte, event.ID, delivery.ID, []byte) transport.Observation {
			cancel()
			o := transport.Observation{Err: context.Canceled, Classification: delivery.ClassificationRetryableFailure}
			if observed {
				o.HTTPStatus = 204
				o.Classification = delivery.ClassificationSucceeded
			}
			return o
		})
		cleanup := false
		f.history.inspectOutcome = func(ctx context.Context) {
			cleanup = true
			if ctx.Err() != nil {
				t.Error("cleanup used canceled context")
			}
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > f.deps.CleanupTimeout {
				t.Error("unbounded cleanup")
			}
		}
		result, err := f.execute(t, ctx, validPayload)
		if !errors.Is(err, context.Canceled) || result != nil {
			t.Fatalf("cancellation=%s %v", result, err)
		}
		if observed {
			if !cleanup || len(f.history.outcomes) != 1 {
				t.Fatal("lost genuinely observed response")
			}
		} else if cleanup || len(f.history.outcomes) != 0 {
			t.Fatal("fabricated outcome after cancellation")
		}
	}
	f := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	f.history.afterStart = cancel
	_, err := f.execute(t, ctx, validPayload)
	if !errors.Is(err, context.Canceled) || f.sends != 0 || len(f.history.outcomes) != 0 {
		t.Fatal("sent after cancellation between start and send")
	}
}

func TestCanceledBeforeExecution(t *testing.T) {
	f := setup(t)
	f.deps.Runs = getter[delivery.RunID, delivery.Run](func(context.Context, delivery.RunID) (delivery.Run, error) {
		t.Error("read after cancellation")
		return delivery.Run{}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := f.execute(t, ctx, validPayload)
	if result != nil || !errors.Is(err, context.Canceled) || len(f.history.starts) != 0 {
		t.Fatal("canceled execution performed work")
	}
}

func TestCleanupFailureDoesNotReportSuccess(t *testing.T) {
	f := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.history.outcomeErr = errors.New("storage failure")
	f.deps.Sender = sendFunc(func(context.Context, string, []byte, event.ID, delivery.ID, []byte) transport.Observation {
		cancel()
		return transport.Observation{HTTPStatus: 204, Classification: delivery.ClassificationSucceeded, Err: context.Canceled}
	})
	result, err := f.execute(t, ctx, validPayload)
	requireFailure(t, err, ErrOutcome, workerclient.FailureRetryable)
	if result != nil || len(f.history.outcomes) != 0 {
		t.Fatal("failed cleanup reported success")
	}
}

func TestContextPropagationAndSecretLifetime(t *testing.T) {
	f := setup(t)
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "execution")
	check := func(ctx context.Context) {
		t.Helper()
		if ctx.Value(key{}) != "execution" {
			t.Error("execution context lost")
		}
	}
	f.deps.Runs = getter[delivery.RunID, delivery.Run](func(ctx context.Context, id delivery.RunID) (delivery.Run, error) { check(ctx); return f.run, nil })
	secret := bytes.Repeat([]byte{8}, 32)
	f.deps.Credentials = func(ctx context.Context, id endpoint.ID) ([]byte, error) { check(ctx); return secret, nil }
	f.deps.Sender = sendFunc(func(ctx context.Context, _ string, _ []byte, _ event.ID, _ delivery.ID, s []byte) transport.Observation {
		check(ctx)
		if !bytes.Equal(s, secret) {
			t.Error("secret changed before signing")
		}
		return f.observation
	})
	f.history.inspectOutcome = check
	if _, err := f.execute(t, ctx, validPayload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(secret, make([]byte, 32)) {
		t.Fatal("plaintext secret retained after execution")
	}
}
