package mercury

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/application/dispatcher"
)

const accepted = `{"id":"job-1","task_type":"webhook.deliver.v1","payload":{"delivery_id":"delivery-1","run_id":"run-1"},"max_attempts":3,"state":"succeeded"}`

var submitted = dispatcher.Submission{Key: "iris:delivery-run:run-1", DeliveryID: "delivery-1", RunID: "run-1"}

func TestSubmissionUncertainReplayAcrossClients(t *testing.T) {
	var mu sync.Mutex
	var requests, keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/jobs" || r.Header.Get("Authorization") != "Bearer producer-secret" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("incorrect submission envelope")
		}
		data, _ := io.ReadAll(r.Body)
		mu.Lock()
		requests = append(requests, string(data))
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		first := len(requests) == 1
		mu.Unlock()
		if first {
			w.WriteHeader(201)
			fmt.Fprint(w, `{"id":`)
			return
		} // accepted remotely; response lost/truncated
		w.WriteHeader(200)
		fmt.Fprint(w, accepted)
	}))
	defer server.Close()
	first, err := New(server.URL, "producer-secret", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Submit(context.Background(), submitted); !errors.Is(err, dispatcher.ErrAcknowledgment) {
		t.Fatalf("uncertain response: %v", err)
	}
	first.Close()
	// A fresh process/client must construct exactly the same bytes, not a new time.
	second, err := New(server.URL, "producer-secret", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if id, err := second.Submit(context.Background(), submitted); err != nil || id != "job-1" {
		t.Fatalf("replay=%q %v", id, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 || requests[0] != requests[1] || keys[0] != submitted.Key || keys[1] != keys[0] {
		t.Fatal("replay changed submission identity")
	}
	if requests[0] != `{"task_type":"webhook.deliver.v1","payload":{"delivery_id":"delivery-1","run_id":"run-1"},"max_attempts":3}` {
		t.Fatalf("v1 request changed: %s", requests[0])
	}
}

func TestSubmissionResponses(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status int
		body   string
		want   error
	}{
		{"created", 201, accepted, nil}, {"replayed terminal job", 200, accepted, nil},
		{"authentication", 401, "secret detail", dispatcher.ErrAuthentication}, {"forbidden", 403, "", dispatcher.ErrAuthentication},
		{"conflict", 409, "", dispatcher.ErrConflict}, {"unsupported task", 400, "", dispatcher.ErrConfiguration},
		{"redirect", 302, "", dispatcher.ErrConfiguration}, {"rate limit", 429, "", dispatcher.ErrTransient}, {"server", 503, "", dispatcher.ErrTransient},
		{"empty acknowledgment", 201, `{}`, dispatcher.ErrAcknowledgment}, {"wrong run", 200, strings.ReplaceAll(accepted, "run-1", "other"), dispatcher.ErrAcknowledgment},
		{"wrong policy", 200, strings.ReplaceAll(accepted, `"max_attempts":3`, `"max_attempts":4`), dispatcher.ErrAcknowledgment},
		{"oversized", 201, strings.Repeat(" ", maxResponseBytes+1), dispatcher.ErrAcknowledgment},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "/other")
				w.WriteHeader(tt.status)
				fmt.Fprint(w, tt.body)
			}))
			defer server.Close()
			client, err := New(server.URL, "credential", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			_, err = client.Submit(context.Background(), submitted)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v want %v", err, tt.want)
			}
		})
	}
}

func TestSubmissionCancellationAndTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	defer server.Close()
	client, err := New(server.URL, "credential", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Submit(context.Background(), submitted); !errors.Is(err, dispatcher.ErrTransient) {
		t.Fatalf("timeout=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Submit(ctx, submitted); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}

func TestSubmissionConfiguration(t *testing.T) {
	for _, origin := range []string{"", "http://mercury.example", "https://user:secret@example.com", "https://example.com/path", "https://example.com?token=secret"} {
		if _, err := New(origin, "token", time.Second); !errors.Is(err, dispatcher.ErrConfiguration) {
			t.Fatal("invalid origin accepted")
		}
	}
	for _, key := range []string{"", "a b", strings.Repeat("a", 256)} {
		client, err := New("https://example.com", "token", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		request := submitted
		request.Key = key
		if _, err := client.Submit(context.Background(), request); !errors.Is(err, dispatcher.ErrConfiguration) {
			t.Fatal("invalid key accepted")
		}
		client.Close()
	}
}

func TestCertificateFailureIsConfigurationError(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS request reached handler") }))
	defer server.Close()
	client, err := New(server.URL, "credential", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Submit(context.Background(), submitted); !errors.Is(err, dispatcher.ErrConfiguration) {
		t.Fatalf("certificate error=%v", err)
	}
}
