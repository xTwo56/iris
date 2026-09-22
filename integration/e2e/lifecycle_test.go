// Package e2e_test exercises real executables and public APIs, never Mercury's
// internal packages. Explicit opt-in is mandatory because it starts processes,
// creates disposable databases and uses a user-owned public HTTPS ingress.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

type acceptanceResult struct {
	EventID string `json:"event_id"`
	Count   int    `json:"delivery_count"`
}
type deliveryView struct {
	ID         string    `json:"id"`
	EventID    string    `json:"event_id"`
	EndpointID string    `json:"endpoint_id"`
	CreatedAt  time.Time `json:"created_at"`
}
type terminalView struct {
	Job   string    `json:"mercury_job_id"`
	State string    `json:"state"`
	At    time.Time `json:"observed_at"`
}
type runView struct {
	ID         string        `json:"id"`
	DeliveryID string        `json:"delivery_id"`
	At         time.Time     `json:"created_at"`
	Trigger    string        `json:"trigger"`
	Resolution string        `json:"resolution"`
	Terminal   *terminalView `json:"terminal_observation"`
}
type outcomeView struct {
	FinishedAt     time.Time `json:"finished_at"`
	Status         *int      `json:"http_status"`
	Classification string    `json:"classification"`
}
type attemptView struct {
	ID          string       `json:"id"`
	RunID       string       `json:"run_id"`
	At          time.Time    `json:"started_at"`
	Observation string       `json:"observation"`
	Outcome     *outcomeView `json:"outcome"`
}
type redeliveryResult struct {
	DeliveryID string `json:"delivery_id"`
	RunID      string `json:"run_id"`
}

func list[T any](ctx context.Context, c *http.Client, origin, token, path string) ([]T, int) {
	var all []T
	cursor := ""
	for pages := 0; pages < 10; pages++ {
		query := "?limit=200"
		if cursor != "" {
			query += "&cursor=" + url.QueryEscape(cursor)
		}
		status, data, _ := call(ctx, c, origin, token, "GET", path+query, "", nil)
		if status != 200 {
			return nil, status
		}
		var page struct {
			Items []T     `json:"items"`
			Next  *string `json:"next_cursor"`
		}
		if json.Unmarshal(data, &page) != nil {
			return nil, 0
		}
		all = append(all, page.Items...)
		if page.Next == nil {
			return all, 200
		}
		cursor = *page.Next
	}
	return nil, 0
}

func TestDeliveryLifecycle(t *testing.T) {
	if os.Getenv("IRIS_E2E") != "1" {
		t.Skip("opt in with IRIS_E2E=1; see docs/e2e-lifecycle.txt")
	}
	if required(t, "IRIS_E2E_DISPOSABLE") != "1" {
		t.Fatal("IRIS_E2E_DISPOSABLE=1 must acknowledge dedicated disposable PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal("cannot locate Iris root")
	}
	mercuryRoot := required(t, "IRIS_E2E_MERCURY_DIR")
	irisAdmin := required(t, "IRIS_E2E_IRIS_ADMIN_DSN")
	mercuryAdmin := required(t, "IRIS_E2E_MERCURY_ADMIN_DSN")
	binDir := required(t, "IRIS_E2E_BIN_DIR")
	binaries := map[string]string{}
	for _, name := range []string{"iris-api", "iris-dispatcher", "iris-worker", "iris-reconciler", "mercury"} {
		binaries[name] = verifiedRuntimeBinary(t, binDir, name, name == "iris-worker" || name == "iris-reconciler")
	}
	origin := required(t, "IRIS_E2E_RECEIVER_ORIGIN")
	listen := os.Getenv("IRIS_E2E_RECEIVER_LISTEN")
	if listen == "" {
		listen = "127.0.0.1:19090"
	}
	receiver, destination, challenge := receiverServer(t, origin, listen)
	httpClient := localClient(t)
	// The ingress is manual setup. Check it before provisioning databases; ordinary
	// TLS verification and no redirects apply, and no credentials are sent here.
	await(t, ctx, "public HTTPS receiver ingress", 30*time.Second, nil, func(ctx context.Context) (bool, string) {
		status, data, _ := call(ctx, httpClient, destination, "", "GET", "/ready", "", nil)
		return status == 200 && string(data) == challenge, "receiver challenge unavailable; check public HTTPS forwarding, certificate and path preservation"
	})
	irisDB, irisDSN := database(t, ctx, "iris", irisAdmin, filepath.Join(root, "migrations"))
	mercuryDB, mercuryDSN := database(t, ctx, "mercury", mercuryAdmin, filepath.Join(mercuryRoot, "migrations"))
	management, producer, workerToken := randomHex(t, 32), randomHex(t, 32), randomHex(t, 32)
	keyBytes, _ := hex.DecodeString(randomHex(t, 32))
	encryption := base64.StdEncoding.EncodeToString(keyBytes)
	clear(keyBytes)
	irisAddress, mercuryAddress := freeAddress(t), freeAddress(t)
	irisURL, mercuryURL := "http://"+irisAddress, "http://"+mercuryAddress
	mercuryEnv := map[string]string{"MERCURY_DATABASE_URL": mercuryDSN, "MERCURY_ROLE": "api", "MERCURY_HTTP_LISTEN_ADDRESS": mercuryAddress, "MERCURY_PRODUCER_BEARER_TOKEN": producer, "MERCURY_WORKER_BEARER_TOKEN": workerToken, "MERCURY_EXTERNAL_TASK_TYPES": "webhook.deliver.v1"}
	processes := []*process{start(t, binaries["mercury"], "Mercury API", mercuryEnv)}
	processes = append(processes, start(t, binaries["mercury"], "Mercury scheduler", map[string]string{"MERCURY_DATABASE_URL": mercuryDSN, "MERCURY_ROLE": "scheduler", "MERCURY_RECOVERY_INTERVAL": "1s"}))
	processes = append(processes, start(t, binaries["iris-api"], "Iris API", map[string]string{"IRIS_DATABASE_URL": irisDSN, "IRIS_HTTP_ADDR": irisAddress, "IRIS_MANAGEMENT_TOKEN": management, "IRIS_SECRET_ENCRYPTION_KEY": encryption}))
	await(t, ctx, "authenticated APIs ready", 30*time.Second, processes, func(ctx context.Context) (bool, string) {
		iris, _, _ := call(ctx, httpClient, irisURL, management, "GET", "/v1/deliveries/e2e-missing", "", nil)
		mercury, _, _ := call(ctx, httpClient, mercuryURL, workerToken, "GET", "/v1/worker/jobs/e2e-missing", "", nil)
		return iris == 404 && mercury == 404, fmt.Sprintf("Iris=%d Mercury=%d; expected authenticated missing-resource responses", iris, mercury)
	})
	// Confirm these executables actually enforce separate producer/management auth.
	jsonCall(t, ctx, httpClient, irisURL, "", "POST", "/v1/endpoints", "", []byte(`{}`), 401, nil)
	jsonCall(t, ctx, httpClient, mercuryURL, "", "POST", "/v1/jobs", "", []byte(`{}`), 401, nil)
	jsonCall(t, ctx, httpClient, mercuryURL, producer, "GET", "/v1/worker/jobs/e2e-missing", "", nil, 401, nil)

	// Creation returns the secret once. It remains in harness memory; no response,
	// token, environment map or database connection string is written to diagnostics.
	var endpoint struct {
		ID     string `json:"id"`
		Secret string `json:"signing_secret"`
	}
	status, data, headers := call(ctx, httpClient, irisURL, management, "POST", "/v1/endpoints", "", encoded(t, map[string]string{"url": destination}))
	if status != 201 || json.Unmarshal(data, &endpoint) != nil || endpoint.ID == "" || headers.Get("Cache-Control") != "no-store" {
		t.Fatal("endpoint creation/one-time-secret response failed")
	}
	secret, err := base64.StdEncoding.Strict().DecodeString(endpoint.Secret)
	endpoint.Secret = ""
	clear(data)
	if err != nil || len(secret) != 32 {
		t.Fatal("endpoint creation did not return a valid signing secret")
	}
	defer clear(secret)
	var endpointRead map[string]json.RawMessage
	jsonCall(t, ctx, httpClient, irisURL, management, "GET", "/v1/endpoints/"+url.PathEscape(endpoint.ID), "", nil, 200, &endpointRead)
	if _, ok := endpointRead["signing_secret"]; ok {
		t.Fatal("endpoint read leaked signing secret")
	}
	jsonCall(t, ctx, httpClient, irisURL, management, "POST", "/v1/subscriptions", "", encoded(t, map[string]string{"endpoint_id": endpoint.ID, "event_type": "e2e.synthetic.created"}), 201, nil)

	// Acceptance and its exact replay occur before dispatch. Direct count assertions
	// supplement the history APIs to prove transaction fan-out and outbox uniqueness.
	payload := []byte("{\n  \"synthetic\": true, \"amount\": 1.00, \"escaped\": \"\\u0061\"\n}")
	eventID := "evt_e2e_" + randomHex(t, 8)
	eventAt := time.Now().UTC().Truncate(time.Microsecond)
	submission := []byte(fmt.Sprintf(`{"id":%q,"event_type":"e2e.synthetic.created","created_at":%q,"payload":%s}`, eventID, eventAt.Format(time.RFC3339Nano), payload))
	var accepted, replayed acceptanceResult
	jsonCall(t, ctx, httpClient, irisURL, management, "POST", "/v1/events", "event-key", submission, 201, &accepted)
	jsonCall(t, ctx, httpClient, irisURL, management, "POST", "/v1/events", "event-key", submission, 200, &replayed)
	if accepted != replayed || accepted.EventID != eventID || accepted.Count != 1 {
		t.Fatal("event replay changed the acceptance result")
	}
	assertCounts(t, ctx, irisDB, 1, 1)
	deliveries, code := list[deliveryView](ctx, httpClient, irisURL, management, "/v1/events/"+eventID+"/deliveries")
	if code != 200 || len(deliveries) != 1 || deliveries[0].EventID != eventID || deliveries[0].EndpointID != endpoint.ID {
		t.Fatal("event delivery history did not match acceptance")
	}
	deliveryID := deliveries[0].ID
	runsPath := "/v1/deliveries/" + url.PathEscape(deliveryID) + "/runs"
	initial, code := list[runView](ctx, httpClient, irisURL, management, runsPath)
	if code != 200 || len(initial) != 1 || initial[0].Trigger != "initial" || initial[0].Terminal != nil || initial[0].Resolution != "unresolved" {
		t.Fatal("initial run history invalid")
	}
	initialID := initial[0].ID
	receiver.configure(secret, payload, eventID, deliveryID)

	// Real runtime roles own dispatch, SDK lifecycle and reconciliation. There are
	// no database transactions spanning HTTP, no fake leases and no retry shortcuts.
	processes = append(processes, start(t, binaries["iris-dispatcher"], "Iris dispatcher", map[string]string{"IRIS_DATABASE_URL": irisDSN, "IRIS_MERCURY_URL": mercuryURL, "IRIS_MERCURY_SUBMISSION_TOKEN": producer, "IRIS_DISPATCHER_POLL_INTERVAL": "200ms", "IRIS_DISPATCHER_BATCH_SIZE": "10"}))
	processes = append(processes, start(t, binaries["iris-worker"], "Iris worker", map[string]string{"IRIS_DATABASE_URL": irisDSN, "IRIS_MERCURY_URL": mercuryURL, "IRIS_MERCURY_WORKER_TOKEN": workerToken, "IRIS_SECRET_ENCRYPTION_KEY": encryption, "IRIS_WORKER_ID": "iris-e2e-worker", "IRIS_WORKER_CONCURRENCY": "1", "IRIS_WORKER_POLL_INTERVAL": "200ms"}))
	processes = append(processes, start(t, binaries["iris-reconciler"], "Iris reconciler", map[string]string{"IRIS_DATABASE_URL": irisDSN, "IRIS_MERCURY_URL": mercuryURL, "IRIS_MERCURY_WORKER_TOKEN": workerToken, "IRIS_RECONCILER_POLL_INTERVAL": "200ms", "IRIS_RECONCILER_BATCH_SIZE": "10"}))
	inspector, err := workerclient.New(workerclient.Config{ServerURL: mercuryURL, BearerToken: workerToken, HTTPClient: httpClient})
	if err != nil {
		t.Fatal("public SDK inspection client configuration failed")
	}
	// Inspect using only the pinned public SDK. Never print the returned Job: it
	// can contain lease credentials and last_error, neither of which is diagnostic output.
	waitRun := func(runID string, minExecutions int) (runView, workerclient.Job) {
		t.Helper()
		var observed runView
		var confirmed workerclient.Job
		jobID := ""
		await(t, ctx, "run terminal reconciliation", 3*time.Minute, processes, func(ctx context.Context) (bool, string) {
			var job *string
			var submissionKey string
			if err := irisDB.QueryRow(ctx, `SELECT mercury_job_id,submission_key FROM outbox WHERE run_id=$1`, runID).Scan(&job, &submissionKey); err != nil {
				return false, "outbox lookup unavailable"
			}
			if submissionKey != "iris:delivery-run:"+runID {
				t.Fatal("outbox submission key changed")
			}
			good, bad, temporary, reason := receiver.totals()
			if bad > 0 {
				t.Fatalf("receiver rejected a webhook: %s (verified=%d rejected=%d)", reason, good, bad)
			}
			if job == nil {
				return false, "outbox pending: inspect producer credential, task allowlist or dispatcher startup"
			}
			if jobID != "" && jobID != *job {
				t.Fatal("acknowledged Mercury job identity changed")
			}
			jobID = *job
			current, err := inspector.Inspect(ctx, workerclient.JobID(jobID))
			if err != nil {
				var op *workerclient.OperationError
				if errors.As(err, &op) {
					return false, fmt.Sprintf("SDK inspection HTTP %d", op.StatusCode)
				}
				return false, "SDK inspection unavailable"
			}
			var refs map[string]string
			if current.ID != workerclient.JobID(jobID) || current.TaskType != "webhook.deliver.v1" || json.Unmarshal(current.Payload, &refs) != nil || len(refs) != 2 || refs["run_id"] != runID || refs["delivery_id"] != deliveryID || current.MaxAttempts != 3 {
				t.Fatal("Mercury job contract/association mismatch")
			}
			if current.State == workerclient.StateFailed {
				t.Fatalf("Mercury job failed: attempts=%d receiver_verified=%d temporary_responses=%d; inspect TLS/SSRF, credentials and storage configuration", current.AttemptsStarted, good, temporary)
			}
			views, status := list[runView](ctx, httpClient, irisURL, management, runsPath)
			if status != 200 {
				return false, "run history unavailable"
			}
			for _, view := range views {
				if view.ID == runID && view.Terminal != nil {
					if view.Terminal.Job != jobID || view.Terminal.State != "succeeded" || view.Resolution != "terminal" {
						t.Fatal("reconciled terminal association/state mismatch")
					}
					// These are separate remote/local reads. Reconciliation can commit
					// after our earlier nonterminal inspection; refresh rather than
					// treating that ordinary race as an inconsistent result.
					if current.State != workerclient.StateSucceeded {
						return false, "terminal observation arrived after SDK read; refreshing"
					}
					if current.AttemptsStarted < minExecutions || current.CompletedAt == nil || current.Lease != nil {
						t.Fatal("terminal Mercury lifecycle projection invalid")
					}
					observed = view
					confirmed = current
					return true, "confirmed"
				}
			}
			return false, fmt.Sprintf("job_state=%s executions=%d receiver_verified=%d temporary_responses=%d; awaiting terminal history", knownState(current.State), current.AttemptsStarted, good, temporary)
		})
		return observed, confirmed
	}
	firstRun, firstJob := waitRun(initialID, 2)
	firstAttempts, code := list[attemptView](ctx, httpClient, irisURL, management, "/v1/runs/"+url.PathEscape(initialID)+"/attempts")
	if code != 200 {
		t.Fatal("initial attempt history unavailable")
	}
	seen := map[string]bool{}
	retry, success := false, false
	for _, attempt := range firstAttempts {
		if attempt.RunID != initialID || attempt.ID == "" || seen[attempt.ID] {
			t.Fatal("attempt identity/run association invalid")
		}
		seen[attempt.ID] = true
		if attempt.Outcome != nil && attempt.Outcome.Status != nil {
			retry = retry || (*attempt.Outcome.Status == 503 && attempt.Outcome.Classification == "retryable_failure")
			success = success || (*attempt.Outcome.Status == 204 && attempt.Outcome.Classification == "succeeded")
		}
	}
	if !retry || !success {
		t.Fatal("same-run history lacks observed temporary failure and success")
	}
	assertCounts(t, ctx, irisDB, 1, 1)
	if count(t, ctx, mercuryDB, `SELECT count(*) FROM jobs`) != 1 {
		t.Fatal("automatic retry created another Mercury job")
	}
	good, bad, temporary, _ := receiver.totals()
	if good < 2 || bad != 0 || temporary < 1 {
		t.Fatal("independent receiver verification/retry evidence missing")
	}

	// A new request creates a separate execution cycle after confirmed termination.
	// Replay it immediately, before the new cycle need be reconciled. The stored
	// receipt must win over the unresolved-run guard and preserve the returned run.
	beforeRun, beforeAttempts := encoded(t, firstRun), encoded(t, firstAttempts)
	manualPath := "/v1/deliveries/" + url.PathEscape(deliveryID) + "/redeliver"
	var manual, manualReplay redeliveryResult
	jsonCall(t, ctx, httpClient, irisURL, management, "POST", manualPath, "manual-key", []byte(`{}`), 201, &manual)
	jsonCall(t, ctx, httpClient, irisURL, management, "POST", manualPath, "manual-key", []byte(`{}`), 200, &manualReplay)
	if manual != manualReplay || manual.DeliveryID != deliveryID || manual.RunID == initialID || manual.RunID == "" {
		t.Fatal("manual redelivery/replay identity invalid")
	}
	assertCounts(t, ctx, irisDB, 1, 2)
	secondRun, secondJob := waitRun(manual.RunID, 1)
	if secondRun.Trigger != "manual_redelivery" || secondJob.ID == firstJob.ID || secondJob.MaxAttempts != firstJob.MaxAttempts {
		t.Fatal("manual run did not receive a distinct job with a fresh attempt budget")
	}
	var unchanged deliveryView
	jsonCall(t, ctx, httpClient, irisURL, management, "GET", "/v1/deliveries/"+url.PathEscape(deliveryID), "", nil, 200, &unchanged)
	if !reflect.DeepEqual(unchanged, deliveries[0]) {
		t.Fatal("manual redelivery changed original delivery identity/metadata")
	}
	afterRuns, code := list[runView](ctx, httpClient, irisURL, management, runsPath)
	if code != 200 || len(afterRuns) != 2 {
		t.Fatal("manual replay duplicated runs")
	}
	found := false
	for _, view := range afterRuns {
		if view.ID == initialID {
			found = bytes.Equal(beforeRun, encoded(t, view))
		}
	}
	if !found {
		t.Fatal("original run/terminal history changed")
	}
	afterAttempts, code := list[attemptView](ctx, httpClient, irisURL, management, "/v1/runs/"+url.PathEscape(initialID)+"/attempts")
	if code != 200 || !bytes.Equal(beforeAttempts, encoded(t, afterAttempts)) {
		t.Fatal("original attempt history changed")
	}
	manualAttempts, code := list[attemptView](ctx, httpClient, irisURL, management, "/v1/runs/"+url.PathEscape(manual.RunID)+"/attempts")
	if code != 200 || len(manualAttempts) == 0 {
		t.Fatal("manual run attempt history missing")
	}
	for _, attempt := range manualAttempts {
		if attempt.RunID != manual.RunID || seen[attempt.ID] {
			t.Fatal("manual run reused an old attempt")
		}
	}
	jsonCall(t, ctx, httpClient, irisURL, management, "POST", manualPath, "manual-key", []byte(`{}`), 200, &manualReplay)
	if manualReplay != manual {
		t.Fatal("completed manual request replay changed result")
	}
	assertCounts(t, ctx, irisDB, 1, 2)
	if count(t, ctx, mercuryDB, `SELECT count(*) FROM jobs`) != 2 {
		t.Fatal("manual replay duplicated Mercury jobs")
	}
	finalGood, finalBad, _, reason := receiver.totals()
	if finalBad != 0 || finalGood <= good {
		t.Fatalf("manual receiver verification missing: verified=%d rejected=%d reason=%s", finalGood, finalBad, reason)
	}
	var storedBody []byte
	var storedAt time.Time
	var storedType string
	if err := irisDB.QueryRow(ctx, `SELECT payload,created_at,event_type FROM events WHERE id=$1`, eventID).Scan(&storedBody, &storedAt, &storedType); err != nil || !bytes.Equal(storedBody, payload) || !storedAt.Equal(eventAt) || storedType != "e2e.synthetic.created" {
		t.Fatal("original event fields changed")
	}
	if count(t, ctx, irisDB, `SELECT count(*) FROM events`) != 1 {
		t.Fatal("event replay duplicated events")
	}
	t.Logf("verified lifecycle: 1 delivery, 2 runs/jobs, %d initial attempts, %d manual attempts, %d independently authenticated receiver requests", len(firstAttempts), len(manualAttempts), finalGood)
}
func knownState(state workerclient.State) string {
	switch state {
	case workerclient.StateQueued, workerclient.StateLeased, workerclient.StateRunning, workerclient.StateRetryScheduled, workerclient.StateSucceeded, workerclient.StateFailed:
		return string(state)
	default:
		return "unknown"
	}
}
