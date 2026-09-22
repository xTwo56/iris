package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const sdkVersion = "v0.0.0-20260920225522-9253ed6b4571"

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal("random test identity generation failed")
	}
	return hex.EncodeToString(b)
}
func required(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Fatalf("opted-in test requires %s", name)
	}
	return v
}

// Create databases, never reuse them. Dedicated admin URLs must select postgres;
// only successfully created, randomly named child databases can reach cleanup.
// Both migrations and verification SQL stay outside application runtimes.
func database(t *testing.T, ctx context.Context, role, dsn, migrations string) (*pgxpool.Pool, string) {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil || config.Database != "postgres" {
		t.Fatalf("%s admin DSN must select postgres on a dedicated disposable server", role)
	}
	config.ConnectTimeout = 5 * time.Second
	admin, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("%s disposable database admin connection failed", role)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	name := "iris_e2e_" + role + "_" + randomHex(t, 10)
	quoted := pgx.Identifier{name}.Sanitize()
	op, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := admin.Exec(op, "CREATE DATABASE "+quoted); err != nil {
		t.Fatalf("%s disposable database creation failed", role)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+quoted); err != nil {
			t.Errorf("cleanup failed for harness-owned database %s; stop leftover harness processes before dropping this exact database", name)
		}
	})
	childDSN := databaseURL(t, dsn, name)
	pool, err := pgxpool.New(ctx, childDSN)
	if err != nil {
		t.Fatalf("%s disposable pool creation failed", role)
	}
	t.Cleanup(pool.Close)
	files, err := filepath.Glob(filepath.Join(migrations, "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("%s migration files missing", role)
	}
	sort.Strings(files)
	for _, file := range files {
		sql, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("%s migration read failed", role)
		}
		migrationCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		// Iris files own their transactions; Mercury files use one explicit tx each.
		if role == "mercury" {
			tx, e := pool.Begin(migrationCtx)
			if e == nil {
				_, e = tx.Exec(migrationCtx, string(sql))
				if e == nil {
					e = tx.Commit(migrationCtx)
				}
				_ = tx.Rollback(migrationCtx)
			}
			err = e
		} else {
			_, err = pool.Exec(migrationCtx, string(sql))
		}
		cancel()
		if err != nil {
			t.Fatalf("%s migration %s failed (details suppressed)", role, filepath.Base(file))
		}
	}
	return pool, childDSN
}

// Commands run as real independent processes. Configuration is allowlisted rather
// than inheriting development database URLs, tokens or TLS/proxy overrides.
// No application logs are dumped: diagnostics below expose only safe projections.
type process struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}
}

func start(t *testing.T, bin, name string, env map[string]string) *process {
	t.Helper()
	p := &process{name: name, cmd: exec.Command(bin), done: make(chan struct{})}
	for _, k := range []string{"PATH", "HOME", "LANG", "TZ"} {
		if value := os.Getenv(k); value != "" {
			p.cmd.Env = append(p.cmd.Env, k+"="+value)
		}
	}
	for key, value := range env {
		p.cmd.Env = append(p.cmd.Env, key+"="+value)
	}
	p.cmd.Stdout = io.Discard
	p.cmd.Stderr = io.Discard
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("cannot start %s binary", name)
	}
	go func() { _ = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
			return
		default:
		}
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()
		select {
		case <-p.done:
			return
		case <-timer.C:
		}
		_ = p.cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Errorf("%s did not exit after forced cleanup", name)
		}
	})
	return p
}
func verifiedRuntimeBinary(t *testing.T, dir, name string, checkSDK bool) string {
	t.Helper()
	file := filepath.Join(dir, name)
	info, err := buildinfo.ReadFile(file)
	if err != nil {
		t.Fatalf("build %s before running E2E", name)
	}
	if checkSDK {
		found := false
		for _, dep := range info.Deps {
			if dep.Path == "github.com/xtwo56/mercury" {
				found = dep.Version == sdkVersion && dep.Replace == nil
			}
		}
		if !found {
			t.Fatalf("%s must use Iris's pinned public SDK, without a replacement", name)
		}
	}
	return file
}
func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("cannot allocate loopback API address")
	}
	address := l.Addr().String()
	_ = l.Close()
	return address
}

// Poll observable state until a deadline. Waiting is cadence-controlled, not an
// assumption that a fixed sleep makes asynchronous work complete. Process exits
// fail early; timeout diagnostics never include tokens, payloads or remote errors.
func await(t *testing.T, parent context.Context, stage string, timeout time.Duration, processes []*process, check func(context.Context) (bool, string)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	last := "not checked"
	for {
		for _, p := range processes {
			select {
			case <-p.done:
				t.Fatalf("%s: %s exited prematurely; check documented configuration/build prerequisites", stage, p.name)
			default:
			}
		}
		done, summary := check(ctx)
		last = summary
		if done {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s timed out: %s", stage, last)
		case <-ticker.C:
		}
	}
}
func localClient(t *testing.T) *http.Client {
	t.Helper()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func call(ctx context.Context, c *http.Client, origin, token, method, path, key string, body []byte) (int, []byte, http.Header) {
	r, err := http.NewRequestWithContext(ctx, method, origin+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(r)
	if err != nil {
		return 0, nil, nil
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return resp.StatusCode, nil, resp.Header
	}
	return resp.StatusCode, data, resp.Header
}
func jsonCall(t *testing.T, ctx context.Context, c *http.Client, origin, token, method, path, key string, body []byte, want int, out any) {
	t.Helper()
	status, data, _ := call(ctx, c, origin, token, method, path, key, body)
	if status != want {
		t.Fatalf("%s %s: HTTP %d, expected %d (response withheld)", method, path, status, want)
	}
	if out != nil && json.Unmarshal(data, out) != nil {
		t.Fatalf("%s %s: invalid JSON response", method, path)
	}
}
func encoded(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal("cannot encode synthetic request")
	}
	return data
}
func count(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatal("verification count query failed")
	}
	return n
}
func assertCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, deliveries, runs int) {
	t.Helper()
	var d, r, o int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM deliveries),(SELECT count(*) FROM delivery_runs),(SELECT count(*) FROM outbox)`).Scan(&d, &r, &o); err != nil {
		t.Fatal("Iris counts unavailable")
	}
	if d != deliveries || r != runs || o != runs {
		t.Fatalf("deliveries/runs/outbox=%d/%d/%d; expected %d/%d/%d", d, r, o, deliveries, runs, runs)
	}
}

func databaseURL(t *testing.T, dsn, name string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("E2E admin DSNs must be PostgreSQL URLs")
	}
	u.Path = "/" + name
	u.RawPath = ""
	q := u.Query()
	q.Del("database")
	q.Del("dbname")
	u.RawQuery = q.Encode()
	return u.String()
}
