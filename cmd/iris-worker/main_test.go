package main

import (
	"strings"
	"testing"
	"time"
)

func workerEnvironment() map[string]string {
	return map[string]string{
		"IRIS_DATABASE_URL": "postgres://private@iris/db", "IRIS_MERCURY_URL": "https://mercury.example", "IRIS_MERCURY_WORKER_TOKEN": "worker-secret", "IRIS_WORKER_ID": "iris-worker-1", "IRIS_SECRET_ENCRYPTION_KEY": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
	}
}
func TestWorkerConfiguration(t *testing.T) {
	env := workerEnvironment()
	c, err := loadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	defer clear(c.key)
	if c.worker.Concurrency != 4 || c.worker.HeartbeatInterval != 20*time.Second || c.worker.UncertainClaimHold < time.Minute {
		t.Fatal("invalid SDK defaults")
	}
	for _, tt := range []struct{ key, value string }{
		{"IRIS_WORKER_ID", ""}, {"IRIS_DATABASE_URL", ""}, {"IRIS_MERCURY_WORKER_TOKEN", ""}, {"IRIS_MERCURY_WORKER_TOKEN", "contains space"},
		{"IRIS_SECRET_ENCRYPTION_KEY", "bad"}, {"IRIS_MERCURY_URL", "http://remote.example"}, {"IRIS_MERCURY_URL", "https://user:secret@remote.example"},
		{"IRIS_WORKER_CONCURRENCY", "0"}, {"IRIS_WORKER_CONCURRENCY", "1001"}, {"IRIS_WORKER_HEARTBEAT_INTERVAL", "40s"}, {"IRIS_WORKER_POLL_INTERVAL", "0s"},
		{"IRIS_WORKER_SHUTDOWN_TIMEOUT", "1s"}, {"IRIS_MERCURY_REQUEST_TIMEOUT", "1m"},
	} {
		t.Run(tt.key+tt.value, func(t *testing.T) {
			env := workerEnvironment()
			env[tt.key] = tt.value
			_, err := loadConfig(func(k string) string { return env[k] })
			if err == nil || strings.Contains(err.Error(), "worker-secret") || strings.Contains(err.Error(), "postgres://") {
				t.Fatal("invalid configuration accepted or secrets exposed")
			}
		})
	}
}
func TestFreshAttemptIdentity(t *testing.T) {
	a, err := newAttemptID()
	if err != nil {
		t.Fatal(err)
	}
	b, err := newAttemptID()
	if err != nil {
		t.Fatal(err)
	}
	if a == b || !strings.HasPrefix(string(a), "att_") {
		t.Fatal("attempt identity was reused")
	}
}
