package main

import (
	"strings"
	"testing"
	"time"
)

func TestReconcilerConfiguration(t *testing.T) {
	base := map[string]string{"IRIS_DATABASE_URL": "postgres://test@localhost/iris", "IRIS_MERCURY_URL": "https://mercury.example", "IRIS_MERCURY_WORKER_TOKEN": "test-worker-token"}
	c, err := loadConfig(func(k string) string { return base[k] })
	if err != nil || c.limits.BatchSize != 100 || c.limits.PollInterval != 5*time.Second || c.requestTimeout != 10*time.Second {
		t.Fatalf("defaults: %v", err)
	}
	for _, tt := range []struct{ key, value string }{
		{"IRIS_DATABASE_URL", ""}, {"IRIS_MERCURY_URL", "http://public.example"}, {"IRIS_MERCURY_URL", "https://user@host"},
		{"IRIS_MERCURY_URL", "https://host/path"}, {"IRIS_MERCURY_URL", "https://host?"}, {"IRIS_MERCURY_URL", "https://host#"},
		{"IRIS_MERCURY_WORKER_TOKEN", ""}, {"IRIS_MERCURY_WORKER_TOKEN", "bad token"},
		{"IRIS_RECONCILER_BATCH_SIZE", "0"}, {"IRIS_RECONCILER_BATCH_SIZE", "1001"}, {"IRIS_RECONCILER_BATCH_SIZE", "bad"},
		{"IRIS_RECONCILER_POLL_INTERVAL", "0s"}, {"IRIS_RECONCILER_OPERATION_TIMEOUT", "1s"}, {"IRIS_MERCURY_REQUEST_TIMEOUT", "30s"},
	} {
		_, err := loadConfig(func(k string) string {
			if k == tt.key {
				return tt.value
			}
			return base[k]
		})
		if err == nil || strings.Contains(err.Error(), "test-worker-token") {
			t.Fatalf("invalid config %s accepted or leaked: %v", tt.key, err)
		}
	}
	c, err = loadConfig(func(k string) string {
		switch k {
		case "IRIS_MERCURY_URL":
			return "http://127.0.0.1:18081"
		case "IRIS_RECONCILER_BATCH_SIZE":
			return "7"
		case "IRIS_RECONCILER_POLL_INTERVAL":
			return "2s"
		default:
			return base[k]
		}
	})
	if err != nil || c.limits.BatchSize != 7 || c.limits.PollInterval != 2*time.Second {
		t.Fatalf("overrides: %v", err)
	}
}
