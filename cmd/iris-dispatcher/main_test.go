package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/application/dispatcher"
	outboxpg "github.com/xTwo56/iris/internal/outbox/postgres"
)

func TestDispatcherConfiguration(t *testing.T) {
	base := map[string]string{"IRIS_DATABASE_URL": "postgres://secret@db/iris", "IRIS_MERCURY_URL": "https://mercury.example", "IRIS_MERCURY_SUBMISSION_TOKEN": "producer-secret"}
	c, err := loadConfig(func(k string) string { return base[k] })
	if err != nil || c.limits.BatchSize != 100 || c.requestTimeout != 10*time.Second {
		t.Fatalf("invalid defaults: %v", err)
	}
	for _, key := range []string{"IRIS_DATABASE_URL", "IRIS_MERCURY_URL", "IRIS_MERCURY_SUBMISSION_TOKEN", "IRIS_DISPATCHER_BATCH_SIZE", "IRIS_DISPATCHER_POLL_INTERVAL", "IRIS_MERCURY_REQUEST_TIMEOUT"} {
		t.Run(key, func(t *testing.T) {
			_, err := loadConfig(func(k string) string {
				if k == key {
					if strings.Contains(k, "TIMEOUT") || strings.Contains(k, "INTERVAL") || strings.Contains(k, "SIZE") {
						return "0"
					}
					return ""
				}
				return base[k]
			})
			if err == nil || strings.Contains(err.Error(), "producer-secret") {
				t.Fatal("invalid configuration accepted or credential leaked")
			}
		})
	}
}
func TestOperationalErrorCategories(t *testing.T) {
	categories := map[string]bool{}
	for _, err := range []error{dispatcher.ErrAuthentication, dispatcher.ErrConfiguration, dispatcher.ErrConflict, dispatcher.ErrTransient, dispatcher.ErrAcknowledgment, outboxpg.ErrSubmissionConflict, errors.New("secret raw database detail")} {
		category := errorClass(err)
		if categories[category] || strings.Contains(category, "secret") {
			t.Fatalf("unsafe or indistinct category: %s", category)
		}
		categories[category] = true
	}
}
