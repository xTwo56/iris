package postgres_test

import (
	"context"
	"testing"
	"time"

	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
)

// Invalid pagination must fail before issuing queries, including when the
// repository is called without HTTP's validation in a caller-owned transaction.
func TestHistoryPageValidation(t *testing.T) {
	repo := deliverypg.NewHistoryRepository(nil)
	at := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		limit int
		c     deliverypg.HistoryCursor
	}{
		{0, deliverypg.HistoryCursor{}}, {201, deliverypg.HistoryCursor{}},
		{1, deliverypg.HistoryCursor{ID: "id"}}, {1, deliverypg.HistoryCursor{At: at}},
		{1, deliverypg.HistoryCursor{At: at, ID: " "}}, {1, deliverypg.HistoryCursor{At: at.Add(time.Nanosecond), ID: "id"}},
	} {
		if _, err := repo.DeliveryPage(context.Background(), "event", tt.limit, tt.c); err == nil {
			t.Fatal("delivery page accepted invalid input")
		}
		if _, err := repo.RunPage(context.Background(), "delivery", tt.limit, tt.c); err == nil {
			t.Fatal("run page accepted invalid input")
		}
		if _, err := repo.AttemptPage(context.Background(), "run", tt.limit, tt.c); err == nil {
			t.Fatal("attempt page accepted invalid input")
		}
	}
}
