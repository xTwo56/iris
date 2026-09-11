// package postgres persists events in iris's postgres db
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xTwo56/iris/internal/event"
)

// event with the supplied id already exists
var ErrDuplicateID = errors.New("event ID already exists")

var ErrNotFound = errors.New("event not found")

// both *pgxpool.Pool and pgx.Tx implement it
type Queries interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// provides insert and read operations only
type Repository struct{ db Queries }

// binds a repository to a caller-supplied pool or tx
func New(db Queries) *Repository { return &Repository{db: db} }

func (r *Repository) Create(ctx context.Context, e event.Event) error {
	payload := e.Payload()
	if _, err := event.New(e.ID(), e.Type(), e.CreatedAt(), payload); err != nil {
		return fmt.Errorf("create event: validate: %w", err)
	}
	_, err := r.db.Exec(ctx, `INSERT INTO events (id, event_type, created_at, payload) VALUES ($1, $2, $3, $4)`, string(e.ID()), string(e.Type()), e.CreatedAt(), payload)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "events_pkey" {
			return fmt.Errorf("create event %q: %w", e.ID(), ErrDuplicateID)
		}
		return fmt.Errorf("create event %q: %w", e.ID(), err)
	}
	return nil
}

func (r *Repository) GetByID(ctx context.Context, id event.ID) (event.Event, error) {
	var storedID, eventType string
	var createdAt time.Time
	var payload []byte
	err := r.db.QueryRow(ctx, `SELECT id, event_type, created_at, payload FROM events WHERE id = $1`, string(id)).Scan(&storedID, &eventType, &createdAt, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return event.Event{}, fmt.Errorf("get event %q: %w", id, ErrNotFound)
	}
	if err != nil {
		return event.Event{}, fmt.Errorf("get event %q: %w", id, err)
	}
	e, err := event.New(event.ID(storedID), event.Type(eventType), createdAt, payload)
	if err != nil {
		return event.Event{}, fmt.Errorf("get event %q: reconstruct: %w", id, err)
	}
	return e, nil
}
