// Package postgres persists endpoint metadata and future fan-out eligibility.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xTwo56/iris/internal/endpoint"
)

// ErrDuplicateID indicates an endpoint already has the supplied identity.
var ErrDuplicateID = errors.New("endpoint ID already exists")

// ErrNotFound indicates no endpoint has the requested identity.
var ErrNotFound = errors.New("endpoint not found")

// Queries accepts either a caller-owned *pgxpool.Pool or pgx.Tx using only the
// operations needed here. The repository never commits or rolls back a transaction.
type Queries interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Repository inserts and reads endpoints and changes only their fan-out flag.
// Connection configuration, closure and transaction completion belong to callers.
type Repository struct{ db Queries }

// New binds a pool or transaction without opening connections or running migrations.
func New(db Queries) *Repository { return &Repository{db: db} }

// Create validates immutable metadata before a parameterized INSERT, rejecting
// zero values before querying. It preserves the supplied fan-out state and exact
// URL spelling. Only duplicate primary keys map to ErrDuplicateID; URLs may repeat.
func (r *Repository) Create(ctx context.Context, e endpoint.Endpoint) error {
	if _, err := endpoint.New(e.ID(), e.URL(), e.CreatedAt()); err != nil {
		return fmt.Errorf("create endpoint: validate: %w", err)
	}
	_, err := r.db.Exec(ctx, `INSERT INTO endpoints (id, url, created_at, fanout) VALUES ($1, $2, $3, $4)`, string(e.ID()), e.URL(), e.CreatedAt(), e.Fanout())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "endpoints_pkey" {
			return fmt.Errorf("create endpoint %q: %w", e.ID(), ErrDuplicateID)
		}
		return fmt.Errorf("create endpoint %q: %w", e.ID(), err)
	}
	return nil
}

// GetByID validates stored metadata through the domain constructor, then restores
// the persisted fan-out flag (the constructor starts enabled). Missing rows map
// to ErrNotFound; query and reconstruction errors retain operation context.
func (r *Repository) GetByID(ctx context.Context, id endpoint.ID) (endpoint.Endpoint, error) {
	var storedID, url string
	var createdAt time.Time
	var fanout bool
	err := r.db.QueryRow(ctx, `SELECT id, url, created_at, fanout FROM endpoints WHERE id = $1`, string(id)).Scan(&storedID, &url, &createdAt, &fanout)
	if errors.Is(err, pgx.ErrNoRows) {
		return endpoint.Endpoint{}, fmt.Errorf("get endpoint %q: %w", id, ErrNotFound)
	}
	if err != nil {
		return endpoint.Endpoint{}, fmt.Errorf("get endpoint %q: %w", id, err)
	}
	e, err := endpoint.New(endpoint.ID(storedID), url, createdAt)
	if err != nil {
		return endpoint.Endpoint{}, fmt.Errorf("get endpoint %q: reconstruct: %w", id, err)
	}
	if !fanout {
		e.FanoutDisable()
	}
	return e, nil
}

// SetFanout controls future delivery creation without canceling existing
// deliveries. Only the flag is updated; identity, URL and creation time remain
// unchanged. PostgreSQL counts matched rows even when the value is unchanged,
// so repeated settings succeed while absent IDs return ErrNotFound.
func (r *Repository) SetFanout(ctx context.Context, id endpoint.ID, enabled bool) error {
	tag, err := r.db.Exec(ctx, `UPDATE endpoints SET fanout = $2 WHERE id = $1`, string(id), enabled)
	if err != nil {
		return fmt.Errorf("set endpoint fanout %q: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("set endpoint fanout %q: %w", id, ErrNotFound)
	}
	return nil
}
