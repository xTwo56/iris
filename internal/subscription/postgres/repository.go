// package postgres stores subscription rules and queries their current eligibility.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xTwo56/iris/internal/endpoint"
	"github.com/xTwo56/iris/internal/event"
	"github.com/xTwo56/iris/internal/subscription"
)

var ErrDuplicateID = errors.New("subscription ID already exists")

var ErrDuplicateEndpointType = errors.New("subscription endpoint/type pair already exists")

var ErrEndpointNotFound = errors.New("subscription endpoint not found")

var ErrNotFound = errors.New("subscription not found")

type Queries interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type Repository struct{ db Queries }

func New(db Queries) *Repository { return &Repository{db: db} }

func (r *Repository) Create(ctx context.Context, s subscription.Subscription) error {
	if _, err := subscription.New(s.ID(), s.EndpointID(), s.EventType(), s.CreatedAt()); err != nil {
		return fmt.Errorf("create subscription: validate: %w", err)
	}
	_, err := r.db.Exec(ctx, `INSERT INTO subscriptions (id, endpoint_id, event_type, created_at, enabled) VALUES ($1,$2,$3,$4,$5)`, string(s.ID()), string(s.EndpointID()), string(s.EventType()), s.CreatedAt(), s.Enabled())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch {
			case pgErr.Code == "23505" && pgErr.ConstraintName == "subscriptions_pkey":
				err = ErrDuplicateID
			case pgErr.Code == "23505" && pgErr.ConstraintName == "subscriptions_endpoint_event_type_key":
				err = ErrDuplicateEndpointType
			case pgErr.Code == "23503" && pgErr.ConstraintName == "subscriptions_endpoint_fk":
				err = ErrEndpointNotFound
			}
		}
		return fmt.Errorf("create subscription %q: %w", s.ID(), err)
	}
	return nil
}

func scanSubscription(row interface{ Scan(...any) error }) (subscription.Subscription, error) {
	var id, endpointID, eventType string
	var at time.Time
	var enabled bool
	if err := row.Scan(&id, &endpointID, &eventType, &at, &enabled); err != nil {
		return subscription.Subscription{}, err
	}
	s, err := subscription.New(subscription.ID(id), endpoint.ID(endpointID), event.Type(eventType), at)
	if err != nil {
		return subscription.Subscription{}, fmt.Errorf("reconstruct: %w", err)
	}
	if !enabled {
		s.Disable()
	}
	return s, nil
}

func (r *Repository) GetByID(ctx context.Context, id subscription.ID) (subscription.Subscription, error) {
	s, err := scanSubscription(r.db.QueryRow(ctx, `SELECT id, endpoint_id, event_type, created_at, enabled FROM subscriptions WHERE id=$1`, string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	if err != nil {
		return subscription.Subscription{}, fmt.Errorf("get subscription %q: %w", id, err)
	}
	return s, nil
}

func (r *Repository) SetEnabled(ctx context.Context, id subscription.ID, enabled bool) error {
	tag, err := r.db.Exec(ctx, `UPDATE subscriptions SET enabled=$2 WHERE id=$1`, string(id), enabled)
	if err != nil {
		return fmt.Errorf("set subscription enabled %q: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("set subscription enabled %q: %w", id, ErrNotFound)
	}
	return nil
}

// lists exact event types with both subscription enabled and
// endpoint fanout true, ordered by subscription ID in bytewise C collation (a rule for comparing and sorting text)
func (r *Repository) ListEligible(ctx context.Context, eventType event.Type) ([]subscription.Subscription, error) {
	rows, err := r.db.Query(ctx, `SELECT s.id, s.endpoint_id, s.event_type, s.created_at, s.enabled
 FROM subscriptions s JOIN endpoints e ON e.id=s.endpoint_id
 WHERE s.event_type=$1 AND s.enabled AND e.fanout ORDER BY s.id COLLATE "C"`, string(eventType))
	if err != nil {
		return nil, fmt.Errorf("list eligible subscriptions for %q: %w", eventType, err)
	}
	defer rows.Close()
	result := make([]subscription.Subscription, 0)
	for rows.Next() {
		s, err := scanSubscription(rows)
		if err != nil {
			return nil, fmt.Errorf("list eligible subscriptions for %q: %w", eventType, err)
		}
		result = append(result, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list eligible subscriptions for %q: iterate: %w", eventType, err)
	}
	return result, nil
}
