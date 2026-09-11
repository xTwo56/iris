// package postgres stores event-to-endpoint delivery obligations.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/endpoint"
	"github.com/xTwo56/iris/internal/event"
)

var ErrDuplicateID = errors.New("delivery ID already exists")

var ErrDuplicateEventEndpoint = errors.New("delivery event/endpoint pair already exists")

var ErrEventNotFound = errors.New("delivery event not found")

var ErrEndpointNotFound = errors.New("delivery endpoint not found")

var ErrNotFound = errors.New("delivery not found")

type Queries interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Repository struct{ db Queries }

func New(db Queries) *Repository { return &Repository{db: db} }

func (r *Repository) Create(ctx context.Context, d delivery.Delivery) error {
	if _, err := delivery.New(d.ID(), d.EventID(), d.EndpointID(), d.CreatedAt()); err != nil {
		return fmt.Errorf("create delivery: validate: %w", err)
	}
	_, err := r.db.Exec(ctx, `INSERT INTO deliveries (id,event_id,endpoint_id,created_at) VALUES ($1,$2,$3,$4)`, string(d.ID()), string(d.EventID()), string(d.EndpointID()), d.CreatedAt())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch {
			case pgErr.Code == "23505" && pgErr.ConstraintName == "deliveries_pkey":
				err = ErrDuplicateID
			case pgErr.Code == "23505" && pgErr.ConstraintName == "deliveries_event_endpoint_key":
				err = ErrDuplicateEventEndpoint
			case pgErr.Code == "23503" && pgErr.ConstraintName == "deliveries_event_fk":
				err = ErrEventNotFound
			case pgErr.Code == "23503" && pgErr.ConstraintName == "deliveries_endpoint_fk":
				err = ErrEndpointNotFound
			}
		}
		return fmt.Errorf("create delivery %q: %w", d.ID(), err)
	}
	return nil
}

func scanDelivery(row pgx.Row) (delivery.Delivery, error) {
	var id, eventID, endpointID string
	var at time.Time
	err := row.Scan(&id, &eventID, &endpointID, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return delivery.Delivery{}, ErrNotFound
	}
	if err != nil {
		return delivery.Delivery{}, err
	}
	d, err := delivery.New(delivery.ID(id), event.ID(eventID), endpoint.ID(endpointID), at)
	if err != nil {
		return delivery.Delivery{}, fmt.Errorf("reconstruct: %w", err)
	}
	return d, nil
}

func (r *Repository) GetByID(ctx context.Context, id delivery.ID) (delivery.Delivery, error) {
	d, err := scanDelivery(r.db.QueryRow(ctx, `SELECT id,event_id,endpoint_id,created_at FROM deliveries WHERE id=$1`, string(id)))
	if err != nil {
		return delivery.Delivery{}, fmt.Errorf("get delivery %q: %w", id, err)
	}
	return d, nil
}

func (r *Repository) GetByEventEndpoint(ctx context.Context, eventID event.ID, endpointID endpoint.ID) (delivery.Delivery, error) {
	d, err := scanDelivery(r.db.QueryRow(ctx, `SELECT id,event_id,endpoint_id,created_at FROM deliveries WHERE event_id=$1 AND endpoint_id=$2`, string(eventID), string(endpointID)))
	if err != nil {
		return delivery.Delivery{}, fmt.Errorf("get delivery for event %q endpoint %q: %w", eventID, endpointID, err)
	}
	return d, nil
}
