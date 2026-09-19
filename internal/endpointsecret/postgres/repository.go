// Package postgres persists encrypted endpoint secrets, never plaintext.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xTwo56/iris/internal/endpoint"
	"github.com/xTwo56/iris/internal/endpointsecret"
	"strings"
)

var ErrNotFound = errors.New("endpoint signing secret missing")
var ErrDuplicate = errors.New("endpoint signing secret already exists")
var ErrEndpointNotFound = errors.New("secret endpoint missing")

// Queries accepts a caller-owned pool or transaction. The repository never
// completes transactions or opens/closes connections.
type Queries interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
type Repository struct{ db Queries }

func New(db Queries) *Repository { return &Repository{db: db} }

// Create writes ciphertext alongside endpoint creation in the caller's transaction.
func (r *Repository) Create(ctx context.Context, id endpoint.ID, s endpointsecret.Sealed) error {
	if strings.TrimSpace(string(id)) == "" || s.Format != 1 || len(s.Nonce) != 12 || len(s.Ciphertext) != 48 {
		return endpointsecret.ErrInvalid
	}
	_, err := r.db.Exec(ctx, `INSERT INTO endpoint_secrets (endpoint_id,encryption_format,nonce,ciphertext) VALUES ($1,$2,$3,$4)`, string(id), s.Format, s.Nonce, s.Ciphertext)
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			switch {
			case pe.Code == "23505" && pe.ConstraintName == "endpoint_secrets_pkey":
				err = ErrDuplicate
			case pe.Code == "23503" && pe.ConstraintName == "endpoint_secrets_endpoint_fk":
				err = ErrEndpointNotFound
			}
		}
		return fmt.Errorf("create encrypted endpoint secret: %w", err)
	}
	return nil
}
func (r *Repository) GetByEndpointID(ctx context.Context, id endpoint.ID) (endpointsecret.Sealed, error) {
	var s endpointsecret.Sealed
	err := r.db.QueryRow(ctx, `SELECT encryption_format,nonce,ciphertext FROM endpoint_secrets WHERE endpoint_id=$1`, string(id)).Scan(&s.Format, &s.Nonce, &s.Ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	if err != nil {
		return s, fmt.Errorf("get encrypted endpoint secret: %w", err)
	}
	return s, nil
}

// Load decrypts outside HTTP transport. Missing legacy credentials are an explicit
// error; no unsigned fallback or replacement-secret generation occurs.
func (r *Repository) Load(ctx context.Context, id endpoint.ID, c *endpointsecret.Cipher) ([]byte, error) {
	s, err := r.GetByEndpointID(ctx, id)
	if err != nil {
		return nil, err
	}
	return c.Decrypt(id, s)
}
