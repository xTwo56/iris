// Package endpointcreation atomically creates endpoints and their signing secrets.
package endpointcreation

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/xTwo56/iris/internal/endpoint"
	endpointpg "github.com/xTwo56/iris/internal/endpoint/postgres"
	"github.com/xTwo56/iris/internal/endpointsecret"
	secretpg "github.com/xTwo56/iris/internal/endpointsecret/postgres"
	"time"
)

type Beginner interface {
	Begin(context.Context) (pgx.Tx, error)
}
type Service struct {
	db     Beginner
	cipher *endpointsecret.Cipher
}

func New(db Beginner, c *endpointsecret.Cipher) *Service { return &Service{db: db, cipher: c} }

// Create saves both records together and releases the plaintext only after
// commit. Only the authenticated creation response should expose this result;
// losing it requires a replacement endpoint, not secret retrieval or rotation.
func (s *Service) Create(ctx context.Context, e endpoint.Endpoint) (secret string, err error) {
	if _, err := endpoint.New(e.ID(), e.URL(), e.CreatedAt()); err != nil {
		return "", fmt.Errorf("create endpoint: validate: %w", err)
	}
	plain := make([]byte, 32)
	if _, err := rand.Read(plain); err != nil {
		return "", errors.New("generate endpoint secret failed")
	}
	defer clear(plain)
	sealed, err := s.cipher.Encrypt(e.ID(), plain)
	if err != nil {
		return "", err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("create endpoint: begin: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := tx.Rollback(cleanup); e != nil && !errors.Is(e, pgx.ErrTxClosed) {
			secret = ""
			err = errors.Join(err, fmt.Errorf("create endpoint: rollback: %w", e))
		}
	}()
	if err := endpointpg.New(tx).Create(ctx, e); err != nil {
		return "", err
	}
	if err := secretpg.New(tx).Create(ctx, e.ID(), sealed); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("create endpoint: commit: %w", err)
	}
	return base64.StdEncoding.EncodeToString(plain), nil
}
