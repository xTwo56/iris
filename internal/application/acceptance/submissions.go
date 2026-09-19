package acceptance

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xTwo56/iris/internal/event"
)

// ErrSubmissionConflict means a key was already committed for different input.
var ErrSubmissionConflict = errors.New("submission key input conflicts")

// encodeRequest compares caller input exactly without ambiguous concatenation.
// Length prefixes preserve field boundaries; UTC removes timezone representation
// differences while retaining nanoseconds that PostgreSQL timestamptz would lose.
// A version byte fixes this contract independently of future encoding changes.
func encodeRequest(e event.Event) []byte {
	var data bytes.Buffer
	data.WriteByte(1)
	for _, field := range [][]byte{[]byte(e.ID()), []byte(e.Type()), []byte(e.CreatedAt().UTC().Format(time.RFC3339Nano)), e.Payload()} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		data.Write(size[:])
		data.Write(field)
	}
	return data.Bytes()
}

// acquireSubmission makes one transaction responsible for a new key. The unique
// index waits for competing inserts; ON CONFLICT avoids aborting this transaction.
// At READ COMMITTED the following statement sees the winner's committed result.
// If the competitor rolls back, our INSERT succeeds and acceptance proceeds.
func acquireSubmission(ctx context.Context, tx pgx.Tx, key string, e event.Event) (*Result, error) {
	request := encodeRequest(e)
	tag, err := tx.Exec(ctx, `INSERT INTO event_submissions (submission_key,request_data) VALUES ($1,$2) ON CONFLICT (submission_key) DO NOTHING`, key, request)
	if err != nil {
		return nil, fmt.Errorf("acquire submission: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	var stored []byte
	var id *string
	var count *int
	if err := tx.QueryRow(ctx, `SELECT request_data,event_id,delivery_count FROM event_submissions WHERE submission_key=$1`, key).Scan(&stored, &id, &count); err != nil {
		return nil, fmt.Errorf("read submission: %w", err)
	}
	if !bytes.Equal(request, stored) {
		return nil, ErrSubmissionConflict
	}
	if id == nil || count == nil {
		return nil, errors.New("read submission: committed result is incomplete")
	}
	return &Result{EventID: event.ID(*id), DeliveryCount: *count}, nil
}

// completeSubmission saves the original answer alongside all acceptance writes.
// Only the owning transaction can complete its uncommitted placeholder; any
// failure rolls back the key as well as the event and delivery work.
func completeSubmission(ctx context.Context, tx pgx.Tx, key string, result Result) error {
	tag, err := tx.Exec(ctx, `UPDATE event_submissions SET event_id=$2,delivery_count=$3 WHERE submission_key=$1 AND event_id IS NULL`, key, string(result.EventID), result.DeliveryCount)
	if err != nil {
		return fmt.Errorf("complete submission: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("complete submission: pending key missing")
	}
	return nil
}
