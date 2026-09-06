/*
package event defines immutable webhook events independent of how they are
stored or delivered
*/
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type ID string

type Type string

type Event struct {
	id        ID
	eventType Type
	createdAt time.Time
	payload   []byte
}

// caller assigned ids for domain sepration and tests
func New(id ID, eventType Type, createdAt time.Time, payload []byte) (Event, error) {
	if strings.TrimSpace(string(id)) == "" {
		return Event{}, errors.New("event ID must not be blank")
	}
	if strings.TrimSpace(string(eventType)) == "" {
		return Event{}, errors.New("event type must not be blank")
	}
	if createdAt.IsZero() {
		return Event{}, errors.New("event creation timestamp must not be zero")
	}
	if !json.Valid(payload) {
		return Event{}, errors.New("event payload must be valid JSON")
	}

	return Event{
		id:        id,
		eventType: eventType,
		createdAt: createdAt,
		payload:   bytes.Clone(payload),
	}, nil
}

func (e Event) ID() ID { return e.id }

func (e Event) Type() Type { return e.eventType }

func (e Event) CreatedAt() time.Time { return e.createdAt }

// without cloning e.payload would return the same backing array
func (e Event) Payload() []byte { return bytes.Clone(e.payload) }
