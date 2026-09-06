// package delivery defines immutable obligations to send specific events to endpoints.
package delivery

import (
	"errors"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/endpoint"
	"github.com/xTwo56/iris/internal/event"
)

type ID string

/*
subscription defines what event type an endpoint wants
delivery records one particular event that should be sent to that endpoint
*/
type Delivery struct {
	id         ID
	eventID    event.ID
	endpointID endpoint.ID
	createdAt  time.Time
}

func New(id ID, eventID event.ID, endpointID endpoint.ID, createdAt time.Time) (Delivery, error) {
	if strings.TrimSpace(string(id)) == "" {
		return Delivery{}, errors.New("delivery ID must not be blank")
	}
	if strings.TrimSpace(string(eventID)) == "" {
		return Delivery{}, errors.New("delivery event ID must not be blank")
	}
	if strings.TrimSpace(string(endpointID)) == "" {
		return Delivery{}, errors.New("delivery endpoint ID must not be blank")
	}
	if createdAt.IsZero() {
		return Delivery{}, errors.New("delivery creation timestamp must not be zero")
	}
	return Delivery{
		id:         id,
		eventID:    eventID,
		endpointID: endpointID,
		createdAt:  createdAt,
	}, nil
}

func (d Delivery) ID() ID { return d.id }

func (d Delivery) EventID() event.ID { return d.eventID }

func (d Delivery) EndpointID() endpoint.ID { return d.endpointID }

func (d Delivery) CreatedAt() time.Time { return d.createdAt }
