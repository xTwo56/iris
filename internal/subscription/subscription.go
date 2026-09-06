// package subscription defines exact event-type routing rules for endpoints.
package subscription

import (
	"errors"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/endpoint"
	"github.com/xTwo56/iris/internal/event"
)

type ID string

// relates one event type to one endpoint
type Subscription struct {
	id         ID
	endpointID endpoint.ID
	eventType  event.Type
	createdAt  time.Time
	enabled    bool
}

func New(id ID, endpointID endpoint.ID, eventType event.Type, createdAt time.Time) (Subscription, error) {
	if strings.TrimSpace(string(id)) == "" {
		return Subscription{}, errors.New("subscription ID must not be blank")
	}
	if strings.TrimSpace(string(endpointID)) == "" {
		return Subscription{}, errors.New("subscription endpoint ID must not be blank")
	}
	if strings.TrimSpace(string(eventType)) == "" {
		return Subscription{}, errors.New("subscription event type must not be blank")
	}
	if createdAt.IsZero() {
		return Subscription{}, errors.New("subscription creation timestamp must not be zero")
	}
	return Subscription{
		id: id, endpointID: endpointID, eventType: eventType,
		createdAt: createdAt, enabled: true,
	}, nil
}

func (s Subscription) ID() ID { return s.id }

func (s Subscription) EndpointID() endpoint.ID { return s.endpointID }

func (s Subscription) EventType() event.Type { return s.eventType }

func (s Subscription) CreatedAt() time.Time { return s.createdAt }

func (s Subscription) Enabled() bool { return s.enabled }

func (s *Subscription) Enable() { s.enabled = true }

func (s *Subscription) Disable() { s.enabled = false }

func (s Subscription) Matches(eventType event.Type) bool {
	return s.enabled && s.eventType == eventType
}
