package delivery

import (
	"errors"
	"strings"
	"time"
)

type RunID string

// records why a delivery run was created.
type Trigger string

const (
	TriggerInitial          Trigger = "initial"
	TriggerManualRedelivery Trigger = "manual_redelivery"
)

/*
* one execution cycle for a delivery, backed by one mercury job
* groups the automatic retries of a delivery
* manual redelivery creates a new run while preserving the delivery id
* each run corresponds to one mercury job
 */
type Run struct {
	id         RunID
	deliveryID ID
	createdAt  time.Time
	trigger    Trigger
}

func NewRun(id RunID, deliveryID ID, createdAt time.Time, trigger Trigger) (Run, error) {
	if strings.TrimSpace(string(id)) == "" {
		return Run{}, errors.New("run ID must not be blank")
	}
	if strings.TrimSpace(string(deliveryID)) == "" {
		return Run{}, errors.New("run delivery ID must not be blank")
	}
	if createdAt.IsZero() {
		return Run{}, errors.New("run creation timestamp must not be zero")
	}
	if trigger != TriggerInitial && trigger != TriggerManualRedelivery {
		return Run{}, errors.New("run trigger must be initial or manual_redelivery")
	}
	return Run{
		id:         id,
		deliveryID: deliveryID,
		createdAt:  createdAt,
		trigger:    trigger,
	}, nil
}

func (r Run) ID() RunID { return r.id }

func (r Run) DeliveryID() ID { return r.deliveryID }

func (r Run) CreatedAt() time.Time { return r.createdAt }

func (r Run) Trigger() Trigger { return r.trigger }
