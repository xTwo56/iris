// package endpoint defines webhook destinations and their fan-out eligibility.
package endpoint

import (
	"errors"
	"net/url"
	"strings"
	"time"
)

// ID identifies a webhook destination independently of the events sent to it.
type ID string

// Endpoint has a fixed identity, destination and creation time. Only its enabled
// state can change. Use New to construct an endpoint; the zero value is invalid.
// Disabling prevents future fan-out, while existing deliveries remain eligible.
type Endpoint struct {
	id        ID
	url       string
	createdAt time.Time
	fanout    bool // fanout refers to the procedure when one operation branches out to multiple downstream opns or recepients
}

// no SSRF protection (server is vulnerable to requests to destination accessible to the server's address) cuz of paramterised url
func New(id ID, destinationURL string, createdAt time.Time) (Endpoint, error) {
	if strings.TrimSpace(string(id)) == "" {
		return Endpoint{}, errors.New("endpoint ID must not be blank")
	}
	if createdAt.IsZero() {
		return Endpoint{}, errors.New("endpoint creation timestamp must not be zero")
	}
	parsed, err := url.Parse(destinationURL)
	if err != nil {
		return Endpoint{}, errors.New("endpoint URL must be a valid absolute HTTPS URL")
	}
	if !parsed.IsAbs() || parsed.Scheme != "https" || parsed.Hostname() == "" {
		return Endpoint{}, errors.New("endpoint URL must use absolute HTTPS with a hostname")
	}
	if parsed.User != nil {
		return Endpoint{}, errors.New("endpoint URL must not contain credentials")
	}

	// need no fragments
	if strings.Contains(destinationURL, "#") {
		return Endpoint{}, errors.New("endpoint URL must not contain a fragment")
	}

	return Endpoint{id: id, url: destinationURL, createdAt: createdAt, fanout: true}, nil
}

func (e Endpoint) ID() ID { return e.id }

func (e Endpoint) URL() string { return e.url }

func (e Endpoint) CreatedAt() time.Time { return e.createdAt }

func (e Endpoint) Fanout() bool { return e.fanout }

func (e *Endpoint) FanoutEnable() { e.fanout = true }

func (e *Endpoint) FanoutDisable() { e.fanout = false }
