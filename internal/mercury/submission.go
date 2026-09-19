// Package mercury implements only the verified Mercury job-submission contract.
package mercury

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/application/dispatcher"
)

// The v1 task name versions this two-reference payload and its execution policy.
// Keep these values fixed for pending v1 entries across releases. A future policy
// change requires separately persisted/versioned intent, not a runtime setting.
const TaskType = "webhook.deliver.v1"
const MaxAttempts = 3

// Replays include current terminal results, not just the submission fields.
// Allow room for Mercury's 1 MiB result and JSON escaping while remaining bounded.
const maxResponseBytes = 8 << 20

type payload struct {
	DeliveryID string `json:"delivery_id"`
	RunID      string `json:"run_id"`
}
type request struct {
	TaskType    string  `json:"task_type"`
	Payload     payload `json:"payload"`
	MaxAttempts int     `json:"max_attempts"`
}

// Client owns a bounded HTTP transport. Its credential is a producer/gateway
// bearer credential, not Mercury's remote-worker credential (which does not
// authenticate POST /v1/jobs in the inspected Mercury build).
type Client struct {
	endpoint, credential string
	http                 *http.Client
}

// New requires a configured HTTPS origin and credential. Local cleartext is
// allowed only on loopback for development. Redirects and environment proxies
// are disabled so a submission credential cannot be forwarded elsewhere.
func New(origin, credential string, timeout time.Duration) (*Client, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" || strings.Contains(origin, "#") || u.RawQuery != "" || u.ForceQuery || (u.Path != "" && u.Path != "/") || u.Opaque != "" {
		return nil, dispatcher.ErrConfiguration
	}
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return nil, dispatcher.ErrConfiguration
	}
	if strings.TrimSpace(credential) == "" || strings.ContainsAny(credential, " \t\r\n") || timeout <= 0 {
		return nil, dispatcher.ErrConfiguration
	}
	for _, b := range []byte(credential) {
		if b < 0x21 || b > 0x7e {
			return nil, dispatcher.ErrConfiguration
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxResponseHeaderBytes = 16 << 10
	return &Client{endpoint: strings.TrimRight(origin, "/") + "/v1/jobs", credential: credential, http: &http.Client{Timeout: timeout, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Close releases idle connections; the caller owns the client's lifetime.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// Submit reuses the saved key and fixed request. Omit available_at on every
// invocation: Mercury fingerprints omission separately from an explicit time.
// Neither current time nor dispatcher configuration enters the comparison.
func (c *Client) Submit(ctx context.Context, s dispatcher.Submission) (string, error) {
	if len(s.Key) == 0 || len(s.Key) > 255 || strings.TrimSpace(string(s.RunID)) == "" || strings.TrimSpace(string(s.DeliveryID)) == "" {
		return "", dispatcher.ErrConfiguration
	}
	for _, b := range []byte(s.Key) {
		if b < 0x21 || b > 0x7e {
			return "", dispatcher.ErrConfiguration
		}
	}
	wanted := payload{string(s.DeliveryID), string(s.RunID)}
	body, err := json.Marshal(request{TaskType, wanted, MaxAttempts})
	if err != nil || len(body) > 1<<20 {
		return "", dispatcher.ErrConfiguration
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return "", dispatcher.ErrConfiguration
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", s.Key)
	req.Header.Set("Authorization", "Bearer "+c.credential)
	response, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		var unknown x509.UnknownAuthorityError
		var hostname x509.HostnameError
		var certificate x509.CertificateInvalidError
		if errors.As(err, &unknown) || errors.As(err, &hostname) || errors.As(err, &certificate) {
			return "", dispatcher.ErrConfiguration
		}
		return "", dispatcher.ErrTransient
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK, http.StatusCreated:
	case http.StatusUnauthorized, http.StatusForbidden:
		return "", dispatcher.ErrAuthentication
	case http.StatusConflict:
		return "", dispatcher.ErrConflict
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
		return "", dispatcher.ErrTransient
	default:
		if response.StatusCode >= 500 {
			return "", dispatcher.ErrTransient
		}
		return "", dispatcher.ErrConfiguration
	}
	// A status code alone is not an acknowledgment. Verify job identity and the
	// immutable submission fields; replays may legitimately be in any job state.
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", dispatcher.ErrTransient
	}
	if len(data) > maxResponseBytes {
		return "", dispatcher.ErrAcknowledgment
	}
	var ack struct {
		ID          string          `json:"id"`
		TaskType    string          `json:"task_type"`
		Payload     json.RawMessage `json:"payload"`
		MaxAttempts int             `json:"max_attempts"`
	}
	if json.Unmarshal(data, &ack) != nil || strings.TrimSpace(ack.ID) == "" || ack.TaskType != TaskType || ack.MaxAttempts != MaxAttempts {
		return "", dispatcher.ErrAcknowledgment
	}
	var got payload
	decoder := json.NewDecoder(bytes.NewReader(ack.Payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&got) != nil || got != wanted {
		return "", dispatcher.ErrAcknowledgment
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		return "", dispatcher.ErrAcknowledgment
	}
	return ack.ID, nil
}
