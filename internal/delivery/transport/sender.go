// Package transport sends bounded HTTPS webhooks without persistence or retries.
package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/event"
	"github.com/xTwo56/iris/internal/webhook/signing"
)

const (
	// EventIDHeader carries the event identity unchanged across retries and redelivery.
	EventIDHeader = "X-Iris-Event-ID"
	// DeliveryIDHeader carries the event/destination obligation identity.
	DeliveryIDHeader = "X-Iris-Delivery-ID"
)

// Safe error values deliberately exclude destination URLs, DNS answers and bodies.
var (
	ErrInvalidDestination = errors.New("invalid webhook destination")
	ErrBlockedDestination = errors.New("webhook destination is not public")
	ErrInvalidIdentity    = errors.New("invalid webhook header identity")
	ErrTLSVerification    = errors.New("webhook TLS certificate verification failed")
	ErrNetwork            = errors.New("webhook network failure")
	ErrTimeout            = errors.New("webhook delivery timeout")
)

// Config bounds the entire send and optional response draining. Zero body limit
// skips draining. Timeout must be positive; body limits must be between 0 and 1 MiB.
type Config struct {
	Timeout          time.Duration
	MaxResponseBytes int64
}

// Observation records only safe metadata. HTTPStatus is zero only when no final
// response was received. Err distinguishes caller cancellation/deadline from our
// ErrTimeout. BodyReadIncomplete describes optional draining, not delivery failure.
type Observation struct {
	Classification     delivery.Classification
	HTTPStatus         int
	Err                error
	BodyReadIncomplete bool
}

type resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// Sender is safe for concurrent use. Production construction exposes no resolver,
// dialer, proxy or TLS bypass options; controlled dependencies are private test seams.
type Sender struct {
	config   Config
	resolver resolver
	dial     func(context.Context, string, string) (net.Conn, error)
	roots    *x509.CertPool
}

// New uses system DNS, direct TCP connections and system certificate trust.
func New(config Config) (*Sender, error) {
	if config.Timeout <= 0 || config.MaxResponseBytes < 0 || config.MaxResponseBytes > 1<<20 {
		return nil, errors.New("invalid webhook transport bounds")
	}
	return &Sender{config: config, resolver: net.DefaultResolver, dial: (&net.Dialer{}).DialContext}, nil
}

// Send validates a destination, sends exact bytes once, then discards a bounded
// amount of response data. One deadline spans DNS, TCP, TLS, headers and body.
// No redirects, environment proxies, application retries or signing are involved.
func (s *Sender) Send(caller context.Context, destination string, payload []byte, eventID event.ID, deliveryID delivery.ID) Observation {
	return s.send(caller, destination, payload, eventID, deliveryID, nil)
}
func (s *Sender) send(caller context.Context, destination string, payload []byte, eventID event.ID, deliveryID delivery.ID, signed *signing.Headers) Observation {
	if caller.Err() != nil {
		return failed(caller.Err(), false)
	}
	u, err := validateURL(destination)
	if err != nil {
		return failed(err, true)
	}
	if signed == nil && (!validHeaderID(string(eventID)) || !validHeaderID(string(deliveryID))) {
		return failed(ErrInvalidIdentity, true)
	}
	ctx, cancel := context.WithTimeout(caller, s.config.Timeout)
	defer cancel()
	// Use a fresh connection for each send. This prevents transport-level replay
	// on reused connections and forces each request through address validation.
	tr := &http.Transport{Proxy: nil, DisableKeepAlives: true, DisableCompression: true, MaxResponseHeaderBytes: 32 << 10,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: s.roots},
		TLSNextProto:    map[string]func(string, *tls.Conn) http.RoundTripper{},
		DialContext: func(_ context.Context, network, address string) (net.Conn, error) {
			return s.dialPublic(ctx, network, address)
		}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	// Keep the original URL authority. Only the TCP dial target is replaced;
	// net/http therefore verifies the original TLS hostname and sends its Host.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return failed(ErrInvalidDestination, true)
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(EventIDHeader, string(eventID))
	req.Header.Set(DeliveryIDHeader, string(deliveryID))
	if signed != nil {
		req.Header.Set(EventIDHeader, signed.EventID)
		req.Header.Set(DeliveryIDHeader, signed.DeliveryID)
		req.Header.Set(signing.TimestampHeader, signed.Timestamp)
		req.Header.Set(signing.SignatureHeader, signed.Signature)
	}
	response, err := client.Do(req)
	if err != nil {
		if response != nil {
			response.Body.Close()
		}
		if caller.Err() != nil {
			return failed(caller.Err(), false)
		}
		if ctx.Err() != nil {
			return failed(ErrTimeout, false)
		}
		if errors.Is(err, ErrBlockedDestination) {
			return failed(ErrBlockedDestination, true)
		}
		var verification *tls.CertificateVerificationError
		var unknown x509.UnknownAuthorityError
		var hostname x509.HostnameError
		var invalid x509.CertificateInvalidError
		if errors.As(err, &verification) || errors.As(err, &unknown) || errors.As(err, &hostname) || errors.As(err, &invalid) {
			return failed(ErrTLSVerification, true)
		}
		return failed(ErrNetwork, false)
	}
	defer response.Body.Close()
	observation := Observation{HTTPStatus: response.StatusCode, Classification: classifyStatus(response.StatusCode)}
	// Headers already establish the HTTP outcome. Truncation, body errors or a
	// body deadline cannot turn an observed 2xx into a failure. No body is retained.
	n, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, s.config.MaxResponseBytes))
	observation.BodyReadIncomplete = readErr != nil || (n == s.config.MaxResponseBytes && (response.ContentLength < 0 || response.ContentLength > n))
	if caller.Err() != nil {
		observation.Err = caller.Err()
	} else if ctx.Err() != nil {
		observation.Err = ErrTimeout
	}
	return observation
}
func failed(err error, permanent bool) Observation {
	c := delivery.ClassificationRetryableFailure
	if permanent {
		c = delivery.ClassificationPermanentFailure
	}
	return Observation{Classification: c, Err: err}
}
func classifyStatus(status int) delivery.Classification {
	if status >= 200 && status <= 299 {
		return delivery.ClassificationSucceeded
	}
	if status == 408 || status == 425 || status == 429 || (status >= 500 && status <= 599) {
		return delivery.ClassificationRetryableFailure
	}
	return delivery.ClassificationPermanentFailure
}
func validHeaderID(id string) bool {
	if strings.TrimSpace(id) == "" {
		return false
	}
	for _, c := range []byte(id) {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}
func validateURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.Opaque != "" || u.User != nil || strings.Contains(raw, "#") || strings.Contains(u.Hostname(), "%") {
		return nil, ErrInvalidDestination
	}
	if u.Port() != "" {
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 {
			return nil, ErrInvalidDestination
		}
	}
	if strings.HasSuffix(u.Host, ":") {
		return nil, ErrInvalidDestination
	}
	return u, nil
}

// dialPublic closes the DNS rebinding gap. Validate every resolved address before
// dialing any, reject mixed answers, and pass only a numeric IP to the dialer.
// The transport never performs a second hostname lookup. Address fallback is
// connection establishment, not another HTTP delivery attempt.
func (s *Sender) dialPublic(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, ErrBlockedDestination
	}
	var addresses []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		addresses = []netip.Addr{ip}
	} else {
		addresses, err = s.resolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
	}
	if len(addresses) == 0 {
		return nil, ErrNetwork
	}
	for _, ip := range addresses {
		if !publicIP(ip) {
			return nil, ErrBlockedDestination
		}
	}
	for _, ip := range addresses {
		conn, err := s.dial(ctx, network, net.JoinHostPort(ip.Unmap().String(), port))
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, ErrNetwork
}
