package e2e_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The receiver is deliberately test-only. A user-owned HTTPS ingress forwards
// to this loopback listener; the production worker still resolves public IPs and
// verifies the ingress certificate. No resolver/dialer/TLS override is installed.
type receiver struct {
	mu                   sync.Mutex
	secret, body         []byte
	event, delivery      string
	good, bad, temporary int
	reason               string
}

func (r *receiver) configure(secret, body []byte, event, delivery string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.secret = append([]byte(nil), secret...)
	r.body = append([]byte(nil), body...)
	r.event = event
	r.delivery = delivery
}
func (r *receiver) totals() (int, int, int, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.good, r.bad, r.temporary, r.reason
}
func receiverServer(t *testing.T, origin, listen string) (*receiver, string, string) {
	t.Helper()
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(origin, "#") || (u.Path != "" && u.Path != "/") {
		t.Fatal("IRIS_E2E_RECEIVER_ORIGIN must be a dedicated HTTPS origin")
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil || host != "127.0.0.1" {
		t.Fatal("IRIS_E2E_RECEIVER_LISTEN must bind 127.0.0.1 only")
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		t.Fatal("receiver loopback port unavailable; stop any previous receiver")
	}
	path := "/iris-e2e/" + randomHex(t, 16)
	challenge := randomHex(t, 16)
	receiver := &receiver{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+path+"/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, challenge)
	})
	mux.HandleFunc("POST "+path, receiver.serve)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 16 << 10}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		_ = server.Close()
		receiver.mu.Lock()
		clear(receiver.secret)
		receiver.mu.Unlock()
	})
	return receiver, strings.TrimRight(origin, "/") + path, challenge
}

// Verify independently of Iris's signing implementation. The wire format is
// prefix + four uint64-BE-length-prefixed fields. Raw IDs are decoded from the
// unpadded base64url headers; the exact received bytes are the final field.
func (r *receiver) serve(w http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()
	body, err := io.ReadAll(http.MaxBytesReader(w, request.Body, 64<<10))
	r.mu.Lock()
	defer r.mu.Unlock()
	reject := func(reason string) { r.bad++; r.reason = reason; w.WriteHeader(http.StatusUnauthorized) }
	if err != nil {
		reject("body limit/read failure")
		return
	}
	if len(r.secret) != 32 {
		reject("receiver credentials not configured")
		return
	}
	for _, key := range []string{"X-Iris-Timestamp", "X-Iris-Signature", "X-Iris-Event-ID", "X-Iris-Delivery-ID"} {
		if len(request.Header.Values(key)) != 1 {
			reject("missing/duplicate signing header")
			return
		}
	}
	stamp := request.Header.Get("X-Iris-Timestamp")
	seconds, err := strconv.ParseInt(stamp, 10, 64)
	now := time.Now()
	if err != nil || seconds < 0 || strconv.FormatInt(seconds, 10) != stamp || time.Unix(seconds, 0).Before(now.Add(-5*time.Minute)) || time.Unix(seconds, 0).After(now.Add(5*time.Minute)) {
		reject("timestamp outside tolerance")
		return
	}
	eventID, err := base64.RawURLEncoding.Strict().DecodeString(request.Header.Get("X-Iris-Event-ID"))
	if err != nil {
		reject("event header encoding")
		return
	}
	deliveryID, err := base64.RawURLEncoding.Strict().DecodeString(request.Header.Get("X-Iris-Delivery-ID"))
	if err != nil {
		reject("delivery header encoding")
		return
	}
	signature := request.Header.Get("X-Iris-Signature")
	if !strings.HasPrefix(signature, "v1=") {
		reject("signature version")
		return
	}
	supplied, err := hex.DecodeString(strings.TrimPrefix(signature, "v1="))
	if err != nil || len(supplied) != 32 || signature != "v1="+hex.EncodeToString(supplied) {
		reject("signature encoding")
		return
	}
	mac := hmac.New(sha256.New, r.secret)
	_, _ = mac.Write([]byte("iris-webhook-v1"))
	for _, field := range [][]byte{[]byte(stamp), eventID, deliveryID, body} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = mac.Write(length[:])
		_, _ = mac.Write(field)
	}
	if !hmac.Equal(supplied, mac.Sum(nil)) {
		reject("HMAC mismatch")
		return
	}
	if string(eventID) != r.event || string(deliveryID) != r.delivery || !bytes.Equal(body, r.body) || request.Header.Get("Content-Type") != "application/json" {
		reject("payload or stable identity changed")
		return
	}
	r.good++
	// Fault injection changes only the receiver's response. Mercury's real retry
	// scheduler and SDK must execute again; additional valid requests are allowed.
	if r.temporary == 0 {
		r.temporary++
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
