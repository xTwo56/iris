package transport

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
)

type resolveFunc func(context.Context, string, string) ([]netip.Addr, error)

func (f resolveFunc) LookupNetIP(c context.Context, n, h string) ([]netip.Addr, error) {
	return f(c, n, h)
}
func testSender(t *testing.T, handler http.HandlerFunc) (*Sender, string, *atomic.Int32) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	s, err := New(Config{Timeout: time.Second, MaxResponseBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	s.roots = roots
	calls := new(atomic.Int32)
	s.resolver = resolveFunc(func(ctx context.Context, network, host string) ([]netip.Addr, error) {
		calls.Add(1)
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	s.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "8.8.8.8:443" {
			t.Errorf("not pinned: %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	return s, "https://example.com/hook?secret=hidden", calls
}
func TestRequestAndStatuses(t *testing.T) {
	for _, status := range []int{200, 204, 299, 301, 302, 307, 308, 400, 401, 408, 425, 429, 499, 500, 503, 599} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var requests atomic.Int32
			payload := " \n{\"number\":1.00,\"escaped\":\"\\u0061\"}\t"
			s, url, dns := testSender(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				b, err := io.ReadAll(r.Body)
				if err != nil || string(b) != payload {
					t.Errorf("body mismatch %q %v", b, err)
				}
				if r.Method != "POST" || r.Host != "example.com" || r.TLS.ServerName != "example.com" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get(EventIDHeader) != "evt" || r.Header.Get(DeliveryIDHeader) != "del" {
					t.Errorf("request metadata: %+v", r)
				}
				w.Header().Set("Location", "https://127.0.0.1/secret")
				w.WriteHeader(status)
			})
			got := s.Send(context.Background(), url, []byte(payload), "evt", "del")
			want := delivery.ClassificationPermanentFailure
			if status >= 200 && status < 300 {
				want = delivery.ClassificationSucceeded
			} else if status == 408 || status == 425 || status == 429 || status >= 500 {
				want = delivery.ClassificationRetryableFailure
			}
			if got.HTTPStatus != status || got.Classification != want || got.Err != nil || requests.Load() != 1 || dns.Load() != 1 {
				t.Fatalf("%+v requests=%d dns=%d", got, requests.Load(), dns.Load())
			}
		})
	}
}
func TestAddressPolicy(t *testing.T) {
	for _, raw := range []string{"0.0.0.0", "10.1.2.3", "100.100.100.200", "127.0.0.1", "169.254.169.254", "172.16.0.1", "192.168.1.1", "192.0.0.9", "192.0.2.1", "198.18.0.1", "198.51.100.1", "203.0.113.1", "224.0.0.1", "240.0.0.1", "255.255.255.255", "168.63.129.16", "::", "::1", "::ffff:127.0.0.1", "::ffff:169.254.169.254", "fc00::1", "fe80::1", "ff02::1", "64:ff9b::a00:1", "2001::1", "2001:db8::1", "2002:7f00:1::", "3fff::1", "5f00::1", "fe80::1%eth0"} {
		if publicIP(netip.MustParseAddr(raw)) {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, raw := range []string{"8.8.8.8", "1.1.1.1", "::ffff:8.8.8.8", "2606:4700:4700::1111"} {
		if !publicIP(netip.MustParseAddr(raw)) {
			t.Errorf("rejected %s", raw)
		}
	}
}
func TestRejectBeforeDial(t *testing.T) {
	s, _ := New(Config{Timeout: time.Second})
	var dialed atomic.Int32
	s.dial = func(context.Context, string, string) (net.Conn, error) {
		dialed.Add(1)
		return nil, errors.New("should not dial")
	}
	for _, url := range []string{"http://example.com", "/relative", "https:///x", "https://user:secret@example.com", "https://example.com/#", "https://example.com:0", "https://example.com:65536", "https://[fe80::1%25eth0]/", "https://127.0.0.1", "https://[::ffff:127.0.0.1]"} {
		got := s.Send(context.Background(), url, nil, "e", "d")
		if got.Classification != delivery.ClassificationPermanentFailure || got.HTTPStatus != 0 {
			t.Fatalf("%s: %+v", url, got)
		}
	}
	s.resolver = resolveFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("10.0.0.1")}, nil
	})
	got := s.Send(context.Background(), "https://example.com", nil, "e", "d")
	if !errors.Is(got.Err, ErrBlockedDestination) || dialed.Load() != 0 {
		t.Fatalf("mixed DNS accepted: %+v", got)
	}
}
func TestTLSVerification(t *testing.T) {
	for _, wrongHost := range []bool{false, true} {
		s, url, _ := testSender(t, func(w http.ResponseWriter, r *http.Request) { t.Error("unverified request sent") })
		if wrongHost {
			url = strings.Replace(url, "example.com", "wrong.example", 1)
		} else {
			s.roots = x509.NewCertPool()
		}
		got := s.Send(context.Background(), url, nil, "e", "d")
		if got.Classification != delivery.ClassificationPermanentFailure || !errors.Is(got.Err, ErrTLSVerification) || got.HTTPStatus != 0 {
			t.Fatalf("%+v", got)
		}
	}
}
func TestDeadlinesAndCancellation(t *testing.T) {
	for _, stage := range []string{"dns", "connect", "tls", "headers", "body"} {
		t.Run(stage, func(t *testing.T) {
			s, url, _ := testSender(t, func(w http.ResponseWriter, r *http.Request) {
				if stage == "body" {
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			})
			s.config.Timeout = 80 * time.Millisecond
			if stage == "dns" {
				s.resolver = resolveFunc(func(ctx context.Context, _, _ string) ([]netip.Addr, error) { <-ctx.Done(); return nil, ctx.Err() })
			}
			if stage == "connect" {
				s.dial = func(ctx context.Context, _, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }
			}
			if stage == "tls" {
				s.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
					a, b := net.Pipe()
					go func() { <-ctx.Done(); b.Close() }()
					return a, nil
				}
			}
			got := s.Send(context.Background(), url, nil, "e", "d")
			if !errors.Is(got.Err, ErrTimeout) {
				t.Fatalf("%+v", got)
			}
			if stage == "body" {
				if got.HTTPStatus != 200 || got.Classification != delivery.ClassificationSucceeded || !got.BodyReadIncomplete {
					t.Fatalf("body changed success: %+v", got)
				}
			} else if got.HTTPStatus != 0 || got.Classification != delivery.ClassificationRetryableFailure {
				t.Fatalf("%+v", got)
			}
		})
	}
	s, _ := New(Config{Timeout: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := s.Send(ctx, "https://example.com", nil, "e", "d")
	if !errors.Is(got.Err, context.Canceled) || errors.Is(got.Err, ErrTimeout) {
		t.Fatal(got)
	}
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	got = s.Send(ctx, "https://example.com", nil, "e", "d")
	if !errors.Is(got.Err, context.DeadlineExceeded) {
		t.Fatal(got)
	}
}
func TestBodyLimitAndProxy(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	s, url, _ := testSender(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000000")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(strings.Repeat("s", 100)))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	got := s.Send(context.Background(), url, nil, "e", "d")
	if got.HTTPStatus != 200 || got.Classification != delivery.ClassificationSucceeded || !got.BodyReadIncomplete || got.Err != nil {
		t.Fatalf("%+v", got)
	}
}
func TestRebindingAndSanitizedFailures(t *testing.T) {
	s, url, calls := testSender(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	original := s.resolver
	s.resolver = resolveFunc(func(ctx context.Context, n, h string) ([]netip.Addr, error) {
		if calls.Load() > 0 {
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		}
		return original.LookupNetIP(ctx, n, h)
	})
	if got := s.Send(context.Background(), url, nil, "e", "d"); got.HTTPStatus != 204 {
		t.Fatal(got)
	}
	if got := s.Send(context.Background(), url, nil, "e", "d"); !errors.Is(got.Err, ErrBlockedDestination) {
		t.Fatal(got)
	}
	s.resolver = resolveFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return nil, errors.New("secret DNS detail")
	})
	got := s.Send(context.Background(), url, nil, "e", "d")
	if got.Err != ErrNetwork || strings.Contains(got.Err.Error(), "secret") {
		t.Fatal(got)
	}
	if got := s.Send(context.Background(), url, nil, "e\r\nInjected: x", "d"); got.Err != ErrInvalidIdentity {
		t.Fatal(got)
	}
}

func TestCancellationDuringDNS(t *testing.T) {
	s, _ := New(Config{Timeout: time.Second})
	entered := make(chan struct{})
	s.resolver = resolveFunc(func(ctx context.Context, _, _ string) ([]netip.Addr, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan Observation, 1)
	go func() { done <- s.Send(ctx, "https://example.com", nil, "e", "d") }()
	<-entered
	cancel()
	got := <-done
	if !errors.Is(got.Err, context.Canceled) || got.HTTPStatus != 0 {
		t.Fatal(got)
	}
}
func TestApprovedAddressesOnly(t *testing.T) {
	s, _ := New(Config{Timeout: time.Second})
	s.resolver = resolveFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("2606:4700:4700::1111")}, nil
	})
	var targets []string
	s.dial = func(_ context.Context, _, address string) (net.Conn, error) {
		targets = append(targets, address)
		return nil, errors.New("offline")
	}
	_, err := s.dialPublic(context.Background(), "tcp", "example.com:8443")
	if err != ErrNetwork || len(targets) != 2 || targets[0] != "8.8.8.8:8443" || targets[1] != "[2606:4700:4700::1111]:8443" {
		t.Fatalf("%v %v", targets, err)
	}
}
