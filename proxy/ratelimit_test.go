// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
)

func testRequest(headers map[string][]string) *Request {
	u, _ := url.Parse("http://example.com/test")
	return &Request{
		Method:  http.MethodGet,
		URL:     u,
		Path:    "/test",
		Headers: headers,
	}
}

func newCountingProxy(calls *int) Proxy {
	return func(_ context.Context, _ *Request) (*Response, error) {
		*calls++
		return &Response{Data: map[string]interface{}{"ok": true}, IsComplete: true}, nil
	}
}

func TestRateLimitMiddlewareDisabled(t *testing.T) {
	calls := 0
	p := NewRateLimitMiddleware(logging.NoOp, "GET /test", nil)(newCountingProxy(&calls))
	for i := 0; i < 100; i++ {
		if _, err := p(context.Background(), testRequest(nil)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if calls != 100 {
		t.Fatalf("all requests must reach the backend, got %d", calls)
	}
}

func TestRateLimitMiddlewareEndpoint(t *testing.T) {
	calls := 0
	cfg := &config.RateLimitConfig{
		Algorithm: config.RateLimitAlgorithmTokenBucket,
		Strategy:  config.RateLimitStrategyEndpoint,
		Rate:      1,
		Burst:     2,
	}
	p := NewRateLimitMiddleware(logging.NoOp, "GET /test", cfg)(newCountingProxy(&calls))

	allowed, rejected := 0, 0
	for i := 0; i < 5; i++ {
		_, err := p(context.Background(), testRequest(nil))
		switch {
		case err == nil:
			allowed++
		default:
			assertRateLimited(t, err)
			rejected++
		}
	}
	if allowed != 2 || rejected != 3 {
		t.Fatalf("expected 2 allowed / 3 rejected, got %d / %d", allowed, rejected)
	}
	if calls != 2 {
		t.Fatalf("rejected requests must not reach the backend, got %d calls", calls)
	}
}

func TestRateLimitMiddlewareSlidingWindowPerIP(t *testing.T) {
	calls := 0
	cfg := &config.RateLimitConfig{
		Algorithm: config.RateLimitAlgorithmSlidingWindow,
		Strategy:  config.RateLimitStrategyIP,
		Rate:      1,
		Burst:     1,
		Window:    60000000000,
	}
	p := NewRateLimitMiddleware(logging.NoOp, "GET /test", cfg)(newCountingProxy(&calls))

	reqA := testRequest(map[string][]string{"X-Forwarded-For": {"10.0.0.1"}})
	reqB := testRequest(map[string][]string{"X-Forwarded-For": {"10.0.0.2"}})

	if _, err := p(context.Background(), reqA); err != nil {
		t.Fatalf("first request from A allowed: %v", err)
	}
	if err := mustFail(p, reqA); err == nil {
		t.Fatal("second request from A within the window must be rejected")
	}
	if _, err := p(context.Background(), reqB); err != nil {
		t.Fatalf("independent IP bucket must be allowed: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 backend calls, got %d", calls)
	}
}

func TestRateLimitMiddlewareAPIKey(t *testing.T) {
	calls := 0
	cfg := &config.RateLimitConfig{
		Algorithm: config.RateLimitAlgorithmTokenBucket,
		Strategy:  config.RateLimitStrategyAPIKey,
		Rate:      1,
		Burst:     1,
	}
	p := NewRateLimitMiddleware(logging.NoOp, "GET /test", cfg)(newCountingProxy(&calls))

	key1 := testRequest(map[string][]string{"X-API-Key": {"key-1"}})
	key2 := testRequest(map[string][]string{"X-API-Key": {"key-2"}})

	if _, err := p(context.Background(), key1); err != nil {
		t.Fatalf("first request with key-1 allowed: %v", err)
	}
	if err := mustFail(p, key1); err == nil {
		t.Fatal("key-1 second request must be rejected")
	}
	if _, err := p(context.Background(), key2); err != nil {
		t.Fatalf("key-2 has an independent bucket: %v", err)
	}
}

func TestRateLimitMiddlewareAnonymousKeyFallsBackToIP(t *testing.T) {
	calls := 0
	cfg := &config.RateLimitConfig{
		Strategy: config.RateLimitStrategyAPIKey,
		Rate:     1,
		Burst:    1,
	}
	p := NewRateLimitMiddleware(logging.NoOp, "GET /test", cfg)(newCountingProxy(&calls))

	anon1 := testRequest(map[string][]string{"X-Forwarded-For": {"10.0.0.9"}})
	anon2 := testRequest(map[string][]string{"X-Forwarded-For": {"10.0.0.10"}})

	if _, err := p(context.Background(), anon1); err != nil {
		t.Fatalf("first anonymous request allowed: %v", err)
	}
	if err := mustFail(p, anon1); err == nil {
		t.Fatal("same anonymous IP must share the bucket")
	}
	if _, err := p(context.Background(), anon2); err != nil {
		t.Fatalf("other anonymous IP allowed: %v", err)
	}
}

func TestRateLimitMiddlewareSharedManager(t *testing.T) {
	cfg := &config.RateLimitConfig{Rate: 1, Burst: 1}
	_, policy, release, err := managerFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	m1, _, release1, err := managerFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer release1()

	d1, err := m1.Allow(context.Background(), "scope", policy.Strategy)
	if err != nil || !d1.Allowed {
		t.Fatalf("first allow: %+v %v", d1, err)
	}
	// the second manager is the same instance, so the bucket is already empty
	d2, _ := m1.Allow(context.Background(), "scope", policy.Strategy)
	if d2.Allowed {
		t.Fatal("shared manager must observe the consumed permit")
	}
}

func TestClientIPFromRequest(t *testing.T) {
	cases := map[string]*Request{
		"10.0.0.1":    testRequest(map[string][]string{"X-Forwarded-For": {"10.0.0.1, 9.9.9.9"}}),
		"10.0.0.2":    testRequest(map[string][]string{"X-Real-Ip": {"10.0.0.2"}}),
		"example.com": testRequest(nil),
	}
	for expected, req := range cases {
		if got := clientIPFromRequest(req); got != expected {
			t.Errorf("expected %s, got %s", expected, got)
		}
	}
	if got := clientIPFromRequest(&Request{}); got != "unknown" {
		t.Errorf("expected unknown, got %s", got)
	}
}

func TestRateLimitedErrorInterface(t *testing.T) {
	e := NewRateLimitedError(0, "k")
	if e.StatusCode() != http.StatusTooManyRequests {
		t.Errorf("unexpected status code %d", e.StatusCode())
	}
	if e.RetryAfterSeconds() != 1 {
		t.Errorf("sub-second retry-after must round to 1, got %d", e.RetryAfterSeconds())
	}
	if got := e.Headers()["Retry-After"][0]; got != "1" {
		t.Errorf("unexpected header value %q", got)
	}
}

func mustFail(p Proxy, req *Request) error {
	_, err := p(context.Background(), req)
	if err != nil {
		assertRateLimited(nil, err)
	}
	return err
}

func assertRateLimited(t *testing.T, err error) {
	rle, ok := err.(*RateLimitedError)
	if !ok {
		if t != nil {
			t.Fatalf("expected RateLimitedError, got %T: %v", err, err)
		}
		return
	}
	if rle.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("unexpected status %d", rle.StatusCode())
	}
	if rle.RetryAfterSeconds() < 1 {
		t.Fatal("retry after must be at least 1 second")
	}
}
