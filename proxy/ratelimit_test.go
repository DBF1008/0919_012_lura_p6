// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
)

func TestMemoryStore_TokenBucket(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()

	// burst of 2: the first two requests are allowed
	for i := 0; i < 2; i++ {
		allowed, retryAfter := store.AllowTokenBucket("k", 1, 2, now)
		if !allowed {
			t.Fatalf("request %d should have been allowed", i)
		}
		if retryAfter != 0 {
			t.Errorf("unexpected retry after: %s", retryAfter)
		}
	}

	// the bucket is empty: the third request is rejected
	allowed, retryAfter := store.AllowTokenBucket("k", 1, 2, now)
	if allowed {
		t.Fatal("the request should have been rejected")
	}
	if retryAfter <= 0 || retryAfter > time.Second {
		t.Errorf("unexpected retry after: %s", retryAfter)
	}

	// after one second a new token is available
	allowed, _ = store.AllowTokenBucket("k", 1, 2, now.Add(time.Second))
	if !allowed {
		t.Fatal("the request should have been allowed after the refill")
	}

	// keys are independent
	allowed, _ = store.AllowTokenBucket("other", 1, 2, now)
	if !allowed {
		t.Fatal("a different key should not be affected")
	}
}

func TestMemoryStore_SlidingWindow(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	window := time.Second

	for i := 0; i < 2; i++ {
		allowed, retryAfter := store.AllowSlidingWindow("k", 2, window, now)
		if !allowed {
			t.Fatalf("request %d should have been allowed", i)
		}
		if retryAfter != 0 {
			t.Errorf("unexpected retry after: %s", retryAfter)
		}
	}

	allowed, retryAfter := store.AllowSlidingWindow("k", 2, window, now)
	if allowed {
		t.Fatal("the request should have been rejected")
	}
	if retryAfter <= 0 || retryAfter > window {
		t.Errorf("unexpected retry after: %s", retryAfter)
	}

	// once the window slides, requests are allowed again
	allowed, _ = store.AllowSlidingWindow("k", 2, window, now.Add(window+time.Millisecond))
	if !allowed {
		t.Fatal("the request should have been allowed after the window slid")
	}
}

func TestRateLimitError(t *testing.T) {
	err := RateLimitError{Key: "k", RetryAfter: 1500 * time.Millisecond}
	if !errors.Is(err, ErrRateLimited) {
		t.Error("RateLimitError should unwrap to ErrRateLimited")
	}
	if !IsRateLimitError(err) {
		t.Error("IsRateLimitError should detect the error")
	}
	if IsRateLimitError(errors.New("other")) {
		t.Error("IsRateLimitError should reject unrelated errors")
	}
	if code := err.StatusCode(); code != http.StatusTooManyRequests {
		t.Errorf("unexpected status code: %d", code)
	}
	if got := err.Headers()["Retry-After"]; len(got) != 1 || got[0] != "2" {
		t.Errorf("unexpected Retry-After header: %v", got)
	}
}

func rateLimitedBackend(cfg *config.RateLimitConfig) *config.Backend {
	return &config.Backend{
		ParentEndpoint:       "/test",
		ParentEndpointMethod: "GET",
		URLPattern:           "/",
		RateLimit:            cfg,
	}
}

func TestRateLimitMiddleware_Disabled(t *testing.T) {
	calls := 0
	next := func(_ context.Context, _ *Request) (*Response, error) {
		calls++
		return &Response{}, nil
	}

	for _, cfg := range []*config.RateLimitConfig{nil, {Enabled: false}} {
		backend := rateLimitedBackend(cfg)
		p := NewRateLimitMiddlewareWithLogger(logging.NoOp, backend)(next)
		for i := 0; i < 5; i++ {
			if _, err := p(context.Background(), &Request{}); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		}
	}
	if calls != 10 {
		t.Errorf("expected 10 calls to the next proxy, got %d", calls)
	}
}

func TestRateLimitMiddleware_TokenBucket(t *testing.T) {
	SetStore(NewMemoryStore())
	backend := rateLimitedBackend(&config.RateLimitConfig{
		Enabled:   true,
		Algorithm: config.RateLimitAlgorithmTokenBucket,
		Dimension: config.RateLimitDimensionEndpoint,
		Rate:      0.001,
		Burst:     1,
	})

	next := func(_ context.Context, _ *Request) (*Response, error) {
		return &Response{Data: map[string]interface{}{"ok": true}}, nil
	}
	p := NewRateLimitMiddlewareWithLogger(logging.NoOp, backend)(next)

	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Fatalf("the first request should have been allowed: %s", err)
	}

	resp, err := p(context.Background(), &Request{})
	if resp != nil {
		t.Error("rejected requests should not return a response")
	}
	var rlErr RateLimitError
	if !errors.As(err, &rlErr) {
		t.Fatalf("expected a RateLimitError, got %T (%s)", err, err)
	}
	if rlErr.StatusCode() != http.StatusTooManyRequests {
		t.Errorf("unexpected status code: %d", rlErr.StatusCode())
	}
	if rlErr.RetryAfter <= 0 {
		t.Error("expected a positive RetryAfter")
	}
}

func TestRateLimitMiddleware_SlidingWindow(t *testing.T) {
	SetStore(NewMemoryStore())
	backend := rateLimitedBackend(&config.RateLimitConfig{
		Enabled:   true,
		Algorithm: config.RateLimitAlgorithmSlidingWindow,
		Dimension: config.RateLimitDimensionEndpoint,
		Rate:      0.001,
		Burst:     2,
	})

	next := func(_ context.Context, _ *Request) (*Response, error) {
		return &Response{}, nil
	}
	p := NewRateLimitMiddlewareWithLogger(logging.NoOp, backend)(next)

	for i := 0; i < 2; i++ {
		if _, err := p(context.Background(), &Request{}); err != nil {
			t.Fatalf("request %d should have been allowed: %s", i, err)
		}
	}
	if _, err := p(context.Background(), &Request{}); !IsRateLimitError(err) {
		t.Fatalf("expected a rate limit error, got %v", err)
	}
}

func TestRateLimitMiddleware_Dimensions(t *testing.T) {
	testCases := []struct {
		name      string
		cfg       *config.RateLimitConfig
		reqA      *Request
		reqB      *Request
		sharedKey bool
	}{
		{
			name: "endpoint",
			cfg: &config.RateLimitConfig{
				Enabled:   true,
				Dimension: config.RateLimitDimensionEndpoint,
				Rate:      0.001,
				Burst:     1,
			},
			reqA:      &Request{Headers: map[string][]string{"X-Forwarded-For": {"1.1.1.1"}}},
			reqB:      &Request{Headers: map[string][]string{"X-Forwarded-For": {"2.2.2.2"}}},
			sharedKey: true,
		},
		{
			name: "ip",
			cfg: &config.RateLimitConfig{
				Enabled:   true,
				Dimension: config.RateLimitDimensionIP,
				Rate:      0.001,
				Burst:     1,
			},
			reqA:      &Request{Headers: map[string][]string{"X-Forwarded-For": {"1.1.1.1, 10.0.0.1"}}},
			reqB:      &Request{Headers: map[string][]string{"X-Forwarded-For": {"2.2.2.2"}}},
			sharedKey: false,
		},
		{
			name: "apikey",
			cfg: &config.RateLimitConfig{
				Enabled:      true,
				Dimension:    config.RateLimitDimensionAPIKey,
				Rate:         0.001,
				Burst:        1,
				APIKeyHeader: "X-Custom-Key",
			},
			reqA:      &Request{Headers: map[string][]string{"X-Custom-Key": {"aaa"}}},
			reqB:      &Request{Headers: map[string][]string{"X-Custom-Key": {"bbb"}}},
			sharedKey: false,
		},
		{
			name: "apikey default header",
			cfg: &config.RateLimitConfig{
				Enabled:   true,
				Dimension: config.RateLimitDimensionAPIKey,
				Rate:      0.001,
				Burst:     1,
			},
			reqA:      &Request{Headers: map[string][]string{"X-Api-Key": {"aaa"}}},
			reqB:      &Request{Headers: map[string][]string{"X-Api-Key": {"bbb"}}},
			sharedKey: false,
		},
	}

	next := func(_ context.Context, _ *Request) (*Response, error) {
		return &Response{}, nil
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			SetStore(NewMemoryStore())
			p := NewRateLimitMiddlewareWithLogger(logging.NoOp, rateLimitedBackend(tc.cfg))(next)

			if _, err := p(context.Background(), tc.reqA); err != nil {
				t.Fatalf("the first request should have been allowed: %s", err)
			}
			// the second request with key A is rejected
			if _, err := p(context.Background(), tc.reqA); !IsRateLimitError(err) {
				t.Fatalf("the second request with the same key should have been rejected: %v", err)
			}
			// a request with key B
			_, err := p(context.Background(), tc.reqB)
			if tc.sharedKey && !IsRateLimitError(err) {
				t.Error("requests sharing the key should have been rejected")
			}
			if !tc.sharedKey && err != nil {
				t.Errorf("requests with a different key should have been allowed: %s", err)
			}
		})
	}
}

func TestEndpointRateLimitMiddleware(t *testing.T) {
	SetStore(NewMemoryStore())
	cfg := &config.EndpointConfig{
		Endpoint: "/test",
		Method:   "GET",
		RateLimit: &config.RateLimitConfig{
			Enabled: true,
			Rate:    0.001,
			Burst:   1,
		},
	}

	next := func(_ context.Context, _ *Request) (*Response, error) {
		return &Response{}, nil
	}
	p := NewEndpointRateLimitMiddlewareWithLogger(logging.NoOp, cfg)(next)

	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Fatalf("the first request should have been allowed: %s", err)
	}
	if _, err := p(context.Background(), &Request{}); !IsRateLimitError(err) {
		t.Fatalf("expected a rate limit error, got %v", err)
	}
}

func TestRateLimitMiddleware_SharedStore(t *testing.T) {
	// two stacks (e.g. two instances of the same backend) sharing a store
	// share the limiter state
	SetStore(NewMemoryStore())
	backend := rateLimitedBackend(&config.RateLimitConfig{
		Enabled: true,
		Rate:    0.001,
		Burst:   1,
	})

	next := func(_ context.Context, _ *Request) (*Response, error) {
		return &Response{}, nil
	}
	p1 := NewRateLimitMiddlewareWithLogger(logging.NoOp, backend)(next)
	p2 := NewRateLimitMiddlewareWithLogger(logging.NoOp, backend)(next)

	if _, err := p1(context.Background(), &Request{}); err != nil {
		t.Fatalf("the first request should have been allowed: %s", err)
	}
	if _, err := p2(context.Background(), &Request{}); !IsRateLimitError(err) {
		t.Fatalf("expected a rate limit error from the shared store, got %v", err)
	}
}

func TestClientIPKey(t *testing.T) {
	testCases := []struct {
		name     string
		headers  map[string][]string
		expected string
	}{
		{"forwarded for", map[string][]string{"X-Forwarded-For": {"1.2.3.4, 10.0.0.1"}}, "1.2.3.4"},
		{"real ip", map[string][]string{"X-Real-Ip": {"1.2.3.4"}}, "1.2.3.4"},
		{"lowercase header", map[string][]string{"x-forwarded-for": {"1.2.3.4"}}, "1.2.3.4"},
		{"no headers", nil, "unknown"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clientIPKey(&Request{Headers: tc.headers}); got != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestRateLimitError_Message(t *testing.T) {
	err := RateLimitError{Key: "k", RetryAfter: time.Second}
	if !strings.Contains(err.Error(), ErrRateLimited.Error()) {
		t.Errorf("the error message should contain %q: %s", ErrRateLimited, err.Error())
	}
}
