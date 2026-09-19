// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"net/http"
	"testing"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
)

// TestDefaultFactory_rateLimitBlocksBeforeForwarding wires the real default
// factory stack with a backend proxy that would always succeed and verifies
// the endpoint rate limiter rejects excess requests without touching it.
func TestDefaultFactory_rateLimitBlocksBeforeForwarding(t *testing.T) {
	backendCalls := 0
	backendFactory := func(_ *config.Backend) Proxy {
		return func(_ context.Context, _ *Request) (*Response, error) {
			backendCalls++
			return &Response{Data: map[string]interface{}{"ok": true}, IsComplete: true}, nil
		}
	}

	factory := NewDefaultFactory(backendFactory, logging.NoOp)
	cfg := &config.EndpointConfig{
		Endpoint: "/limited",
		Method:   "GET",
		Backend: []*config.Backend{{
			Host:       []string{"http://127.0.0.1:8080"},
			URLPattern: "/upstream",
		}},
		ConcurrentCalls: 1,
		Timeout:         1000000000,
		RateLimit: &config.RateLimitConfig{
			Algorithm: config.RateLimitAlgorithmTokenBucket,
			Strategy:  config.RateLimitStrategyEndpoint,
			Rate:      1,
			Burst:     1,
		},
	}
	cfg.Backend[0].ParentEndpoint = "/limited"
	cfg.Backend[0].ParentEndpointMethod = "GET"

	p, err := factory.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := testRequest(nil)
	if _, err := p(context.Background(), req); err != nil {
		t.Fatalf("first request should be allowed: %v", err)
	}

	_, err = p(context.Background(), req)
	if err == nil {
		t.Fatal("second request should be rejected by the endpoint limiter")
	}
	rle, ok := err.(*RateLimitedError)
	if !ok {
		t.Fatalf("expected RateLimitedError, got %T: %v", err, err)
	}
	if rle.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("unexpected status %d", rle.StatusCode())
	}
	if backendCalls != 1 {
		t.Fatalf("the backend must be called only once, got %d", backendCalls)
	}
}

func TestDefaultFactory_noRateLimitPassthrough(t *testing.T) {
	backendCalls := 0
	backendFactory := func(_ *config.Backend) Proxy {
		return func(_ context.Context, _ *Request) (*Response, error) {
			backendCalls++
			return &Response{Data: map[string]interface{}{"ok": true}, IsComplete: true}, nil
		}
	}
	factory := NewDefaultFactory(backendFactory, logging.NoOp)
	cfg := &config.EndpointConfig{
		Endpoint:        "/open",
		Method:          "GET",
		ConcurrentCalls: 1,
		Timeout:         1000000000,
		Backend:         []*config.Backend{{Host: []string{"http://127.0.0.1:8080"}, URLPattern: "/upstream"}},
	}
	cfg.Backend[0].ParentEndpoint = "/open"
	cfg.Backend[0].ParentEndpointMethod = "GET"

	p, err := factory.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := p(context.Background(), testRequest(nil)); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	if backendCalls != 10 {
		t.Fatalf("expected 10 backend calls, got %d", backendCalls)
	}
}
