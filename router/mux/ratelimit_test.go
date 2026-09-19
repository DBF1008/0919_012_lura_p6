// SPDX-License-Identifier: Apache-2.0

package mux

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/proxy"
)

func TestEndpointHandler_rateLimited(t *testing.T) {
	p := func(_ context.Context, _ *proxy.Request) (*proxy.Response, error) {
		return nil, proxy.NewRateLimitedError(3*time.Second, "ep|endpoint")
	}

	cfg := &config.EndpointConfig{
		Endpoint: "/limited",
		Method:   "GET",
		Timeout:  time.Second,
	}
	hf := CustomEndpointHandler(NewRequestBuilder(NoopParamExtractor))
	handler := hf(cfg, p)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/limited", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "3" {
		t.Fatalf("expected Retry-After: 3, got %q", got)
	}
}
