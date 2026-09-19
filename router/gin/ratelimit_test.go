// SPDX-License-Identifier: Apache-2.0

package gin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
	"github.com/luraproject/lura/v2/transport/http/server"
)

func TestEndpointHandler_rateLimited(t *testing.T) {
	gin.SetMode(gin.TestMode)

	p := func(_ context.Context, _ *proxy.Request) (*proxy.Response, error) {
		return nil, proxy.NewRateLimitedError(2*time.Second, "ep|endpoint")
	}

	cfg := &config.EndpointConfig{
		Endpoint: "/limited",
		Method:   "GET",
		Timeout:  time.Second,
	}
	handler := CustomErrorEndpointHandler(logging.NoOp, server.DefaultToHTTPError)(cfg, p)

	engine := gin.New()
	engine.GET("/limited", handler)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/limited", nil)
	engine.ServeHTTP(w, r)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("expected Retry-After: 2, got %q", got)
	}
}
