// SPDX-License-Identifier: Apache-2.0

package config

import "testing"

func TestRateLimitConfig_Enabled(t *testing.T) {
	var nilCfg *RateLimitConfig
	if nilCfg.Enabled() {
		t.Fatal("nil config must be disabled")
	}
	if (&RateLimitConfig{}).Enabled() {
		t.Fatal("zero rate must be disabled")
	}
	if !(&RateLimitConfig{Rate: 1}).Enabled() {
		t.Fatal("positive rate must be enabled")
	}
}

func TestEndpointAndBackendRateLimitFields(t *testing.T) {
	ep := EndpointConfig{
		Endpoint: "/test",
		Method:   "GET",
		RateLimit: &RateLimitConfig{
			Algorithm: RateLimitAlgorithmSlidingWindow,
			Strategy:  RateLimitStrategyAPIKey,
			Rate:      10,
			Burst:     20,
		},
		Backend: []*Backend{
			{
				Host:       []string{"http://backend.example.com"},
				URLPattern: "/upstream",
				RateLimit:  &RateLimitConfig{Algorithm: RateLimitAlgorithmTokenBucket, Rate: 5},
			},
		},
	}

	if !ep.RateLimit.Enabled() || ep.RateLimit.Algorithm != RateLimitAlgorithmSlidingWindow {
		t.Fatalf("unexpected endpoint rate limit: %+v", ep.RateLimit)
	}
	if !ep.Backend[0].RateLimit.Enabled() {
		t.Fatal("backend rate limit must be enabled")
	}
}
