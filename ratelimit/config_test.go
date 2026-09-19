// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
)

func TestParseDefaults(t *testing.T) {
	p, err := Parse(&config.RateLimitConfig{Rate: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Algorithm != config.RateLimitAlgorithmTokenBucket {
		t.Errorf("unexpected algorithm %s", p.Algorithm)
	}
	if p.Strategy != config.RateLimitStrategyEndpoint {
		t.Errorf("unexpected strategy %s", p.Strategy)
	}
	if p.Burst != 10 {
		t.Errorf("unexpected burst %d", p.Burst)
	}
	if p.Window != time.Second {
		t.Errorf("unexpected window %s", p.Window)
	}
	if p.APIKeyHeader != config.DefaultRateLimitAPIKeyHeader {
		t.Errorf("unexpected header %s", p.APIKeyHeader)
	}
	if p.Store.Type != config.RateLimitStoreMemory {
		t.Errorf("unexpected store %s", p.Store.Type)
	}
	if p.Store.KeyPrefix != defaultKeyPrefix || p.Store.PoolSize != defaultPoolSize {
		t.Errorf("unexpected store defaults: %+v", p.Store)
	}
	if !p.FailOpen() {
		t.Error("fail open should default to true")
	}
}

func TestParseSubunitRate(t *testing.T) {
	p, err := Parse(&config.RateLimitConfig{Rate: 0.1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Burst != 1 {
		t.Errorf("burst for rate < 1 must be 1, got %d", p.Burst)
	}
}

func TestParseFailClosed(t *testing.T) {
	failOpen := false
	p, err := Parse(&config.RateLimitConfig{Rate: 1, Store: config.RateLimitStoreConfig{FailOpen: &failOpen}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.FailOpen() {
		t.Error("policy should fail closed")
	}
}

func TestParseErrors(t *testing.T) {
	for name, cfg := range map[string]*config.RateLimitConfig{
		"nil":              nil,
		"zero rate":        {Rate: 0},
		"bad algorithm":    {Rate: 1, Algorithm: "leaky"},
		"bad strategy":     {Rate: 1, Strategy: "user"},
		"bad store":        {Rate: 1, Store: config.RateLimitStoreConfig{Type: "etcd"}},
		"redis no address": {Rate: 1, Store: config.RateLimitStoreConfig{Type: config.RateLimitStoreRedis}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(cfg); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestRedisDefaults(t *testing.T) {
	p, err := Parse(&config.RateLimitConfig{
		Rate:  5,
		Store: config.RateLimitStoreConfig{Type: config.RateLimitStoreRedis, Address: "redis:6379"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Store.DialTimeout != defaultDialTimeout || p.Store.ReadTimeout != defaultReadTimeout {
		t.Errorf("unexpected timeouts: %+v", p.Store)
	}
}
