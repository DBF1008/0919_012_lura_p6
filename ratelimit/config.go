// SPDX-License-Identifier: Apache-2.0

/*
Package ratelimit provides rate limiting primitives used by the proxy layer.

Two algorithms are supported: token bucket and sliding window. The state of the
limiters can be kept in the local process (memory store) or shared across all the
gateway instances through a Redis server (redis store), which is the recommended
setup for distributed deployments.
*/
package ratelimit

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/luraproject/lura/v2/config"
)

const (
	defaultWindow          = time.Second
	defaultDialTimeout     = 100 * time.Millisecond
	defaultReadTimeout     = 100 * time.Millisecond
	defaultKeyPrefix       = "lura:ratelimit"
	defaultPoolSize        = 5
	defaultCleanupInterval = time.Minute
)

// Common errors returned while parsing or building the limiters.
var (
	// ErrInvalidRate is returned when a policy defines a non positive rate.
	ErrInvalidRate = errors.New("ratelimit: rate must be greater than 0")
	// ErrUnknownAlgorithm is returned when the algorithm name is not supported.
	ErrUnknownAlgorithm = errors.New("ratelimit: unknown algorithm")
	// ErrUnknownStrategy is returned when the key strategy is not supported.
	ErrUnknownStrategy = errors.New("ratelimit: unknown strategy")
	// ErrUnknownStore is returned when the store type is not supported.
	ErrUnknownStore = errors.New("ratelimit: unknown store")
	// ErrMissingRedisAddress is returned when the redis store has no address.
	ErrMissingRedisAddress = errors.New("ratelimit: redis store requires an address")
)

// Policy is the normalized representation of a config.RateLimitConfig.
type Policy struct {
	Algorithm    string
	Strategy     string
	Rate         float64
	Burst        int
	Window       time.Duration
	APIKeyHeader string
	Store        config.RateLimitStoreConfig
	// Dialer overrides the default TCP dialer, mostly useful for tests and for
	// transports such as unix sockets.
	Dialer func(ctx context.Context, address string) (net.Conn, error)
}

// Parse validates and normalizes the received rate limit configuration.
func Parse(cfg *config.RateLimitConfig) (Policy, error) {
	if cfg == nil || !cfg.Enabled() {
		return Policy{}, ErrInvalidRate
	}

	algorithm := cfg.Algorithm
	if algorithm == "" {
		algorithm = config.RateLimitAlgorithmTokenBucket
	}
	if algorithm != config.RateLimitAlgorithmTokenBucket &&
		algorithm != config.RateLimitAlgorithmSlidingWindow {
		return Policy{}, ErrUnknownAlgorithm
	}

	strategy := cfg.Strategy
	if strategy == "" {
		strategy = config.RateLimitStrategyEndpoint
	}
	if strategy != config.RateLimitStrategyEndpoint &&
		strategy != config.RateLimitStrategyIP &&
		strategy != config.RateLimitStrategyAPIKey {
		return Policy{}, ErrUnknownStrategy
	}

	burst := cfg.Burst
	if burst <= 0 {
		burst = int(cfg.Rate)
		if burst < 1 {
			burst = 1
		}
	}

	window := cfg.Window
	if window <= 0 {
		window = defaultWindow
	}

	header := cfg.APIKeyHeader
	if header == "" {
		header = config.DefaultRateLimitAPIKeyHeader
	}

	store := cfg.Store
	if store.Type == "" {
		store.Type = config.RateLimitStoreMemory
	}
	if store.Type != config.RateLimitStoreMemory && store.Type != config.RateLimitStoreRedis {
		return Policy{}, ErrUnknownStore
	}
	if store.Type == config.RateLimitStoreRedis && store.Address == "" {
		return Policy{}, ErrMissingRedisAddress
	}
	if store.DialTimeout <= 0 {
		store.DialTimeout = defaultDialTimeout
	}
	if store.ReadTimeout <= 0 {
		store.ReadTimeout = defaultReadTimeout
	}
	if store.KeyPrefix == "" {
		store.KeyPrefix = defaultKeyPrefix
	}
	if store.PoolSize <= 0 {
		store.PoolSize = defaultPoolSize
	}

	return Policy{
		Algorithm:    algorithm,
		Strategy:     strategy,
		Rate:         cfg.Rate,
		Burst:        burst,
		Window:       window,
		APIKeyHeader: header,
		Store:        store,
	}, nil
}

// FailOpen reports whether requests must proceed when the store is unavailable.
func (p Policy) FailOpen() bool {
	return p.Store.FailOpen == nil || *p.Store.FailOpen
}
