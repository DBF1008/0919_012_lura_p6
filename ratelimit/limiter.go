// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"context"
	"time"
)

// Decision is the result of a single Allow evaluation.
type Decision struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// RetryAfter is the suggested time the client should wait before retrying.
	// It is zero when the request is allowed.
	RetryAfter time.Duration
	// Remaining is the number of permits left right after the decision.
	Remaining int
	// Limit is the configured burst / window capacity.
	Limit int
}

// Limiter evaluates requests against a single bucket identified by key.
type Limiter interface {
	// Allow consumes one permit for the key at the current time.
	Allow(ctx context.Context, key string) (Decision, error)
}

// Manager owns a set of limiters, one per named scope (for instance, an
// endpoint). Each scope keeps independent buckets keyed by the configured
// dimension (endpoint, client IP or API key).
type Manager interface {
	// Allow consumes one permit from the bucket of the received scope (for
	// instance an endpoint) and dimension key.
	Allow(ctx context.Context, scope, key string) (Decision, error)
	// Close releases every resource held by the manager.
	Close() error
}

// scopeKey prefixes the bucket key with the scope it belongs to, so different
// endpoints using the same manager cannot share counters.
func scopeKey(scope, key string) string {
	return scope + "|" + key
}
