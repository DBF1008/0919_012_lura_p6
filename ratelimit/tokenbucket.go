// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"sync"
	"time"
)

// tokenBucket is a thread-safe refill-at-rate token bucket with a capped
// burst capacity.
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	rate       float64
	capacity   float64
	lastRefill time.Time
}

func newTokenBucket(rate float64, burst int, now time.Time) *tokenBucket {
	return &tokenBucket{
		tokens:     float64(burst),
		rate:       rate,
		capacity:   float64(burst),
		lastRefill: now,
	}
}

// take consumes one token, refilling the bucket up to its capacity first.
func (b *tokenBucket) take(now time.Time) (allowed bool, retryAfter time.Duration, remaining int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if elapsed := now.Sub(b.lastRefill).Seconds(); elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.lastRefill = now
	}

	if b.tokens >= 1 {
		b.tokens--
		return true, 0, int(b.tokens)
	}

	// time needed to accumulate the missing fraction of a token
	retryAfter = time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
	return false, retryAfter, 0
}
