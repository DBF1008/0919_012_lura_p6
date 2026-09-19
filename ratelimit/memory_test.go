// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"context"
	"testing"
	"time"
)

func newTestMemoryManager(policy Policy, now *time.Time) *MemoryManager {
	m := NewMemoryManager(policy)
	m.now = func() time.Time { return *now }
	return m
}

func TestTokenBucketAllowsBurstThenRefills(t *testing.T) {
	start := time.Unix(1000, 0)
	policy := Policy{
		Algorithm: "token_bucket",
		Rate:      10,
		Burst:     3,
	}
	m := newTestMemoryManager(policy, &start)
	defer m.Close()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		d, err := m.Allow(ctx, "ep", "endpoint")
		if err != nil || !d.Allowed {
			t.Fatalf("request %d should be allowed: %+v %v", i, d, err)
		}
		if d.Limit != 3 {
			t.Errorf("unexpected limit %d", d.Limit)
		}
	}

	d, _ := m.Allow(ctx, "ep", "endpoint")
	if d.Allowed {
		t.Fatal("4th immediate request should be rejected")
	}
	if d.RetryAfter < time.Second {
		t.Errorf("retry-after must round up to 1s, got %s", d.RetryAfter)
	}

	// after 100ms one token must be available again
	start = start.Add(100 * time.Millisecond)
	d, _ = m.Allow(ctx, "ep", "endpoint")
	if !d.Allowed {
		t.Fatalf("request after refill should be allowed: %+v", d)
	}
}

func TestSlidingWindow(t *testing.T) {
	start := time.Unix(1000, 0)
	policy := Policy{
		Algorithm: "sliding_window",
		Burst:     2,
		Window:    time.Second,
	}
	m := newTestMemoryManager(policy, &start)
	defer m.Close()
	ctx := context.Background()

	if d, _ := m.Allow(ctx, "ep", "ip-1"); !d.Allowed {
		t.Fatal("first request allowed")
	}
	if d, _ := m.Allow(ctx, "ep", "ip-1"); !d.Allowed {
		t.Fatal("second request allowed")
	}
	if d, _ := m.Allow(ctx, "ep", "ip-1"); d.Allowed {
		t.Fatal("third request within the window must be rejected")
	}

	// a different key has an independent window
	if d, _ := m.Allow(ctx, "ep", "ip-2"); !d.Allowed {
		t.Fatal("other keys must have independent windows")
	}

	// once the window slides, requests are accepted again
	start = start.Add(time.Second + time.Millisecond)
	if d, _ := m.Allow(ctx, "ep", "ip-1"); !d.Allowed {
		t.Fatal("request after the window should be allowed")
	}
}

func TestIndependentScopes(t *testing.T) {
	start := time.Unix(1000, 0)
	policy := Policy{Algorithm: "token_bucket", Rate: 1, Burst: 1}
	m := newTestMemoryManager(policy, &start)
	defer m.Close()
	ctx := context.Background()

	if d, _ := m.Allow(ctx, "GET /a", "endpoint"); !d.Allowed {
		t.Fatal("scope a first request allowed")
	}
	if d, _ := m.Allow(ctx, "GET /b", "endpoint"); !d.Allowed {
		t.Fatal("scope b has an independent bucket")
	}
	if d, _ := m.Allow(ctx, "GET /a", "endpoint"); d.Allowed {
		t.Fatal("scope a second request rejected")
	}
}

func TestEvictIdleBuckets(t *testing.T) {
	now := time.Unix(1000, 0)
	policy := Policy{Algorithm: "token_bucket", Rate: 1, Burst: 1}
	m := NewMemoryManager(policy)
	m.now = func() time.Time { return now }

	if _, err := m.Allow(context.Background(), "ep", "k"); err != nil {
		t.Fatal(err)
	}
	if len(m.buckets) != 1 {
		t.Fatalf("unexpected bucket count %d", len(m.buckets))
	}
	m.evict(now)
	if len(m.buckets) != 1 {
		t.Fatal("fresh bucket must not be evicted")
	}
	// janitor runs at time T and evicts entries not used since T-idleTimeout
	m.evict(now.Add(11 * defaultCleanupInterval).Add(time.Nanosecond))
	if len(m.buckets) != 0 {
		t.Fatal("idle bucket must be evicted")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}
