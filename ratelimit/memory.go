// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"context"
	"sync"
	"time"
)

// bucket is the common interface of the in-memory algorithms.
type bucket interface {
	take(now time.Time) (allowed bool, retryAfter time.Duration, remaining int)
}

type bucketEntry struct {
	bucket   bucket
	lastUsed time.Time
}

// MemoryManager keeps the rate limiting state in the local process. Each
// scope/key pair owns an independent bucket and unused buckets are evicted by a
// background janitor to bound memory growth.
type MemoryManager struct {
	policy Policy
	now    func() time.Time

	mu      sync.RWMutex
	buckets map[string]*bucketEntry

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// NewMemoryManager builds an in-memory manager for the received policy.
func NewMemoryManager(policy Policy) *MemoryManager {
	m := &MemoryManager{
		policy:  policy,
		now:     time.Now,
		buckets: map[string]*bucketEntry{},
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go m.cleanup()
	return m
}

// Allow implements the Manager interface.
func (m *MemoryManager) Allow(_ context.Context, scope, key string) (Decision, error) {
	now := m.now()
	b := m.getBucket(scopeKey(scope, key), now)

	allowed, retryAfter, remaining := b.take(now)
	if !allowed && retryAfter < time.Second {
		// Retry-After is expressed in whole seconds, never 0 for a rejection
		retryAfter = time.Second
	}
	return Decision{
		Allowed:    allowed,
		RetryAfter: retryAfter,
		Remaining:  remaining,
		Limit:      m.policy.Burst,
	}, nil
}

func (m *MemoryManager) getBucket(key string, now time.Time) bucket {
	m.mu.RLock()
	entry, ok := m.buckets[key]
	m.mu.RUnlock()
	if ok {
		entry.lastUsed = now
		return entry.bucket
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.buckets[key]; ok {
		entry.lastUsed = now
		return entry.bucket
	}

	var b bucket
	if m.policy.Algorithm == "sliding_window" {
		b = newSlidingWindow(m.policy.Burst, m.policy.Window)
	} else {
		b = newTokenBucket(m.policy.Rate, m.policy.Burst, now)
	}
	m.buckets[key] = &bucketEntry{bucket: b, lastUsed: now}
	return b
}

func (m *MemoryManager) cleanup() {
	defer close(m.done)
	ticker := time.NewTicker(defaultCleanupInterval)
	defer ticker.Stop()

	idleTimeout := 10 * defaultCleanupInterval
	if m.policy.Window > idleTimeout {
		idleTimeout = 10 * m.policy.Window
	}

	for {
		select {
		case <-m.stop:
			return
		case now := <-ticker.C:
			m.evict(now.Add(-idleTimeout))
		}
	}
}

func (m *MemoryManager) evict(threshold time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, entry := range m.buckets {
		if entry.lastUsed.Before(threshold) {
			delete(m.buckets, key)
		}
	}
}

// Close implements the Manager interface and stops the janitor goroutine.
func (m *MemoryManager) Close() error {
	m.stopOnce.Do(func() { close(m.stop) })
	<-m.done
	return nil
}
