// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"sync"
	"time"
)

// slidingWindow is a thread-safe sliding window log: it keeps the timestamps
// of the last accepted requests and rejects new ones while more than capacity
// timestamps fall inside the window.
type slidingWindow struct {
	mu       sync.Mutex
	entries  []time.Time
	capacity int
	window   time.Duration
}

func newSlidingWindow(capacity int, window time.Duration) *slidingWindow {
	return &slidingWindow{
		entries:  make([]time.Time, 0, capacity+1),
		capacity: capacity,
		window:   window,
	}
}

// take records the current timestamp when there is room. The returned
// retryAfter is the time until the oldest in-window entry expires.
func (w *slidingWindow) take(now time.Time) (allowed bool, retryAfter time.Duration, remaining int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	threshold := now.Add(-w.window)
	drop := 0
	for drop < len(w.entries) && !w.entries[drop].After(threshold) {
		drop++
	}
	if drop > 0 {
		w.entries = w.entries[drop:]
	}

	if len(w.entries) < w.capacity {
		w.entries = append(w.entries, now)
		return true, 0, w.capacity - len(w.entries)
	}

	retryAfter = w.entries[0].Add(w.window).Sub(now)
	return false, retryAfter, 0
}
