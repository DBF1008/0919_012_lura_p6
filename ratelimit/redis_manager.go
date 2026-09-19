// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"
)

// RedisManager evaluates every bucket through Lua scripts executed against a
// Redis server, so all the gateway instances share the same rate limiting
// counters.
type RedisManager struct {
	policy Policy
	pool   *redisPool
	now    func() time.Time
	seq    uint64
}

// NewRedisManager builds a manager backed by the Redis server defined in the
// policy. Connections are opened lazily on the first evaluation.
func NewRedisManager(policy Policy) *RedisManager {
	return &RedisManager{
		policy: policy,
		pool:   newRedisPool(policy),
		now:    time.Now,
	}
}

// Allow implements the Manager interface.
func (m *RedisManager) Allow(ctx context.Context, scope, key string) (Decision, error) {
	fullKey := m.policy.Store.KeyPrefix + ":" + scopeKey(scope, key)
	now := m.now()

	var values []int64
	var err error
	if m.policy.Algorithm == "sliding_window" {
		member := strconv.FormatInt(now.UnixNano(), 10) + "-" +
			strconv.FormatUint(atomic.AddUint64(&m.seq, 1), 10)
		values, err = m.pool.eval(ctx, redisSlidingWindowScript,
			[]string{fullKey},
			strconv.Itoa(m.policy.Burst),
			strconv.FormatInt(m.policy.Window.Milliseconds(), 10),
			strconv.FormatInt(now.UnixNano()/int64(time.Millisecond), 10),
			member,
		)
	} else {
		values, err = m.pool.eval(ctx, redisTokenBucketScript,
			[]string{fullKey},
			strconv.FormatFloat(m.policy.Rate, 'f', -1, 64),
			strconv.Itoa(m.policy.Burst),
			strconv.FormatFloat(float64(now.UnixNano())/float64(time.Second), 'f', 4, 64),
			"1",
		)
	}
	if err != nil {
		return Decision{}, ErrRedisUnavailable
	}
	if len(values) != 3 {
		return Decision{}, ErrRedisUnavailable
	}

	allowed := values[0] == 1
	retryAfter := time.Duration(values[1]) * time.Second
	remaining := int(values[2])
	if !allowed && retryAfter < time.Second {
		retryAfter = time.Second
	}
	return Decision{
		Allowed:    allowed,
		RetryAfter: retryAfter,
		Remaining:  remaining,
		Limit:      m.policy.Burst,
	}, nil
}

// Close implements the Manager interface and releases the connection pool.
func (m *RedisManager) Close() error {
	m.pool.close()
	return nil
}
