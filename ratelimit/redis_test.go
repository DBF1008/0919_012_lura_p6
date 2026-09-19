// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
)

// fakeRedis is a tiny RESP server able to reply to AUTH/SELECT and to emulate
// the two rate limiting Lua scripts, so the distributed path can be tested
// without a real Redis server.
type fakeRedis struct {
	address string

	mu          sync.Mutex
	buckets     map[string]*fakeBucket
	windows     map[string][]float64
	evalCounter int
	failMode    bool
	wg          sync.WaitGroup
	conns       []net.Conn
}

type fakeBucket struct {
	tokens float64
	ts     float64
}

func startFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()
	s := &fakeRedis{
		address: "fake-redis",
		buckets: map[string]*fakeBucket{},
		windows: map[string][]float64{},
	}
	return s
}

func (s *fakeRedis) addr() string { return s.address }

func (s *fakeRedis) dialer() func(context.Context, string) (net.Conn, error) {
	return func(_ context.Context, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		s.mu.Lock()
		s.conns = append(s.conns, server)
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(server)
		}()
		return client, nil
	}
}

func (s *fakeRedis) close() {
	s.mu.Lock()
	for _, c := range s.conns {
		c.Close()
	}
	s.conns = nil
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *fakeRedis) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := readCommand(reader)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		switch strings.ToUpper(args[0]) {
		case "AUTH", "SELECT":
			fmt.Fprint(conn, "+OK\r\n")
		case "EVAL":
			fmt.Fprint(conn, s.runScript(args))
		default:
			fmt.Fprint(conn, "-ERR unknown command\r\n")
		}
	}
}

// readCommand parses a RESP inline command array.
func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("unexpected line %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		header, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		header = strings.TrimSpace(header)
		if len(header) == 0 || header[0] != '$' {
			return nil, fmt.Errorf("unexpected bulk header %q", header)
		}
		size, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (s *fakeRedis) runScript(args []string) string {
	s.mu.Lock()
	failed := s.failMode
	s.mu.Unlock()
	if failed {
		return "-ERR simulated failure\r\n"
	}

	// args: script, numkeys, key, ...params
	numKeys, _ := strconv.Atoi(args[2])
	key := args[3]
	params := args[3+numKeys:]

	s.evalCounter++

	// detect the script by its first non-space token ("local key")
	if strings.Contains(args[1], "HMGET") {
		return s.runTokenBucket(key, params)
	}
	return s.runSlidingWindow(key, params)
}

func (s *fakeRedis) runTokenBucket(key string, params []string) string {
	rate, _ := strconv.ParseFloat(params[0], 64)
	capacity, _ := strconv.ParseFloat(params[1], 64)
	now, _ := strconv.ParseFloat(params[2], 64)

	b, ok := s.buckets[key]
	if !ok {
		b = &fakeBucket{tokens: capacity, ts: now}
		s.buckets[key] = b
	}
	b.tokens = minFloat(capacity, b.tokens+(now-b.ts)*rate)
	b.ts = now

	allowed, retry := int64(1), int64(0)
	if b.tokens < 1 {
		allowed = 0
		retry = int64((1 - b.tokens) / rate)
		if retry < 1 {
			retry = 1
		}
	} else {
		b.tokens--
	}
	return formatArray(allowed, retry, int64(b.tokens))
}

func (s *fakeRedis) runSlidingWindow(key string, params []string) string {
	capacity, _ := strconv.ParseFloat(params[0], 64)
	window, _ := strconv.ParseFloat(params[1], 64)
	now, _ := strconv.ParseFloat(params[2], 64)

	kept := s.windows[key][:0]
	for _, ts := range s.windows[key] {
		if ts > now-window {
			kept = append(kept, ts)
		}
	}
	s.windows[key] = kept

	if float64(len(kept)) < capacity {
		s.windows[key] = append(kept, now)
		return formatArray(1, 0, int64(capacity)-int64(len(kept)))
	}
	oldest := kept[0]
	retry := int64(oldest + window - now)
	if retry < 1 {
		retry = 1
	}
	return formatArray(0, retry, 0)
}

func formatArray(values ...int64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(values))
	for _, v := range values {
		fmt.Fprintf(&b, ":%d\r\n", v)
	}
	return b.String()
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func redisTestPolicy(t *testing.T, srv *fakeRedis, algorithm string) Policy {
	t.Helper()
	cfg := &config.RateLimitConfig{
		Algorithm: algorithm,
		Strategy:  config.RateLimitStrategyEndpoint,
		Rate:      1,
		Burst:     1,
		Window:    time.Second,
		Store: config.RateLimitStoreConfig{
			Type:        config.RateLimitStoreRedis,
			Address:     srv.addr(),
			DialTimeout: time.Second,
			ReadTimeout: time.Second,
			PoolSize:    2,
		},
	}
	p, err := Parse(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.Dialer = srv.dialer()
	return p
}

func TestRedisManagerTokenBucket(t *testing.T) {
	srv := startFakeRedis(t)
	defer srv.close()

	m := NewRedisManager(redisTestPolicy(t, srv, config.RateLimitAlgorithmTokenBucket))
	defer m.Close()
	ctx := context.Background()

	d1, err := m.Allow(ctx, "ep", "endpoint")
	if err != nil || !d1.Allowed {
		t.Fatalf("first allow: %+v %v", d1, err)
	}
	d2, err := m.Allow(ctx, "ep", "endpoint")
	if err != nil || d2.Allowed {
		t.Fatalf("second allow must be rejected: %+v %v", d2, err)
	}
	if d2.RetryAfter < time.Second {
		t.Errorf("retry-after rounded to seconds: %s", d2.RetryAfter)
	}
}

func TestRedisManagerSlidingWindow(t *testing.T) {
	srv := startFakeRedis(t)
	defer srv.close()

	m := NewRedisManager(redisTestPolicy(t, srv, config.RateLimitAlgorithmSlidingWindow))
	defer m.Close()
	ctx := context.Background()

	if d, err := m.Allow(ctx, "ep", "a"); err != nil || !d.Allowed {
		t.Fatalf("first: %+v %v", d, err)
	}
	if d, err := m.Allow(ctx, "ep", "a"); err != nil || d.Allowed {
		t.Fatalf("second must reject: %+v %v", d, err)
	}
	if d, err := m.Allow(ctx, "ep", "b"); err != nil || !d.Allowed {
		t.Fatalf("independent key: %+v %v", d, err)
	}
}

func TestRedisManagerSharedStateAcrossManagers(t *testing.T) {
	srv := startFakeRedis(t)
	defer srv.close()

	policy := redisTestPolicy(t, srv, config.RateLimitAlgorithmTokenBucket)
	m1 := NewRedisManager(policy)
	m2 := NewRedisManager(policy)
	defer m1.Close()
	defer m2.Close()

	if d, err := m1.Allow(context.Background(), "ep", "endpoint"); err != nil || !d.Allowed {
		t.Fatalf("m1 allow: %+v %v", d, err)
	}
	if d, err := m2.Allow(context.Background(), "ep", "endpoint"); err != nil || d.Allowed {
		t.Fatalf("m2 must observe the shared counter: %+v %v", d, err)
	}
}

func TestRedisManagerUnavailable(t *testing.T) {
	cfg := &config.RateLimitConfig{
		Rate: 1,
		Store: config.RateLimitStoreConfig{
			Type:        config.RateLimitStoreRedis,
			Address:     "127.0.0.1:0",
			DialTimeout: 50 * time.Millisecond,
			ReadTimeout: 50 * time.Millisecond,
		},
	}
	policy, err := Parse(cfg)
	if err != nil {
		t.Fatal(err)
	}
	m := NewRedisManager(policy)
	defer m.Close()

	if _, err := m.Allow(context.Background(), "ep", "endpoint"); err != ErrRedisUnavailable {
		t.Fatalf("expected ErrRedisUnavailable, got %v", err)
	}
}

func TestRedisManagerFailureMode(t *testing.T) {
	srv := startFakeRedis(t)
	defer srv.close()

	m := NewRedisManager(redisTestPolicy(t, srv, config.RateLimitAlgorithmTokenBucket))
	defer m.Close()

	if _, err := m.Allow(context.Background(), "ep", "endpoint"); err != nil {
		t.Fatal(err)
	}
	srv.failMode = true
	if _, err := m.Allow(context.Background(), "ep", "endpoint"); err != ErrRedisUnavailable {
		t.Fatalf("simulated failure must surface, got %v", err)
	}
}
