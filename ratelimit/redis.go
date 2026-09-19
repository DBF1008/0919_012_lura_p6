// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// ErrRedisUnavailable is returned by the redis manager when the store cannot
// be reached.
var ErrRedisUnavailable = errors.New("ratelimit: redis store unavailable")

const (
	redisTokenBucketScript = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl = math.ceil(capacity / rate) + 1

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local last = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  last = now
end

tokens = math.min(capacity, tokens + (now - last) * rate)
local allowed = 0
local retry = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  retry = math.ceil((requested - tokens) / rate)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, ttl)
return {allowed, retry, math.floor(tokens)}`

	redisSlidingWindowScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < capacity then
  redis.call('ZADD', key, now, member)
  redis.call('PEXPIRE', key, window)
  return {1, 0, capacity - count - 1}
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local retry = window
if #oldest >= 2 then
  retry = math.ceil(tonumber(oldest[2]) + window - now)
end
return {0, retry, 0}`
)

type redisConn struct {
	c      net.Conn
	reader *bufio.Reader
}

// redisPool is a minimal channel based pool of RESP connections.
type redisPool struct {
	cfg    Policy
	dial   func(context.Context) (*redisConn, error)
	mu     sync.Mutex
	conns  chan *redisConn
	closed bool
}

func newRedisPool(policy Policy) *redisPool {
	p := &redisPool{
		cfg:   policy,
		conns: make(chan *redisConn, policy.Store.PoolSize),
	}
	p.dial = p.defaultDial
	return p
}

func (p *redisPool) defaultDial(ctx context.Context) (*redisConn, error) {
	address := p.cfg.Store.Address
	if _, _, err := net.SplitHostPort(address); err != nil {
		address = net.JoinHostPort(address, "6379")
	}

	var conn net.Conn
	var err error
	if p.cfg.Dialer != nil {
		conn, err = p.cfg.Dialer(ctx, address)
	} else {
		dialer := &net.Dialer{Timeout: p.cfg.Store.DialTimeout}
		conn, err = dialer.DialContext(ctx, "tcp", address)
	}
	if err != nil {
		return nil, err
	}
	rc := &redisConn{c: conn, reader: bufio.NewReader(conn)}

	if p.cfg.Store.Password != "" {
		if err := rc.command(ctx, p.cfg.Store.ReadTimeout, "AUTH", p.cfg.Store.Password); err != nil {
			conn.Close()
			return nil, err
		}
	}
	if p.cfg.Store.DB > 0 {
		if err := rc.command(ctx, p.cfg.Store.ReadTimeout, "SELECT", strconv.Itoa(p.cfg.Store.DB)); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return rc, nil
}

func (p *redisPool) get(ctx context.Context) (*redisConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrRedisUnavailable
	}
	p.mu.Unlock()

	select {
	case rc := <-p.conns:
		if rc != nil {
			return rc, nil
		}
	default:
	}
	return p.dial(ctx)
}

func (p *redisPool) put(rc *redisConn, broken bool) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		rc.c.Close()
		return
	}
	p.mu.Unlock()

	if broken {
		rc.c.Close()
		return
	}
	select {
	case p.conns <- rc:
	default:
		rc.c.Close()
	}
}

func (p *redisPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true

	// drain the idle connections; in-flight connections are closed by put()
	for {
		select {
		case rc := <-p.conns:
			rc.c.Close()
		default:
			p.mu.Unlock()
			return
		}
	}
}

// eval runs a Lua script and returns the integer elements of its reply.
func (p *redisPool) eval(ctx context.Context, script string, keys []string, args ...string) ([]int64, error) {
	rc, err := p.get(ctx)
	if err != nil {
		return nil, err
	}

	cmdArgs := append([]string{script}, strconv.Itoa(len(keys)))
	cmdArgs = append(cmdArgs, keys...)
	cmdArgs = append(cmdArgs, args...)

	reply, err := rc.eval(ctx, p.cfg.Store.ReadTimeout, append([]string{"EVAL"}, cmdArgs...)...)
	broken := err != nil
	p.put(rc, broken)
	if err != nil {
		return nil, err
	}

	values, ok := reply.([]interface{})
	if !ok {
		return nil, fmt.Errorf("ratelimit: unexpected redis reply type %T", reply)
	}
	result := make([]int64, 0, len(values))
	for _, v := range values {
		n, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("ratelimit: unexpected redis array element %T", v)
		}
		result = append(result, n)
	}
	return result, nil
}

func (rc *redisConn) setDeadline(timeout time.Duration) error {
	if timeout <= 0 {
		return rc.c.SetDeadline(time.Time{})
	}
	return rc.c.SetDeadline(time.Now().Add(timeout))
}

// command sends a simple command expecting a STATUS or INTEGER reply.
func (rc *redisConn) command(_ context.Context, timeout time.Duration, args ...string) error {
	if err := rc.setDeadline(timeout); err != nil {
		return err
	}
	if err := writeCommand(rc.c, args); err != nil {
		return err
	}
	_, err := readReply(rc.reader)
	return err
}

func (rc *redisConn) eval(_ context.Context, timeout time.Duration, args ...string) (interface{}, error) {
	if err := rc.setDeadline(timeout); err != nil {
		return nil, err
	}
	if err := writeCommand(rc.c, args); err != nil {
		return nil, err
	}
	return readReply(rc.reader)
}

func writeCommand(w io.Writer, args []string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return nil
}

func readReply(r *bufio.Reader) (interface{}, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, errors.New("ratelimit: malformed redis reply")
	}
	typ := line[0]
	payload := line[1 : len(line)-2]

	switch typ {
	case '+':
		if string(payload) != "OK" {
			return nil, fmt.Errorf("ratelimit: redis status error: %s", payload)
		}
		return string(payload), nil
	case '-':
		return nil, fmt.Errorf("ratelimit: redis error: %s", payload)
	case ':':
		return strconv.ParseInt(string(payload), 10, 64)
	case '$':
		n, err := strconv.Atoi(string(payload))
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(string(payload))
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		values := make([]interface{}, n)
		for i := 0; i < n; i++ {
			if values[i], err = readReply(r); err != nil {
				return nil, err
			}
		}
		return values, nil
	default:
		return nil, fmt.Errorf("ratelimit: unknown redis reply type %q", typ)
	}
}
