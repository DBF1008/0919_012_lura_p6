# Rate limiting

The `proxy` layer ships with a rate limiting middleware that rejects traffic
before any request is built or forwarded to a backend. Rejected requests get a
`429 Too Many Requests` response with a `Retry-After` header and a `WARNING` log
line.

## Configuration

A `rate_limit` block can be placed both at the endpoint level and at the
backend level of `endpoints[]`:

```json
{
  "endpoint": "/foo",
  "method": "GET",
  "rate_limit": {
    "algorithm": "token_bucket",
    "strategy": "ip",
    "rate": 10,
    "burst": 20
  },
  "backend": [
    {
      "url_pattern": "/foo",
      "rate_limit": {
        "algorithm": "sliding_window",
        "strategy": "endpoint",
        "rate": 100,
        "burst": 100,
        "window": "1s",
        "store": { "type": "memory" }
      }
    }
  ]
}
```

| Field | Description | Default |
| --- | --- | --- |
| `algorithm` | `token_bucket` or `sliding_window` | `token_bucket` |
| `strategy` | Limit dimension: `endpoint`, `ip` or `api_key` | `endpoint` |
| `rate` | Permits granted per second (tokens/s). Required, must be `> 0` | — |
| `burst` | Bucket / window capacity. For the sliding window it is the maximum number of requests inside `window` | `rate` (min 1) |
| `window` | Window size, sliding window only | `1s` |
| `api_key_header` | Header inspected by the `api_key` strategy | `X-API-Key` |
| `store` | State backend, see below | in-memory |

When `rate_limit` is omitted or `rate <= 0`, the middleware is a no-op.

## Algorithms

* **Token bucket** — steady refill at `rate` tokens per second up to `burst`.
  Allows short bursts over the average rate.
* **Sliding window** — keeps the timestamps of the last `burst` accepted
  requests and rejects while all of them fall inside `window`. The window slides
  continuously, avoiding the fixed-window boundary spike.

## Limit dimensions

* `endpoint` — every request to the endpoint/backend shares one bucket.
* `ip` — independent bucket per client IP. The IP is taken from the first
  `X-Forwarded-For` entry, then `X-Real-Ip`, then the remote address.
* `api_key` — independent bucket per value of `api_key_header`. Requests that
  do not carry the header fall back to the client IP so anonymous traffic cannot
  bypass the limit.

## Distributed deployments

The default `memory` store keeps counters in each gateway process, so the
effective limit is multiplied by the number of instances. Set the store to
`redis` to share the counters across every instance (the evaluation is atomic in
a Lua script):

```json
"store": {
  "type": "redis",
  "address": "redis:6379",
  "password": "",
  "db": 0,
  "dial_timeout": "100ms",
  "read_timeout": "100ms",
  "key_prefix": "lura:ratelimit",
  "pool_size": 5,
  "fail_open": true
}
```

* `fail_open: true` (default) lets the request proceed if Redis is unreachable,
  trading enforcement for availability. Set it to `false` to reject with `429`
  while the store is down.
* Timeouts are intentionally small: rate limiting sits on the hot path and must
  never add noticeable latency.
* Custom stores (etcd, memcached, ...) can be plugged in through
  `proxy.SetLimiterBuilder`.

## Response

When a request is rejected:

* the proxy returns a rate limited error (`proxy.NewRateLimitedError`) and logs
  `[RATELIMIT] ... rejected request key=... retry_after=...` at WARNING level;
* the HTTP handlers translate it to `429 Too Many Requests` and set
  `Retry-After` to the number of whole seconds the client should wait.
