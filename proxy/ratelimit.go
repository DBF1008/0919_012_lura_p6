// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/ratelimit"
)

// RateLimitedError signals that a request was rejected by the rate limiter. It
// carries the suggested Retry-After delay and the dimension key that hit the
// limit, so the transport layer can build the proper 429 response.
type RateLimitedError struct {
	RetryAfter time.Duration
	Key        string
}

// NewRateLimitedError builds a rate limited error.
func NewRateLimitedError(retryAfter time.Duration, key string) *RateLimitedError {
	if retryAfter < time.Second {
		retryAfter = time.Second
	}
	return &RateLimitedError{RetryAfter: retryAfter, Key: key}
}

// Error implements the error interface.
func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("rate limit exceeded (%s)", e.Key)
}

// StatusCode implements the responseError interface used by the routers.
func (e *RateLimitedError) StatusCode() int { return http.StatusTooManyRequests }

// RetryAfterSeconds returns the delay rounded up to whole seconds, as required
// by the Retry-After header when it carries a delta.
func (e *RateLimitedError) RetryAfterSeconds() int {
	return int((e.RetryAfter + time.Second - 1) / time.Second)
}

// Headers implements the headerResponseError interface used by the gin router:
// the Retry-After header is attached before the response is written.
func (e *RateLimitedError) Headers() map[string][]string {
	return map[string][]string{
		"Retry-After": {strconv.Itoa(e.RetryAfterSeconds())},
	}
}

// LimiterBuilder constructs the shared state manager for a given policy. It can
// be replaced to plug custom stores (etcd, memcached, ...).
type LimiterBuilder func(ratelimit.Policy) (ratelimit.Manager, error)

var (
	limiterRegistryMu sync.Mutex
	limiterRegistry   = map[string]*registeredManager{}

	defaultBuilders = map[string]LimiterBuilder{
		config.RateLimitStoreMemory: func(p ratelimit.Policy) (ratelimit.Manager, error) {
			return ratelimit.NewMemoryManager(p), nil
		},
		config.RateLimitStoreRedis: func(p ratelimit.Policy) (ratelimit.Manager, error) {
			return ratelimit.NewRedisManager(p), nil
		},
	}
)

type registeredManager struct {
	manager ratelimit.Manager
	refs    int
}

// SetLimiterBuilder registers a custom manager builder for a store type.
func SetLimiterBuilder(storeType string, builder LimiterBuilder) {
	limiterRegistryMu.Lock()
	defer limiterRegistryMu.Unlock()
	defaultBuilders[storeType] = builder
}

// managerFor parses the configuration, reusing an already built manager when
// the same store/policy was already requested. The returned release function
// decrements the reference counter.
func managerFor(cfg *config.RateLimitConfig) (ratelimit.Manager, ratelimit.Policy, func(), error) {
	policy, err := ratelimit.Parse(cfg)
	if err != nil {
		return nil, policy, func() {}, err
	}

	// managers are shared only by policies with the same algorithm, capacity
	// and store coordinates. Distinct rate limits keep independent buckets.
	fingerprint := policy.Store.Type + "|" + policy.Store.Address + "|" +
		policy.Store.KeyPrefix + "|" + strconv.Itoa(policy.Store.DB) + "|" +
		policy.Algorithm + "|" + strconv.FormatFloat(policy.Rate, 'f', -1, 64) + "|" +
		strconv.Itoa(policy.Burst) + "|" + policy.Window.String()

	limiterRegistryMu.Lock()
	defer limiterRegistryMu.Unlock()

	if rm, ok := limiterRegistry[fingerprint]; ok {
		rm.refs++
		return rm.manager, policy, releaseFunc(fingerprint), nil
	}

	builder, ok := defaultBuilders[policy.Store.Type]
	if !ok {
		return nil, policy, func() {}, ratelimit.ErrUnknownStore
	}
	manager, err := builder(policy)
	if err != nil {
		return nil, policy, func() {}, err
	}
	limiterRegistry[fingerprint] = &registeredManager{manager: manager, refs: 1}
	return manager, policy, releaseFunc(fingerprint), nil
}

func releaseFunc(fingerprint string) func() {
	return func() {
		limiterRegistryMu.Lock()
		defer limiterRegistryMu.Unlock()
		if rm, ok := limiterRegistry[fingerprint]; ok {
			rm.refs--
			if rm.refs <= 0 {
				rm.manager.Close()
				delete(limiterRegistry, fingerprint)
			}
		}
	}
}

// NewRateLimitMiddleware returns a no-op middleware when rate limiting is not
// configured and the enforcing middleware otherwise. The scope identifies the
// endpoint or backend owning the limiter and the logger receives a WARN entry
// every time a request is rejected.
func NewRateLimitMiddleware(logger logging.Logger, scope string, cfg *config.RateLimitConfig) Middleware {
	if !cfg.Enabled() {
		return emptyMiddlewareFallback(logger)
	}

	manager, policy, release, err := managerFor(cfg)
	if err != nil {
		logger.Warning("[RATELIMIT]", scope, "disabled due to a bad configuration:", err.Error())
		return emptyMiddlewareFallback(logger)
	}

	logger.Info(fmt.Sprintf("[RATELIMIT] %s enabled: algorithm=%s strategy=%s rate=%v burst=%d store=%s",
		scope, policy.Algorithm, policy.Strategy, policy.Rate, policy.Burst, policy.Store.Type))

	return func(next ...Proxy) Proxy {
		if len(next) > 1 {
			logger.Fatal("too many proxies for this proxy middleware: NewRateLimitMiddleware only accepts 1 proxy, got %d", len(next))
			release()
			return nil
		}
		nextProxy := next[0]
		return func(ctx context.Context, request *Request) (*Response, error) {
			key := limitKey(policy, request)
			decision, err := manager.Allow(ctx, scope, key)
			if err != nil {
				if policy.FailOpen() {
					logger.Warning("[RATELIMIT]", scope, "store unavailable, failing open:", err.Error())
					return nextProxy(ctx, request)
				}
				logger.Warning("[RATELIMIT]", scope, "store unavailable, failing closed:", err.Error())
				return nil, NewRateLimitedError(time.Second, key)
			}

			if !decision.Allowed {
				logger.Warning(fmt.Sprintf("[RATELIMIT] %s rejected request key=%s retry_after=%s remaining=%d/%d",
					scope, key, decision.RetryAfter, decision.Remaining, decision.Limit))
				return nil, NewRateLimitedError(decision.RetryAfter, key)
			}
			return nextProxy(ctx, request)
		}
	}
}

func limitKey(policy ratelimit.Policy, request *Request) string {
	switch policy.Strategy {
	case config.RateLimitStrategyIP:
		return clientIPFromRequest(request)
	case config.RateLimitStrategyAPIKey:
		if v := firstHeader(request, policy.APIKeyHeader); v != "" {
			return "api:" + v
		}
		// requests without an API key are grouped with their client IP so
		// anonymous traffic cannot bypass the limit.
		return "api:anonymous:" + clientIPFromRequest(request)
	default:
		return config.RateLimitStrategyEndpoint
	}
}

func firstHeader(request *Request, name string) string {
	if request == nil || request.Headers == nil {
		return ""
	}
	for k, values := range request.Headers {
		if strings.EqualFold(k, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

// clientIPFromRequest extracts the client IP honoring the X-Forwarded-For and
// X-Real-Ip headers populated by the router request builders.
func clientIPFromRequest(request *Request) string {
	if xff := firstHeader(request, "X-Forwarded-For"); xff != "" {
		if idx := strings.IndexByte(xff, ','); idx >= 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	if ip := firstHeader(request, "X-Real-Ip"); ip != "" {
		return strings.TrimSpace(ip)
	}
	if request != nil && request.URL != nil {
		if host := request.URL.Host; host != "" {
			if hostOnly, _, err := net.SplitHostPort(host); err == nil {
				return hostOnly
			}
			return host
		}
	}
	return "unknown"
}
