// SPDX-License-Identifier: Apache-2.0

// Command ratelimitdemo starts a small lura gateway exposing two rate limited
// endpoints in front of a local backend. It is used by ../../test.sh for the
// manual end to end checks.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	ginlib "github.com/gin-gonic/gin"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
	gingin "github.com/luraproject/lura/v2/router/gin"
)

func main() {
	backendAddr := flag.String("backend", "127.0.0.1:18080", "backend address")
	gatewayAddr := flag.String("gateway", "127.0.0.1:8080", "gateway address")
	flag.Parse()

	ginlib.SetMode(ginlib.ReleaseMode)
	logger, _ := logging.NewLogger("DEBUG", os.Stdout, "")

	gatewayHost, gatewayPortRaw, err := net.SplitHostPort(*gatewayAddr)
	if err != nil {
		log.Fatal(err)
	}
	gatewayPort, err := strconv.Atoi(gatewayPortRaw)
	if err != nil {
		log.Fatal(err)
	}

	// the upstream service the gateway forwards to
	backendMux := http.NewServeMux()
	backendMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"backend":"ok"}`)
	})
	backend := &http.Server{Addr: *backendAddr, Handler: backendMux}
	go func() {
		logger.Info("backend listening on", *backendAddr)
		if err := backend.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	failOpen := true
	cfg := config.ServiceConfig{
		Version: config.ConfigVersion,
		Name:    "ratelimit demo",
		Port:    gatewayPort,
		Address: gatewayHost,
		Timeout: 3 * time.Second,
		Host:    []string{"http://" + *backendAddr},
		Endpoints: []*config.EndpointConfig{
			{
				Endpoint: "/tb",
				Method:   "GET",
				Backend: []*config.Backend{{
					Host:       []string{"http://" + *backendAddr},
					URLPattern: "/",
				}},
				RateLimit: &config.RateLimitConfig{
					Algorithm: config.RateLimitAlgorithmTokenBucket,
					Strategy:  config.RateLimitStrategyIP,
					Rate:      1,
					Burst:     3,
				},
			},
			{
				Endpoint:      "/sw",
				Method:        "GET",
				HeadersToPass: []string{"X-API-Key"},
				Backend: []*config.Backend{{
					Host:          []string{"http://" + *backendAddr},
					URLPattern:    "/",
					HeadersToPass: []string{"X-API-Key"},
				}},
				RateLimit: &config.RateLimitConfig{
					Algorithm:    config.RateLimitAlgorithmSlidingWindow,
					Strategy:     config.RateLimitStrategyAPIKey,
					Rate:         0.2,
					Burst:        2,
					Window:       10 * time.Second,
					APIKeyHeader: "X-API-Key",
					Store: config.RateLimitStoreConfig{
						Type:     config.RateLimitStoreMemory,
						FailOpen: &failOpen,
					},
				},
			},
		},
	}
	if err := cfg.Init(); err != nil {
		log.Fatal(err)
	}

	proxyFactory := proxy.DefaultFactory(logger)

	engine := ginlib.New()
	routerEngine := gingin.NewFactory(gingin.Config{
		Engine:         engine,
		HandlerFactory: gingin.EndpointHandler,
		ProxyFactory:   proxyFactory,
		Logger:         logger,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		logger.Info("gateway listening on", *gatewayAddr)
		routerEngine.NewWithContext(ctx).Run(cfg)
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	backend.Shutdown(shutdownCtx)
	logger.Info("demo stopped")
}
