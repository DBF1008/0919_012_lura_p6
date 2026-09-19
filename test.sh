#!/usr/bin/env bash
#
# Manual unit-test script for the proxy rate limiting feature.
#
# Usage:
#   ./test.sh
#
# Note: some pre-existing tests in the repo require opening local
# sockets (httptest servers, TLS listeners). If your environment
# restricts binding/connecting to ports, those unrelated tests may
# fail; the rate limiting tests below do not need the network.

set -euo pipefail
cd "$(dirname "$0")"

# use a writable build cache if the default one is not available
if ! go env GOCACHE >/dev/null 2>&1 || [ ! -w "$(go env GOCACHE)" ]; then
	export GOCACHE="${TMPDIR:-/tmp}/gocache"
fi

echo "==> 1/6 Build all packages"
go build ./...

echo "==> 2/6 Vet the touched packages"
go vet ./config/ ./proxy/ ./transport/http/server/

echo "==> 3/6 Rate limit unit tests (proxy)"
go test ./proxy/ -run 'RateLimit|MemoryStore|ClientIP' -v

echo "==> 4/6 Rate limit handler tests (http server)"
go test ./transport/http/server/ -run 'RateLimit' -v

echo "==> 5/6 Config package tests (new rate_limit fields)"
go test ./config/ -v

echo "==> 6/6 Proxy factory/stack regression tests"
go test ./proxy/ -run 'TestDefaultFactory|TestNewDefaultFactory' -v

echo
echo "All rate limiting unit tests passed."
echo
echo "Optional full-package runs (need local network permissions):"
echo "  go test ./proxy/ ./config/ ./transport/http/server/"
