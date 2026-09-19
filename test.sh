#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# test.sh - rate limiting feature test runner
#
#   ./test.sh              # run everything: unit tests + manual e2e checks
#   ./test.sh unit         # only the automated unit tests
#   ./test.sh e2e          # only the manual end-to-end checks (needs curl)
#   ./test.sh help         # show the available test groups
#
# The "e2e" group builds a small demo gateway (test/ratelimitdemo), starts it
# locally and drives it with curl, so a human can verify the 200 -> 429 +
# Retry-After behavior of both algorithms and dimensions.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# put the go build cache somewhere writable when the default cache is read-only
export GOCACHE="${GOCACHE:-/tmp/lura-gocache}"
mkdir -p "$GOCACHE"

GATEWAY_HOST="127.0.0.1"
GATEWAY_PORT="${GATEWAY_PORT:-18090}"
BACKEND_PORT="${BACKEND_PORT:-18091}"
GATEWAY_URL="http://${GATEWAY_HOST}:${GATEWAY_PORT}"
DEMO_BIN="$(mktemp -d)/ratelimitdemo"
DEMO_PID=""

red()    { printf '\033[31m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }
bold()   { printf '\033[1m%s\033[0m\n' "$*"; }

fail() { red "FAIL: $*"; exit 1; }

usage() {
	cat <<USAGE
Usage: $0 [unit|e2e|all|help]

  unit   automated unit tests for config, ratelimit, proxy middleware and the
         http transport (go test, no network listeners required)
  e2e    manual end-to-end checks against a real gateway process:
           * token bucket, per client IP   -> GET /tb
           * sliding window, per API key   -> GET /sw (X-API-Key header)
         verifies 200 responses followed by 429 + Retry-After
  all    runs unit tests and then the e2e checks (default)
USAGE
}

# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

run_unit() {
	bold "== [1/2] unit tests =="

	local packages=(
		"./config"
		"./ratelimit"
		"./proxy"
		"./router/mux"
		"./router/gin"
		"./transport/http/server"
	)

	green "-- build & vet --"
	go build ./...
	go vet "${packages[@]}"

	green "-- ratelimit algorithm + store unit tests (race detector) --"
	go test -race -v ./ratelimit \
		-run 'TestParse|TestTokenBucket|TestSlidingWindow|TestIndependentScopes|TestEvictIdle|TestRedisManager'

	green "-- config unit tests --"
	go test -race -v ./config -run RateLimit

	green "-- proxy rate limit middleware unit tests --"
	go test -race -v ./proxy \
		-run 'TestRateLimit|TestClientIP|TestRateLimited'

	green "-- transport / router 429 + Retry-After unit tests --"
	go test -race -v ./transport/http/server -run RateLimit
	go test -race -v ./router/mux -run rateLimited
	go test -race -v ./router/gin -run rateLimited

	green "unit tests finished OK"
}

# ---------------------------------------------------------------------------
# Manual end to end checks
# ---------------------------------------------------------------------------

require_curl() {
	command -v curl >/dev/null 2>&1 || fail "curl is required for the e2e checks"
}

# wait_for_tcp probes the gateway TCP port without consuming rate limit tokens.
wait_for_tcp() {
	local host="$1" port="$2" attempts="${3:-100}"
	for _ in $(seq 1 "$attempts"); do
		if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
			exec 3>&- 3<&-
			return 0
		fi
		sleep 0.1
	done
	return 1
}

start_demo() {
	green "-- building demo gateway --"
	go build -o "$DEMO_BIN" ./test/ratelimitdemo

	green "-- starting demo gateway on ${GATEWAY_URL} --"
	"$DEMO_BIN" \
		-gateway "${GATEWAY_HOST}:${GATEWAY_PORT}" \
		-backend "${GATEWAY_HOST}:${BACKEND_PORT}" &
	DEMO_PID=$!

	wait_for_tcp "$GATEWAY_HOST" "$GATEWAY_PORT" || fail "demo gateway did not start"
	green "gateway is up (pid $DEMO_PID)"
}

stop_demo() {
	if [[ -n "$DEMO_PID" ]]; then
		kill "$DEMO_PID" >/dev/null 2>&1 || true
		wait "$DEMO_PID" 2>/dev/null || true
	fi
}

# do_dump PATH [API-KEY] -> performs the request and dumps the response headers
# on stdout (body discarded)
do_dump() {
	local path="$1" api_key="${2:-}"
	local args=(-s -D - -o /dev/null)
	if [[ -n "$api_key" ]]; then
		args+=(-H "X-API-Key: ${api_key}")
	fi
	curl "${args[@]}" "${GATEWAY_URL}${path}"
}

# do_request PATH [API-KEY] -> prints "<http_code> <retry_after_seconds>"
do_request() {
	do_dump "$@" | awk '
		BEGIN { code = ""; retry = "" }
		tolower($1) == "http/1.1" || tolower($1) == "http/2" { code = $2 }
		tolower($1) == "retry-after:" { retry = $2 }
		END { gsub(/\r/, "", code); gsub(/\r/, "", retry); print code, retry }'
}
run_e2e_token_bucket_per_ip() {
	bold "-- token bucket / per client IP: rate=1rps, burst=3 --"
	local results code
	results=""
	for i in 1 2 3 4 5; do
		code="$(do_request /tb | awk '{print $1}')"
		results+="${code} "
		printf '  request %d -> HTTP %s\n' "$i" "$code"
	done
	echo "$results" | grep -q "200 200 200 429 429" \
		|| fail "expected 200 200 200 429 429, got: $results"
	green "  burst of 3 allowed, following requests rejected with 429"

	local retry
	retry="$(do_request /tb | awk '{print $2}')"
	[[ -n "$retry" && "$retry" =~ ^[0-9]+$ ]] \
		|| fail "Retry-After header missing or invalid: '$retry'"
	green "  Retry-After present: ${retry}s"

	yellow "  waiting 3.5s for token refill..."
	sleep 3.5
	code="$(do_request /tb | awk '{print $1}')"
	[[ "$code" == "200" ]] || fail "request after refill expected 200, got $code"
	green "  request accepted again after refill"
}

run_e2e_sliding_window_per_api_key() {
	bold "-- sliding window / per API key: 2 requests per 10s --"
	local code results
	results=""
	for i in 1 2 3; do
		code="$(do_request /sw "sk-demo-1" | awk '{print $1}')"
		results+="${code} "
		printf '  request %d (api key sk-demo-1) -> HTTP %s\n' "$i" "$code"
	done
	echo "$results" | grep -q "200 200 429" \
		|| fail "expected 200 200 429 for sk-demo-1, got: $results"
	green "  same API key limited to 2 per window"

	code="$(do_request /sw "sk-demo-2" | awk '{print $1}')"
	[[ "$code" == "200" ]] || fail "a different API key must have its own bucket, got $code"
	green "  independent API key sk-demo-2 accepted"

	local retry
	retry="$(do_request /sw "sk-demo-1" | awk '{print $2}')"
	[[ -n "$retry" && "$retry" =~ ^[0-9]+$ ]] \
		|| fail "Retry-After header missing for sliding window: '$retry'"
	green "  sliding window 429 carries Retry-After: ${retry}s"
}

run_e2e() {
	bold "== [2/2] manual end-to-end checks =="
	require_curl
	trap stop_demo EXIT
	start_demo

	run_e2e_token_bucket_per_ip
	run_e2e_sliding_window_per_api_key

	green "all e2e checks passed"
}

# ---------------------------------------------------------------------------

case "${1:-all}" in
	unit) run_unit ;;
	e2e)  run_e2e ;;
	all)  run_unit; run_e2e ;;
	help|-h|--help) usage ;;
	*) usage; exit 1 ;;
esac
