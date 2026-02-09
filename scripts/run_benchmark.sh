#!/usr/bin/env bash
# Shared benchmark lifecycle: start server, health-check, run vitest bench, stop.
#
# Usage:
#   run_benchmark.sh <label> <host> <port> <health_path> <results_suffix> <server_cmd...>
#
# Environment:
#   BENCHMARK_DIR          - directory containing benchmark.bench.mjs (required)
#   BENCHMARK_VITEST_FLAGS - extra vitest flags (default: --silent=passed-only)

set -euo pipefail

LABEL="$1"; shift
HOST="$1"; shift
PORT="$1"; shift
HEALTH_PATH="$1"; shift
RESULTS_SUFFIX="$1"; shift
# Remaining args are the server command

BENCHMARK_DIR="${BENCHMARK_DIR:?BENCHMARK_DIR must be set}"
VITEST_FLAGS="${BENCHMARK_VITEST_FLAGS:---silent=passed-only}"

BASE_URL="http://${HOST}:${PORT}"
HEALTH_URL="${BASE_URL}${HEALTH_PATH}"

SERVER_PID=""
cleanup() {
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
    fi
}
trap cleanup EXIT INT TERM

echo ""
echo "=== Benchmark: ${LABEL} ==="

"$@" & SERVER_PID=$!

echo "Waiting for health check at ${HEALTH_URL}..."
HEALTHY=0
for _ in $(seq 1 30); do
    if curl -s "$HEALTH_URL" > /dev/null 2>&1; then
        HEALTHY=1
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Server for ${LABEL} exited before becoming healthy"
        exit 1
    fi
    sleep 1
done
if [ "$HEALTHY" -ne 1 ]; then
    echo "Server for ${LABEL} did not pass health check in time"
    exit 1
fi

echo "Running benchmarks for ${LABEL}..."
cd "$BENCHMARK_DIR" && BENCHMARK_URL="$BASE_URL" npx vitest bench --run $VITEST_FLAGS benchmark.bench.mjs
RESULT=$?

echo "Stopping server for ${LABEL}..."
cleanup

# Wait for port to free up
for _ in $(seq 1 10); do
    if curl -s "$HEALTH_URL" > /dev/null 2>&1; then
        sleep 1
    else
        break
    fi
done

if [ -f "$BENCHMARK_DIR/benchmark-results.json" ]; then
    cp "$BENCHMARK_DIR/benchmark-results.json" "$BENCHMARK_DIR/benchmark-results-${RESULTS_SUFFIX}.json"
    echo "Saved results to $BENCHMARK_DIR/benchmark-results-${RESULTS_SUFFIX}.json"
fi

exit "$RESULT"
