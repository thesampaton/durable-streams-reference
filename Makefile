.PHONY: build release lint fmt-check test conformance benchmark integration-test integration-test-electric docker docker-up docker-down docs clean help

# Default target
help:
	@echo "Durable Streams Rust Server - Make Targets"
	@echo ""
	@echo "Build & Run:"
	@echo "  build                 - Build debug binary"
	@echo "  release               - Build release binary"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint                  - Run clippy with warnings as errors"
	@echo "  fmt-check             - Check code formatting"
	@echo "  fmt                   - Format code"
	@echo ""
	@echo "Testing:"
	@echo "  test                  - Run all tests (unit + integration)"
	@echo "  test-unit             - Run unit tests only"
	@echo "  test-integration      - Run Rust integration tests only"
	@echo "  conformance           - Run external conformance test suite"
	@echo "  benchmark             - Run benchmark suite (release build)"
	@echo "  integration-test      - Run full stack integration test (Docker)"
	@echo "  integration-test-electric - Run Electric integration test (Docker)"
	@echo ""
	@echo "Docker:"
	@echo "  docker                - Build Docker image"
	@echo "  docker-up             - Start Docker stack (server + Envoy proxy)"
	@echo "  docker-down           - Stop Docker stack"
	@echo ""
	@echo "Other:"
	@echo "  docs                  - Build documentation"
	@echo "  clean                 - Clean build artifacts"

# Build targets
build:
	cargo build

release:
	cargo build --release

# Code quality targets
lint:
	cargo clippy -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

# Test targets
test: test-unit test-integration

test-unit:
	cargo test --lib

test-integration:
	cargo test --test '*'

# Conformance test target
# Requires: node, npm
# On first run, installs the conformance harness to /tmp/conformance-run
CONFORMANCE_DIR := /tmp/conformance-run
CONFORMANCE_VERSION := 0.2.1

$(CONFORMANCE_DIR)/node_modules: $(CONFORMANCE_DIR)/package.json
	cd $(CONFORMANCE_DIR) && npm install

$(CONFORMANCE_DIR)/package.json:
	mkdir -p $(CONFORMANCE_DIR)
	cd $(CONFORMANCE_DIR) && npm init -y && npm install @durable-streams/server-conformance-tests@$(CONFORMANCE_VERSION)
	printf 'import { runConformanceTests } from "@durable-streams/server-conformance-tests";\nconst baseUrl = process.env.CONFORMANCE_TEST_URL;\nif (!baseUrl) throw new Error("CONFORMANCE_TEST_URL is required");\nrunConformanceTests({ baseUrl, longPollTimeoutMs: 2000 });\n' > $(CONFORMANCE_DIR)/conformance.test.mjs

conformance: build $(CONFORMANCE_DIR)/node_modules
	@echo "Starting server on port 4437..."
	@LONG_POLL_TIMEOUT_SECS=2 cargo run & SERVER_PID=$$!; \
	echo "Waiting for health check..."; \
	for i in $$(seq 1 30); do curl -s http://localhost:4437/healthz > /dev/null 2>&1 && break; sleep 1; done; \
	echo "Running conformance tests..."; \
	cd $(CONFORMANCE_DIR) && CONFORMANCE_TEST_URL=http://localhost:4437 npx vitest run --reporter=verbose conformance.test.mjs; \
	RESULT=$$?; \
	echo "Stopping server..."; \
	kill $$SERVER_PID 2>/dev/null || true; \
	wait $$SERVER_PID 2>/dev/null || true; \
	exit $$RESULT

# Benchmark target
# Requires: node, npm
# On first run, installs the benchmark harness to /tmp/benchmark-run
# Builds and runs the server with --release for accurate performance numbers
BENCHMARK_DIR := /tmp/benchmark-run
BENCHMARK_VERSION := 0.2.1

$(BENCHMARK_DIR)/node_modules: $(BENCHMARK_DIR)/package.json
	cd $(BENCHMARK_DIR) && npm install

$(BENCHMARK_DIR)/package.json:
	mkdir -p $(BENCHMARK_DIR)
	cd $(BENCHMARK_DIR) && npm init -y && npm install @durable-streams/benchmarks@$(BENCHMARK_VERSION)
	printf 'import { runBenchmarks } from "@durable-streams/benchmarks";\nconst baseUrl = process.env.BENCHMARK_URL;\nif (!baseUrl) throw new Error("BENCHMARK_URL is required");\nrunBenchmarks({ baseUrl, environment: process.env.BENCHMARK_ENV || "local" });\n' > $(BENCHMARK_DIR)/benchmark.bench.mjs

benchmark: release $(BENCHMARK_DIR)/node_modules
	@echo "Starting server (release build) on port 4437..."
	@cargo run --release & SERVER_PID=$$!; \
	echo "Waiting for health check..."; \
	for i in $$(seq 1 30); do curl -s http://localhost:4437/healthz > /dev/null 2>&1 && break; sleep 1; done; \
	echo "Running benchmarks..."; \
	cd $(BENCHMARK_DIR) && BENCHMARK_URL=http://localhost:4437 npx vitest bench --reporter=verbose benchmark.bench.mjs; \
	RESULT=$$?; \
	echo "Stopping server..."; \
	kill $$SERVER_PID 2>/dev/null || true; \
	wait $$SERVER_PID 2>/dev/null || true; \
	if [ -f $(BENCHMARK_DIR)/benchmark-results.json ]; then \
		echo ""; \
		echo "Results saved to $(BENCHMARK_DIR)/benchmark-results.json"; \
	fi; \
	exit $$RESULT

# Docker integration test targets (to be implemented)
integration-test:
	@echo "Integration tests not yet implemented"
	@echo "Will run: docker compose up + authenticated scenario tests"
	@exit 1

integration-test-electric:
	@echo "Electric integration tests not yet implemented"
	@echo "Will run: docker compose --profile electric up + postgres insertion tests"
	@exit 1

# Docker targets
docker:
	docker-compose build

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

# Documentation target
docs:
	@echo "Documentation build not yet implemented"
	@echo "Will run: mdbook build"
	@exit 1

# Clean target
clean:
	cargo clean
	rm -rf target/
