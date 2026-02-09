.PHONY: build release lint fmt-check test conformance benchmark benchmark-memory benchmark-file benchmark-node benchmark-node-memory benchmark-node-file node-ref-build integration-test integration-test-sessions integration-test-electric docker docker-up docker-down docs docs-serve clean help dev dev-down dev-ui
.NOTPARALLEL: benchmark benchmark-memory benchmark-file benchmark-node benchmark-node-memory benchmark-node-file

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
	@echo "  benchmark             - Run benchmark-memory then benchmark-file (release)"
	@echo "  benchmark-memory      - Run benchmark suite with in-memory storage"
	@echo "  benchmark-file        - Run benchmark suite with file storage (durable mode)"
	@echo "  benchmark-node        - Run benchmark-node-memory then benchmark-node-file"
	@echo "  benchmark-node-memory - Run benchmark suite against Node reference (memory)"
	@echo "  benchmark-node-file   - Run benchmark suite against Node reference (file)"
	@echo "  integration-test      - Run full stack integration test (Docker)"
	@echo "  integration-test-sessions - Run sessions + DB sync test (Docker)"
	@echo ""
	@echo "Docker:"
	@echo "  docker                - Build Docker image"
	@echo "  docker-up             - Start Docker stack (server + Envoy proxy)"
	@echo "  docker-down           - Stop Docker stack"
	@echo ""
	@echo "Dev Observability:"
	@echo "  dev                   - Start full observability stack (sync + dev profiles)"
	@echo "  dev-down              - Stop the dev observability stack"
	@echo "  dev-ui                - Clone test-ui (once) and run on :3000"
	@echo ""
	@echo "Other:"
	@echo "  docs                  - Build mdbook documentation"
	@echo "  docs-serve            - Serve docs locally with live reload"
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
# Builds and runs the server with --release for accurate performance numbers.
# Runs benchmarks for both storage backends and saves separate result files.
BENCHMARK_DIR := /tmp/benchmark-run
BENCHMARK_VERSION := 0.2.1
BENCHMARK_PORT := 4437
BENCHMARK_FILE_STORAGE_DIR := /tmp/benchmark-file-storage
BENCHMARK_NODE_PORT := 4438
BENCHMARK_NODE_FILE_STORAGE_DIR := /tmp/node-ref-file-store
NODE_REF_DIR := .dev/durable-streams
NODE_REF_SERVER_MODULE := $(NODE_REF_DIR)/packages/server/dist/index.js
NODE_REF_RUNNER := scripts/node_ref_benchmark_server.mjs

$(BENCHMARK_DIR)/node_modules: $(BENCHMARK_DIR)/package.json
	cd $(BENCHMARK_DIR) && npm install

$(BENCHMARK_DIR)/package.json:
	mkdir -p $(BENCHMARK_DIR)
	cd $(BENCHMARK_DIR) && npm init -y && npm install @durable-streams/benchmarks@$(BENCHMARK_VERSION)
	printf 'import { runBenchmarks } from "@durable-streams/benchmarks";\nconst baseUrl = process.env.BENCHMARK_URL;\nif (!baseUrl) throw new Error("BENCHMARK_URL is required");\nrunBenchmarks({ baseUrl, environment: process.env.BENCHMARK_ENV || "local" });\n' > $(BENCHMARK_DIR)/benchmark.bench.mjs

benchmark: benchmark-memory benchmark-file
	@echo ""
	@echo "Benchmark comparison files:"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-memory.json"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-file.json"

benchmark-memory: release $(BENCHMARK_DIR)/node_modules
	@set -e; \
	echo ""; \
	echo "=== Benchmark backend: memory ==="; \
	STORAGE_MODE=memory cargo run --release & SERVER_PID=$$!; \
	echo "Waiting for health check on :$(BENCHMARK_PORT)..."; \
	HEALTHY=0; \
	for i in $$(seq 1 30); do \
		if curl -s http://localhost:$(BENCHMARK_PORT)/healthz > /dev/null 2>&1; then \
			HEALTHY=1; \
			break; \
		fi; \
		if ! kill -0 $$SERVER_PID 2>/dev/null; then \
			echo "Server for memory exited before becoming healthy"; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	if [ $$HEALTHY -ne 1 ]; then \
		echo "Server for memory did not pass health check in time"; \
		kill $$SERVER_PID 2>/dev/null || true; \
		wait $$SERVER_PID 2>/dev/null || true; \
		exit 1; \
	fi; \
	echo "Running benchmarks for memory storage..."; \
	cd $(BENCHMARK_DIR) && BENCHMARK_URL=http://localhost:$(BENCHMARK_PORT) npx vitest bench --reporter=verbose benchmark.bench.mjs; \
	RESULT=$$?; \
	echo "Stopping server for memory..."; \
	kill $$SERVER_PID 2>/dev/null || true; \
	wait $$SERVER_PID 2>/dev/null || true; \
	for i in $$(seq 1 10); do \
		if curl -s http://localhost:$(BENCHMARK_PORT)/healthz > /dev/null 2>&1; then \
			sleep 1; \
		else \
			break; \
		fi; \
	done; \
	if [ -f $(BENCHMARK_DIR)/benchmark-results.json ]; then \
		cp $(BENCHMARK_DIR)/benchmark-results.json $(BENCHMARK_DIR)/benchmark-results-memory.json; \
		echo "Saved memory results to $(BENCHMARK_DIR)/benchmark-results-memory.json"; \
	fi; \
	exit $$RESULT

benchmark-file: release $(BENCHMARK_DIR)/node_modules
	@set -e; \
	echo ""; \
	echo "=== Benchmark backend: file ==="; \
	rm -rf $(BENCHMARK_FILE_STORAGE_DIR); \
	STORAGE_MODE=file-durable STORAGE_DIR=$(BENCHMARK_FILE_STORAGE_DIR) cargo run --release & SERVER_PID=$$!; \
	echo "Waiting for health check on :$(BENCHMARK_PORT)..."; \
	HEALTHY=0; \
	for i in $$(seq 1 30); do \
		if curl -s http://localhost:$(BENCHMARK_PORT)/healthz > /dev/null 2>&1; then \
			HEALTHY=1; \
			break; \
		fi; \
		if ! kill -0 $$SERVER_PID 2>/dev/null; then \
			echo "Server for file exited before becoming healthy"; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	if [ $$HEALTHY -ne 1 ]; then \
		echo "Server for file did not pass health check in time"; \
		kill $$SERVER_PID 2>/dev/null || true; \
		wait $$SERVER_PID 2>/dev/null || true; \
		exit 1; \
	fi; \
	echo "Running benchmarks for file storage..."; \
	cd $(BENCHMARK_DIR) && BENCHMARK_URL=http://localhost:$(BENCHMARK_PORT) npx vitest bench --reporter=verbose benchmark.bench.mjs; \
	RESULT=$$?; \
	echo "Stopping server for file..."; \
	kill $$SERVER_PID 2>/dev/null || true; \
	wait $$SERVER_PID 2>/dev/null || true; \
	for i in $$(seq 1 10); do \
		if curl -s http://localhost:$(BENCHMARK_PORT)/healthz > /dev/null 2>&1; then \
			sleep 1; \
		else \
			break; \
		fi; \
	done; \
	if [ -f $(BENCHMARK_DIR)/benchmark-results.json ]; then \
		cp $(BENCHMARK_DIR)/benchmark-results.json $(BENCHMARK_DIR)/benchmark-results-file.json; \
		echo "Saved file results to $(BENCHMARK_DIR)/benchmark-results-file.json"; \
	fi; \
	exit $$RESULT

benchmark-node: benchmark-node-memory benchmark-node-file
	@echo ""
	@echo "Node benchmark comparison files:"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-node-memory.json"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-node-file.json"

node-ref-build:
	@test -d $(NODE_REF_DIR) || (echo "Missing $(NODE_REF_DIR). Run 'make dev-ui' first." && exit 1)
	@cd $(NODE_REF_DIR) && pnpm --filter @durable-streams/server build

benchmark-node-memory: node-ref-build $(BENCHMARK_DIR)/node_modules
	@set -e; \
	echo ""; \
	echo "=== Node reference benchmark backend: memory ==="; \
	NODE_REF_SERVER_MODULE=$(NODE_REF_SERVER_MODULE) MODE=memory HOST=127.0.0.1 PORT=$(BENCHMARK_NODE_PORT) node $(NODE_REF_RUNNER) & SERVER_PID=$$!; \
	READY=0; \
	for i in $$(seq 1 30); do \
		if curl -s http://127.0.0.1:$(BENCHMARK_NODE_PORT)/ > /dev/null 2>&1; then \
			READY=1; \
			break; \
		fi; \
		if ! kill -0 $$SERVER_PID 2>/dev/null; then \
			echo "Node reference server (memory) exited before becoming ready"; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	if [ $$READY -ne 1 ]; then \
		echo "Node reference server (memory) did not become reachable in time"; \
		kill $$SERVER_PID 2>/dev/null || true; \
		wait $$SERVER_PID 2>/dev/null || true; \
		exit 1; \
	fi; \
	cd $(BENCHMARK_DIR) && BENCHMARK_URL=http://127.0.0.1:$(BENCHMARK_NODE_PORT) npx vitest bench --reporter=verbose benchmark.bench.mjs; \
	RESULT=$$?; \
	kill $$SERVER_PID 2>/dev/null || true; \
	wait $$SERVER_PID 2>/dev/null || true; \
	if [ -f $(BENCHMARK_DIR)/benchmark-results.json ]; then \
		cp $(BENCHMARK_DIR)/benchmark-results.json $(BENCHMARK_DIR)/benchmark-results-node-memory.json; \
		echo "Saved node-memory results to $(BENCHMARK_DIR)/benchmark-results-node-memory.json"; \
	fi; \
	exit $$RESULT

benchmark-node-file: node-ref-build $(BENCHMARK_DIR)/node_modules
	@set -e; \
	echo ""; \
	echo "=== Node reference benchmark backend: file ==="; \
	rm -rf $(BENCHMARK_NODE_FILE_STORAGE_DIR); \
	NODE_REF_SERVER_MODULE=$(NODE_REF_SERVER_MODULE) MODE=file DATA_DIR=$(BENCHMARK_NODE_FILE_STORAGE_DIR) HOST=127.0.0.1 PORT=$(BENCHMARK_NODE_PORT) node $(NODE_REF_RUNNER) & SERVER_PID=$$!; \
	READY=0; \
	for i in $$(seq 1 30); do \
		if curl -s http://127.0.0.1:$(BENCHMARK_NODE_PORT)/ > /dev/null 2>&1; then \
			READY=1; \
			break; \
		fi; \
		if ! kill -0 $$SERVER_PID 2>/dev/null; then \
			echo "Node reference server (file) exited before becoming ready"; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	if [ $$READY -ne 1 ]; then \
		echo "Node reference server (file) did not become reachable in time"; \
		kill $$SERVER_PID 2>/dev/null || true; \
		wait $$SERVER_PID 2>/dev/null || true; \
		exit 1; \
	fi; \
	cd $(BENCHMARK_DIR) && BENCHMARK_URL=http://127.0.0.1:$(BENCHMARK_NODE_PORT) npx vitest bench --reporter=verbose benchmark.bench.mjs; \
	RESULT=$$?; \
	kill $$SERVER_PID 2>/dev/null || true; \
	wait $$SERVER_PID 2>/dev/null || true; \
	if [ -f $(BENCHMARK_DIR)/benchmark-results.json ]; then \
		cp $(BENCHMARK_DIR)/benchmark-results.json $(BENCHMARK_DIR)/benchmark-results-node-file.json; \
		echo "Saved node-file results to $(BENCHMARK_DIR)/benchmark-results-node-file.json"; \
	fi; \
	exit $$RESULT

# End-to-end integration tests against the Docker stack (server + Envoy JWT proxy)
integration-test: docker
	@echo "Starting Docker stack..."
	@docker-compose up -d; \
	echo "Waiting for health check..."; \
	for i in $$(seq 1 30); do \
	  curl -s http://localhost:8080/healthz > /dev/null 2>&1 && break; sleep 1; \
	done; \
	echo "Running integration tests..."; \
	cd e2e && npm install && npx vitest run --reporter=verbose integration.test.mjs; \
	RESULT=$$?; \
	echo "Stopping Docker stack..."; \
	docker-compose down; \
	exit $$RESULT

integration-test-sessions:
	@echo "Starting sessions + sync stack..."
	@docker-compose --profile sync up -d --build; \
	echo "Waiting for Envoy health check..."; \
	for i in $$(seq 1 60); do \
	  curl -s http://localhost:8080/healthz > /dev/null 2>&1 && break; sleep 1; \
	done; \
	echo "Waiting for sync service readiness..."; \
	for i in $$(seq 1 30); do \
	  docker-compose logs sync-service 2>&1 | grep -q "Sync service ready" && break; sleep 1; \
	done; \
	echo "Running sessions integration tests..."; \
	cd e2e && npm install && npx vitest run --reporter=verbose sessions.test.mjs; \
	RESULT=$$?; \
	echo "Stopping stack..."; \
	docker-compose --profile sync down; \
	exit $$RESULT

integration-test-electric:
	@echo "Replaced by 'make integration-test-sessions'. See docs/decisions.md."
	@echo "Run: make integration-test-sessions"

# Dev observability stack
DEV_UI_DIR := .dev/durable-streams

dev: docker
	@echo "Starting dev observability stack (sync + dev profiles)..."
	@docker-compose --profile sync --profile dev up -d --build
	@echo ""
	@echo "Dev stack running. Port map:"
	@echo "  4437  - DS server (direct, no auth)"
	@echo "  8080  - Envoy proxy (JWT auth)"
	@echo "  9901  - Envoy admin dashboard"
	@echo "  54321 - Postgres direct access"
	@echo "  8081  - Adminer (DB admin UI)"
	@echo ""
	@echo "Next steps:"
	@echo "  make dev-ui           - Start test-ui on :3000 (separate terminal)"
	@echo "  docker-compose logs -f producer  - Watch heartbeat producer"
	@echo "  http://localhost:8081  - Open Adminer (server: postgres, user: postgres, pw: password)"
	@echo "  make dev-down         - Stop everything"

dev-down:
	docker-compose --profile sync --profile dev down

dev-ui:
	@if [ ! -d "$(DEV_UI_DIR)" ]; then \
		echo "Cloning durable-streams monorepo (shallow)..."; \
		mkdir -p .dev; \
		git clone --depth 1 https://github.com/durable-streams/durable-streams.git $(DEV_UI_DIR); \
		echo "Installing dependencies..."; \
		cd $(DEV_UI_DIR) && pnpm install; \
		echo "Building workspace packages (client, state)..."; \
		cd $(DEV_UI_DIR) && pnpm --filter @durable-streams/client build && pnpm --filter @durable-streams/state build; \
	fi
	@curl -sf http://localhost:4437/healthz > /dev/null 2>&1 || \
		(echo "ERROR: DS server not reachable on :4437. Run 'make dev' first." && exit 1)
	@echo "Starting test-ui on http://localhost:3000..."
	@echo "Connect to DS server at http://localhost:4437"
	cd $(DEV_UI_DIR)/examples/test-ui && pnpm dev

# Docker targets
docker:
	docker-compose build

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

# Documentation targets
docs:
	mdbook build docs/book

docs-serve:
	mdbook serve docs/book --open

# Clean target
clean:
	cargo clean
	rm -rf target/
