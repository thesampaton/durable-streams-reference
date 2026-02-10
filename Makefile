.PHONY: build release release-pgo lint fmt-check test conformance benchmark benchmark-memory benchmark-file benchmark-node benchmark-node-memory benchmark-node-file node-ref-build benchmark-caddy benchmark-caddy-memory benchmark-caddy-file caddy-build pgo-clean pgo-train pgo-train-memory pgo-train-file pgo-merge pgo-verify pgo-build pgo-benchmark pgo-benchmark-memory pgo-benchmark-file integration-test integration-test-sessions integration-test-electric docker docker-up docker-down docs docs-serve clean help dev dev-down dev-ui
.NOTPARALLEL: benchmark benchmark-memory benchmark-file benchmark-node benchmark-node-memory benchmark-node-file benchmark-caddy benchmark-caddy-memory benchmark-caddy-file

# Default target
help:
	@echo "Durable Streams Rust Server - Make Targets"
	@echo ""
	@echo "Build & Run:"
	@echo "  build                 - Build debug binary"
	@echo "  release               - Build release binary"
	@echo "  release-pgo           - Build release binary with profile-use (fails if profile missing/stale)"
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
	@echo "  benchmark-caddy       - Run benchmark-caddy-memory then benchmark-caddy-file"
	@echo "  benchmark-caddy-memory - Run benchmark suite against Caddy plugin (memory)"
	@echo "  benchmark-caddy-file  - Run benchmark suite against Caddy plugin (file)"
	@echo "  pgo-train             - Generate fresh PGO profiles using memory+file benchmarks"
	@echo "  pgo-merge             - Merge raw PGO profiles into a .profdata file"
	@echo "  pgo-verify            - Verify merged profile exists and is fresh enough"
	@echo "  pgo-build             - Build release binary with merged PGO profile"
	@echo "  pgo-benchmark         - Benchmark with profile-use build flags enabled"
	@echo "  (Use BENCHMARK_VERBOSE=1 for verbose benchmark output)"
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
CARGO_FEATURES ?=

build:
	cargo build $(CARGO_FEATURES)

release:
	cargo build --release $(CARGO_FEATURES)

release-pgo: pgo-verify
	RUSTFLAGS="$(PGO_RUSTFLAGS_USE)" cargo build --release $(CARGO_FEATURES)

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
	@LONG_POLL_TIMEOUT_SECS=2 cargo run $(CARGO_FEATURES) & SERVER_PID=$$!; \
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
BENCHMARK_MAX_MEMORY_BYTES ?= 536870912
BENCHMARK_MAX_STREAM_BYTES ?= 268435456
BENCHMARK_VERBOSE ?= 0
ifeq ($(BENCHMARK_VERBOSE),1)
BENCHMARK_VITEST_FLAGS := --reporter=verbose
else
BENCHMARK_VITEST_FLAGS := --silent=passed-only
endif
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

BENCH_SCRIPT := scripts/run_benchmark.sh
RUST_HOST_TRIPLE := $(shell rustc -vV | sed -n 's/^host: //p')
RUST_SYSROOT := $(shell rustc --print sysroot)
LLVM_BIN_DIR := $(RUST_SYSROOT)/lib/rustlib/$(RUST_HOST_TRIPLE)/bin
LLVM_PROFDATA ?= $(LLVM_BIN_DIR)/llvm-profdata
PGO_DIR ?= /tmp/ds-pgo
PGO_PROFILE_DIR := $(PGO_DIR)/profiles
PGO_PROFILE_DATA := $(PGO_DIR)/merged.profdata
PGO_RUSTFLAGS_GEN := -Cprofile-generate=$(PGO_PROFILE_DIR)
PGO_WARN_MISSING ?= false
PGO_RUSTFLAGS_USE := -Cprofile-use=$(PGO_PROFILE_DATA) -Cllvm-args=-pgo-warn-missing-function=$(PGO_WARN_MISSING)
PGO_REQUIRE_FRESH ?= true
PGO_MAX_AGE_HOURS ?= 168

benchmark: benchmark-memory benchmark-file
	@echo ""
	@echo "Benchmark comparison files:"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-memory.json"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-file.json"

benchmark-memory: release $(BENCHMARK_DIR)/node_modules
	@BENCHMARK_DIR=$(BENCHMARK_DIR) BENCHMARK_VITEST_FLAGS="$(BENCHMARK_VITEST_FLAGS)" \
		$(BENCH_SCRIPT) "memory" localhost $(BENCHMARK_PORT) /healthz memory \
		env PORT=$(BENCHMARK_PORT) MAX_MEMORY_BYTES=$(BENCHMARK_MAX_MEMORY_BYTES) MAX_STREAM_BYTES=$(BENCHMARK_MAX_STREAM_BYTES) STORAGE_MODE=memory cargo run --release $(CARGO_FEATURES)

benchmark-file: release $(BENCHMARK_DIR)/node_modules
	@rm -rf $(BENCHMARK_FILE_STORAGE_DIR)
	@BENCHMARK_DIR=$(BENCHMARK_DIR) BENCHMARK_VITEST_FLAGS="$(BENCHMARK_VITEST_FLAGS)" \
		$(BENCH_SCRIPT) "file (durable)" localhost $(BENCHMARK_PORT) /healthz file \
		env PORT=$(BENCHMARK_PORT) MAX_MEMORY_BYTES=$(BENCHMARK_MAX_MEMORY_BYTES) MAX_STREAM_BYTES=$(BENCHMARK_MAX_STREAM_BYTES) STORAGE_MODE=file-durable DATA_DIR=$(BENCHMARK_FILE_STORAGE_DIR) cargo run --release $(CARGO_FEATURES)

pgo-clean:
	@rm -rf $(PGO_DIR)

pgo-train: pgo-clean pgo-train-memory pgo-train-file pgo-merge
	@echo ""
	@echo "PGO profiles ready:"
	@echo "  raw profiles: $(PGO_PROFILE_DIR)"
	@echo "  merged:       $(PGO_PROFILE_DATA)"

pgo-train-memory: $(BENCHMARK_DIR)/node_modules
	@mkdir -p $(PGO_PROFILE_DIR)
	@RUSTFLAGS="$(PGO_RUSTFLAGS_GEN)" cargo build --release $(CARGO_FEATURES)
	@BENCHMARK_DIR=$(BENCHMARK_DIR) BENCHMARK_VITEST_FLAGS="$(BENCHMARK_VITEST_FLAGS)" \
		$(BENCH_SCRIPT) "memory (pgo-generate)" localhost $(BENCHMARK_PORT) /healthz pgo-train-memory \
		env RUSTFLAGS="$(PGO_RUSTFLAGS_GEN)" PORT=$(BENCHMARK_PORT) MAX_MEMORY_BYTES=$(BENCHMARK_MAX_MEMORY_BYTES) MAX_STREAM_BYTES=$(BENCHMARK_MAX_STREAM_BYTES) STORAGE_MODE=memory cargo run --release $(CARGO_FEATURES)

pgo-train-file: $(BENCHMARK_DIR)/node_modules
	@mkdir -p $(PGO_PROFILE_DIR)
	@rm -rf $(BENCHMARK_FILE_STORAGE_DIR)
	@RUSTFLAGS="$(PGO_RUSTFLAGS_GEN)" cargo build --release $(CARGO_FEATURES)
	@BENCHMARK_DIR=$(BENCHMARK_DIR) BENCHMARK_VITEST_FLAGS="$(BENCHMARK_VITEST_FLAGS)" \
		$(BENCH_SCRIPT) "file (pgo-generate)" localhost $(BENCHMARK_PORT) /healthz pgo-train-file \
		env RUSTFLAGS="$(PGO_RUSTFLAGS_GEN)" PORT=$(BENCHMARK_PORT) MAX_MEMORY_BYTES=$(BENCHMARK_MAX_MEMORY_BYTES) MAX_STREAM_BYTES=$(BENCHMARK_MAX_STREAM_BYTES) STORAGE_MODE=file-durable DATA_DIR=$(BENCHMARK_FILE_STORAGE_DIR) cargo run --release $(CARGO_FEATURES)

pgo-merge:
	@test -x "$(LLVM_PROFDATA)" || (echo "Missing llvm-profdata at $(LLVM_PROFDATA)" && exit 1)
	@test -n "$$(find $(PGO_PROFILE_DIR) -name '*.profraw' -print -quit)" || (echo "No .profraw files found under $(PGO_PROFILE_DIR). Run 'make pgo-train' first." && exit 1)
	@mkdir -p $(PGO_DIR)
	@$(LLVM_PROFDATA) merge -o $(PGO_PROFILE_DATA) $(PGO_PROFILE_DIR)/*.profraw
	@echo "Merged profile data written to $(PGO_PROFILE_DATA)"

pgo-verify:
	@test -f "$(PGO_PROFILE_DATA)" || (echo "Missing merged PGO profile at $(PGO_PROFILE_DATA). Run 'make pgo-train' first." && exit 1)
	@if [ "$(PGO_REQUIRE_FRESH)" = "true" ]; then \
		NOW_EPOCH=$$(date +%s); \
		if stat -f %m "$(PGO_PROFILE_DATA)" >/dev/null 2>&1; then \
			PROFILE_EPOCH=$$(stat -f %m "$(PGO_PROFILE_DATA)"); \
		else \
			PROFILE_EPOCH=$$(stat -c %Y "$(PGO_PROFILE_DATA)"); \
		fi; \
		MAX_AGE_SECS=$$(( $(PGO_MAX_AGE_HOURS) * 3600 )); \
		AGE_SECS=$$(( NOW_EPOCH - PROFILE_EPOCH )); \
		if [ $$AGE_SECS -gt $$MAX_AGE_SECS ]; then \
			echo "PGO profile is stale: $$AGE_SECS seconds old (max $$MAX_AGE_SECS)."; \
			echo "Run 'make pgo-train' to regenerate, or override with PGO_REQUIRE_FRESH=false."; \
			exit 1; \
		fi; \
	fi

pgo-build: pgo-merge
	@RUSTFLAGS="$(PGO_RUSTFLAGS_USE)" cargo build --release $(CARGO_FEATURES)
	@echo "Built release binary with PGO profile-use flags"

pgo-benchmark: pgo-benchmark-memory pgo-benchmark-file
	@echo ""
	@echo "PGO benchmark comparison files:"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-pgo-memory.json"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-pgo-file.json"

pgo-benchmark-memory: pgo-build $(BENCHMARK_DIR)/node_modules
	@BENCHMARK_DIR=$(BENCHMARK_DIR) BENCHMARK_VITEST_FLAGS="$(BENCHMARK_VITEST_FLAGS)" \
		$(BENCH_SCRIPT) "memory (pgo-use)" localhost $(BENCHMARK_PORT) /healthz pgo-memory \
		env RUSTFLAGS="$(PGO_RUSTFLAGS_USE)" PORT=$(BENCHMARK_PORT) MAX_MEMORY_BYTES=$(BENCHMARK_MAX_MEMORY_BYTES) MAX_STREAM_BYTES=$(BENCHMARK_MAX_STREAM_BYTES) STORAGE_MODE=memory cargo run --release $(CARGO_FEATURES)

pgo-benchmark-file: pgo-build $(BENCHMARK_DIR)/node_modules
	@rm -rf $(BENCHMARK_FILE_STORAGE_DIR)
	@BENCHMARK_DIR=$(BENCHMARK_DIR) BENCHMARK_VITEST_FLAGS="$(BENCHMARK_VITEST_FLAGS)" \
		$(BENCH_SCRIPT) "file (pgo-use)" localhost $(BENCHMARK_PORT) /healthz pgo-file \
		env RUSTFLAGS="$(PGO_RUSTFLAGS_USE)" PORT=$(BENCHMARK_PORT) MAX_MEMORY_BYTES=$(BENCHMARK_MAX_MEMORY_BYTES) MAX_STREAM_BYTES=$(BENCHMARK_MAX_STREAM_BYTES) STORAGE_MODE=file-durable DATA_DIR=$(BENCHMARK_FILE_STORAGE_DIR) cargo run --release $(CARGO_FEATURES)

benchmark-node: benchmark-node-memory benchmark-node-file
	@echo ""
	@echo "Node benchmark comparison files:"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-node-memory.json"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-node-file.json"

node-ref-build:
	@test -d $(NODE_REF_DIR) || (echo "Missing $(NODE_REF_DIR). Run 'make dev-ui' first." && exit 1)
	@cd $(NODE_REF_DIR) && pnpm --filter @durable-streams/server build

benchmark-node-memory: node-ref-build $(BENCHMARK_DIR)/node_modules
	@BENCHMARK_DIR=$(BENCHMARK_DIR) BENCHMARK_VITEST_FLAGS="$(BENCHMARK_VITEST_FLAGS)" \
		$(BENCH_SCRIPT) "Node reference (memory)" 127.0.0.1 $(BENCHMARK_NODE_PORT) / node-memory \
		env NODE_REF_SERVER_MODULE=$(NODE_REF_SERVER_MODULE) MODE=memory HOST=127.0.0.1 PORT=$(BENCHMARK_NODE_PORT) node $(NODE_REF_RUNNER)

benchmark-node-file: node-ref-build $(BENCHMARK_DIR)/node_modules
	@rm -rf $(BENCHMARK_NODE_FILE_STORAGE_DIR)
	@BENCHMARK_DIR=$(BENCHMARK_DIR) BENCHMARK_VITEST_FLAGS="$(BENCHMARK_VITEST_FLAGS)" \
		$(BENCH_SCRIPT) "Node reference (file)" 127.0.0.1 $(BENCHMARK_NODE_PORT) / node-file \
		env NODE_REF_SERVER_MODULE=$(NODE_REF_SERVER_MODULE) MODE=file DATA_DIR=$(BENCHMARK_NODE_FILE_STORAGE_DIR) HOST=127.0.0.1 PORT=$(BENCHMARK_NODE_PORT) node $(NODE_REF_RUNNER)

# Caddy plugin benchmark targets
# Requires: go (golang.org/dl)
# Builds the Caddy plugin from .dev/durable-streams/packages/caddy-plugin and benchmarks it.
CADDY_PLUGIN_DIR := $(NODE_REF_DIR)/packages/caddy-plugin
CADDY_BINARY := $(CADDY_PLUGIN_DIR)/durable-streams-server
CADDY_BENCHMARK_PORT := 4439
CADDY_CADDYFILE_DIR := /tmp/caddy-benchmark
CADDY_FILE_STORAGE_DIR := /tmp/caddy-benchmark-file-storage

caddy-build:
	@test -d $(CADDY_PLUGIN_DIR) || (echo "Missing $(CADDY_PLUGIN_DIR). Run 'make dev-ui' first to clone the monorepo." && exit 1)
	@command -v go >/dev/null 2>&1 || (echo "Go is required to build the Caddy plugin. Install from https://go.dev/dl/" && exit 1)
	cd $(CADDY_PLUGIN_DIR) && go build -o durable-streams-server ./cmd/caddy

benchmark-caddy: benchmark-caddy-memory benchmark-caddy-file
	@echo ""
	@echo "Caddy benchmark comparison files:"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-caddy-memory.json"
	@echo "  $(BENCHMARK_DIR)/benchmark-results-caddy-file.json"

benchmark-caddy-memory: caddy-build $(BENCHMARK_DIR)/node_modules
	@mkdir -p $(CADDY_CADDYFILE_DIR)
	@printf '{\n\tadmin off\n\tauto_https off\n}\n\n:%s {\n\troute /v1/stream/* {\n\t\tdurable_streams\n\t}\n}\n' $(CADDY_BENCHMARK_PORT) > $(CADDY_CADDYFILE_DIR)/Caddyfile-memory
	@BENCHMARK_DIR=$(BENCHMARK_DIR) BENCHMARK_VITEST_FLAGS="$(BENCHMARK_VITEST_FLAGS)" \
		$(BENCH_SCRIPT) "Caddy plugin (memory)" localhost $(CADDY_BENCHMARK_PORT) / caddy-memory \
		$(CADDY_BINARY) run --config $(CADDY_CADDYFILE_DIR)/Caddyfile-memory

benchmark-caddy-file: caddy-build $(BENCHMARK_DIR)/node_modules
	@rm -rf $(CADDY_FILE_STORAGE_DIR)
	@mkdir -p $(CADDY_CADDYFILE_DIR) $(CADDY_FILE_STORAGE_DIR)
	@printf '{\n\tadmin off\n\tauto_https off\n}\n\n:%s {\n\troute /v1/stream/* {\n\t\tdurable_streams {\n\t\t\tdata_dir %s\n\t\t}\n\t}\n}\n' $(CADDY_BENCHMARK_PORT) $(CADDY_FILE_STORAGE_DIR) > $(CADDY_CADDYFILE_DIR)/Caddyfile-file
	@BENCHMARK_DIR=$(BENCHMARK_DIR) BENCHMARK_VITEST_FLAGS="$(BENCHMARK_VITEST_FLAGS)" \
		$(BENCH_SCRIPT) "Caddy plugin (file)" localhost $(CADDY_BENCHMARK_PORT) / caddy-file \
		$(CADDY_BINARY) run --config $(CADDY_CADDYFILE_DIR)/Caddyfile-file

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
