.PHONY: build release lint fmt-check test conformance integration-test integration-test-electric docker docs clean help

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
	@echo "  integration-test      - Run full stack integration test (Docker)"
	@echo "  integration-test-electric - Run Electric integration test (Docker)"
	@echo ""
	@echo "Other:"
	@echo "  docker                - Build Docker image"
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
# Requires: npm, @durable-streams/server-conformance-tests
conformance: build
	@echo "Starting server on port 4437..."
	@cargo run & SERVER_PID=$$!; \
	sleep 2; \
	echo "Waiting for health check..."; \
	timeout 10 sh -c 'until curl -s http://localhost:4437/healthz > /dev/null; do sleep 0.5; done' || (kill $$SERVER_PID 2>/dev/null; echo "Server failed to start"; exit 1); \
	echo "Running conformance tests..."; \
	npx @durable-streams/server-conformance-tests --run http://localhost:4437/v1/stream; \
	RESULT=$$?; \
	echo "Stopping server..."; \
	kill $$SERVER_PID 2>/dev/null || true; \
	wait $$SERVER_PID 2>/dev/null || true; \
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

# Docker target
docker:
	@echo "Docker build not yet implemented"
	@echo "Will build multi-stage Dockerfile"
	@exit 1

# Documentation target
docs:
	@echo "Documentation build not yet implemented"
	@echo "Will run: mdbook build"
	@exit 1

# Clean target
clean:
	cargo clean
	rm -rf target/
