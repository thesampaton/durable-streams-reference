# Stage 1: Build
FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build

# Cache dependency build layer
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs \
    && cargo build --release \
    && rm -rf src

# Build actual binary
COPY src/ src/
RUN touch src/main.rs src/lib.rs && cargo build --release

# Stage 2: Runtime
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/target/release/durable-streams-server /durable-streams-server

EXPOSE 4437

ENTRYPOINT ["/durable-streams-server"]
