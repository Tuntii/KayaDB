# Multi-stage build: compile kayadb-server + kayactl, ship a slim Debian runtime.
#
# Build from repository root:
#   docker build -f deploy/docker/Dockerfile -t kayadb:latest .

FROM rust:1.85-bookworm AS builder

WORKDIR /build

# Copy workspace manifests first for better layer caching.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

RUN cargo build --release -p kaya-server -p kayactl

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/kayadb-server /usr/local/bin/kayadb-server
COPY --from=builder /build/target/release/kayactl /usr/local/bin/kayactl

# Default data directory; override with --data or a volume mount.
VOLUME ["/data"]

EXPOSE 7379 7481

ENTRYPOINT ["kayadb-server"]
CMD ["--node-id", "1", "--raft-addr", "127.0.0.1:7481", "--client-addr", "127.0.0.1:7379", "--data", "/data"]