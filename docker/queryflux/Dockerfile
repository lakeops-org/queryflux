# syntax=docker/dockerfile:1
#
# Canonical QueryFlux Docker build (server + optional Studio). Edit this file only.
#
# Targets (use --target for non-default):
#   runtime-server   — Rust server + Python venv + DuckDB + ADBC drivers (default final stage)
#   runtime-unified  — server + bundled QueryFlux Studio (Next.js)
#
# Examples (repo root as context):
#   docker build -f docker/queryflux/Dockerfile -t queryflux:local .
#   docker build -f docker/queryflux/Dockerfile --target runtime-unified -t queryflux:unified .
#   docker buildx build --platform linux/amd64,linux/arm64 -f docker/queryflux/Dockerfile \
#     --target runtime-unified -t queryflux:unified --push .
#
# PyO3/sqlglot: builder + runtime use python:3.12.13-slim-trixie.
# Rust: rustup 1.91.1. DuckDB: DUCKDB_DOWNLOAD_LIB links a prebuilt libduckdb.
#
# Final `cargo build` has no `--locked` (stub vs full crate sources); `cargo chef cook` uses `--locked`.

ARG QUERYFLUX_CONSOLE_IMAGE=ghcr.io/lakeops-org/queryflux-console:latest

# =============================================================================
# Chef base — Python + Rust (rustup, cargo-chef)
# =============================================================================
FROM python:3.12.13-slim-trixie AS chef

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/bin:/usr/local/cargo/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    pkg-config \
    libssl-dev \
    libffi-dev \
    build-essential \
    zlib1g-dev \
    cmake \
    ninja-build \
    perl \
    golang-go \
    mold \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    --default-toolchain 1.91.1 \
    --profile minimal \
    && rustup component add rustfmt clippy --toolchain 1.91.1

RUN cargo install cargo-chef --locked

WORKDIR /build

# =============================================================================
# Planner (cargo-chef recipe)
# =============================================================================
FROM chef AS planner

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

RUN cargo chef prepare --recipe-path recipe.json

# =============================================================================
# Builder — cook deps + release binary (multi-arch cache ids; mold on amd64/arm64)
# =============================================================================
FROM chef AS builder

ARG TARGETARCH=amd64
ARG CARGO_BUILD_JOBS=default
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

COPY requirements.txt .
RUN /usr/local/bin/python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt && \
    ln -sfn "$(/opt/venv/bin/python3 -c 'import site; print(site.getsitepackages()[0])')" /opt/venv/site-packages && \
    /opt/venv/bin/python3 -c 'import sys; assert sys.version_info[:2] == (3, 12), f"expected 3.12, got {sys.version}"'

ENV PYO3_PYTHON=/opt/venv/bin/python3
ENV PYTHONPATH=/opt/venv/site-packages
ENV DUCKDB_DOWNLOAD_LIB=1
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/lib/x86_64-linux-gnu:/usr/lib/aarch64-linux-gnu:/usr/lib/arm-linux-gnueabihf:/usr/lib/i386-linux-gnu

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY --from=planner /build/recipe.json recipe.json

COPY crates ./crates
RUN set -eu && find crates -type f -name '*.rs' -delete && \
    for c in queryflux-core queryflux-auth queryflux-persistence queryflux-routing \
        queryflux-translation queryflux-engine-adapters queryflux-frontend \
        queryflux-cluster-manager queryflux-config queryflux-metrics queryflux-e2e-tests; do \
      mkdir -p "crates/$c/src" && echo '// cargo-chef placeholder' > "crates/$c/src/lib.rs"; \
    done && \
    mkdir -p crates/queryflux/src crates/queryflux-bench/src && \
    printf 'fn main() {}\n' | tee crates/queryflux/src/main.rs > crates/queryflux-bench/src/main.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=queryflux-registry \
    --mount=type=cache,target=/build/target,id=queryflux-target-${TARGETARCH} \
    bash -o pipefail -c 'set -eu; \
    if [ "$TARGETARCH" = "amd64" ] || [ "$TARGETARCH" = "arm64" ]; then export RUSTFLAGS="-C link-arg=-fuse-ld=mold"; fi; \
    rm -rf target/release/build/pyo3-* target/release/build/pyo3_ffi-* 2>/dev/null || true; \
    cargo chef cook --release --locked --recipe-path recipe.json --package queryflux'

COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=queryflux-registry \
    --mount=type=cache,target=/build/target,id=queryflux-target-${TARGETARCH} \
    bash -o pipefail -c 'set -eu; \
    if [ "$TARGETARCH" = "amd64" ] || [ "$TARGETARCH" = "arm64" ]; then export RUSTFLAGS="-C link-arg=-fuse-ld=mold"; fi; \
    rm -rf target/release/build/pyo3-* target/release/build/pyo3_ffi-* \
      target/release/deps/libpyo3_ffi-* \
      target/release/.fingerprint/pyo3-* target/release/.fingerprint/pyo3_ffi-* 2>/dev/null || true; \
    cargo build --release -p queryflux --bin queryflux && \
    cp target/release/queryflux /queryflux-binary && \
    cp target/release/deps/libduckdb.so /queryflux-libduckdb.so'

# =============================================================================
# ADBC drivers (dbc CLI → ~/.config/adbc/drivers on Linux)
# =============================================================================
FROM debian:trixie-slim AS adbc-drivers

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://dbc.columnar.tech/install.sh | sh

ENV PATH="/root/.local/bin:/root/.cargo/bin:${PATH}"

RUN set -eu; command -v dbc; dbc install trino \
 && dbc install postgresql \
 && dbc install duckdb \
 && dbc install snowflake \
 && dbc install mysql \
 && dbc install databricks \
 && dbc install bigquery

# =============================================================================
# QueryFlux Console — pre-built standalone image used by runtime-unified
# =============================================================================
FROM ${QUERYFLUX_CONSOLE_IMAGE} AS studio-image

# =============================================================================
# Runtime: server + Studio + Node (bundled UI)
# =============================================================================
FROM python:3.12.13-slim-trixie AS runtime-unified

ARG NODE_VERSION=20.18.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    xz-utils \
    tini \
    && DPKG_ARCH=$(dpkg --print-architecture) \
    && case "$DPKG_ARCH" in \
         amd64) NODE_ARCH=x64 ;; \
         arm64) NODE_ARCH=arm64 ;; \
         armhf) NODE_ARCH=armv7l ;; \
         i386)  NODE_ARCH=x86 ;; \
         *) echo "unsupported arch=$DPKG_ARCH"; exit 1 ;; \
       esac \
    && curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz" \
         | tar -xJf - -C /usr/local --strip-components=1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 queryflux \
    && mkdir -p /app/studio \
    && chown -R queryflux:queryflux /app

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

COPY --from=builder /queryflux-binary /app/queryflux
COPY --from=builder /queryflux-libduckdb.so /usr/local/lib/libduckdb.so
COPY --from=adbc-drivers /root/.config/adbc/drivers/ /usr/local/lib/adbc/
COPY --from=studio-image --chown=queryflux:queryflux /app/ /app/studio/

COPY docker/queryflux-unified/entrypoint-unified.sh /app/entrypoint-unified.sh
RUN chmod +x /app/queryflux /app/entrypoint-unified.sh

ENV LD_LIBRARY_PATH=/usr/local/lib
ENV ADBC_DRIVER_PATH=/usr/local/lib/adbc
ENV PYO3_PYTHON=/opt/venv/bin/python3
WORKDIR /app
USER queryflux

EXPOSE 3000 8080 9000

ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint-unified.sh"]
CMD ["--config", "/etc/queryflux/config.yaml"]

# =============================================================================
# Runtime: server only (default `docker build` final stage)
# =============================================================================
FROM python:3.12.13-slim-trixie AS runtime-server

RUN apt-get update && apt-get install --no-install-recommends -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 queryflux \
    && mkdir -p /app \
    && chown -R queryflux:queryflux /app

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

COPY --from=builder /queryflux-binary /app/queryflux
COPY --from=builder /queryflux-libduckdb.so /usr/local/lib/libduckdb.so
COPY --from=adbc-drivers /root/.config/adbc/drivers/ /usr/local/lib/adbc/
COPY docker/queryflux/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/queryflux /app/entrypoint.sh

ENV LD_LIBRARY_PATH=/usr/local/lib
ENV ADBC_DRIVER_PATH=/usr/local/lib/adbc
ENV PYO3_PYTHON=/opt/venv/bin/python3
WORKDIR /app
USER queryflux

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--config", "/etc/queryflux/config.yaml"]
