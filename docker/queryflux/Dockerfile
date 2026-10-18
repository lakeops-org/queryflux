# syntax=docker/dockerfile:1
#
# Canonical QueryFlux Docker build. Edit this file only.
#
# Builds the QueryFlux server image (runtime-server): Rust server + Python venv + DuckDB + dbc CLI.
# Published as ghcr.io/lakeops-org/queryflux:<version> / :latest (also :<version>-slim / :latest-slim).
#
# Studio is not part of this image: run ghcr.io/lakeops-org/queryflux-studio next to it
# (a separate compose service, or the Helm chart's `studio` sidecar) with ADMIN_API_URL
# pointing at the admin port.
#
# Examples (repo root as context):
#   docker build -f docker/queryflux/Dockerfile -t queryflux:local .
#   docker buildx build --platform linux/amd64,linux/arm64 -f docker/queryflux/Dockerfile \
#     -t queryflux:local --push .
#
# PyO3/sqlglot: builder + runtime use python:3.12.13-slim-trixie.
# Rust: rustup 1.91.1. DuckDB: DUCKDB_DOWNLOAD_LIB links a prebuilt libduckdb.
#
# Final `cargo build` has no `--locked` (stub vs full crate sources); `cargo chef cook` uses `--locked`.
#
# ADBC drivers are not baked in. Declare them at run time with QUERYFLUX_ADBC_DRIVERS or
# QUERYFLUX_ADBC_DRIVERS_FILE; docker/adbc-drivers.sh installs them with the pinned dbc CLI.

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
# dbc CLI — installs the ADBC drivers users declare, at container start
# =============================================================================
FROM debian:trixie-slim AS dbc

ARG DBC_VERSION=0.3.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://dbc.columnar.tech/install.sh \
    | DBC_UNMANAGED_INSTALL=/usr/local/bin sh -s -- --version "${DBC_VERSION}" \
    && dbc --version

# =============================================================================
# Runtime: server only
# =============================================================================
FROM python:3.12.13-slim-trixie AS runtime-server

# curl: container healthchecks (docker compose examples) hit /health with it.
RUN apt-get update && apt-get install --no-install-recommends -y \
    ca-certificates \
    curl \
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
COPY --from=dbc /usr/local/bin/dbc /usr/local/bin/dbc
COPY docker/queryflux/entrypoint.sh /app/entrypoint.sh
COPY docker/adbc-drivers.sh /app/adbc-drivers.sh
RUN chmod +x /app/queryflux /app/entrypoint.sh /app/adbc-drivers.sh \
    && mkdir -p /var/lib/queryflux/adbc \
    && chown -R queryflux:queryflux /var/lib/queryflux

ENV LD_LIBRARY_PATH=/usr/local/lib
ENV PYO3_PYTHON=/opt/venv/bin/python3
WORKDIR /app
USER queryflux

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--config", "/etc/queryflux/config.yaml"]
