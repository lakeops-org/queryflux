# syntax=docker/dockerfile:1
#
# queryflux-collector image: polls engines for each query run and reports it to the
# QueryFlux control plane. Rust only: no Python, DuckDB or ADBC drivers.
# Published as ghcr.io/lakeops-org/queryflux-collector:<version> / :latest.
#
#   docker build -f docker/collector/Dockerfile -t queryflux-collector:local .
#   docker run -v $PWD/collector.yaml:/etc/queryflux/collector.yaml queryflux-collector:local

FROM rust:1.91.1-slim-trixie AS builder

ARG CARGO_BUILD_JOBS=default
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=collector-registry \
    --mount=type=cache,target=/build/target,id=collector-target \
    cargo build --release --locked -p queryflux-collector && \
    cp target/release/queryflux-collector /queryflux-collector

FROM debian:trixie-slim AS runtime

RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 queryflux

COPY --from=builder /queryflux-collector /usr/local/bin/queryflux-collector

USER queryflux
ENV QUERYFLUX_COLLECTOR_CONFIG=/etc/queryflux/collector.yaml
ENTRYPOINT ["/usr/local/bin/queryflux-collector"]
