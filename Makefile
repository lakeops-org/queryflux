CARGO        := $(HOME)/.cargo/bin/cargo
COMPOSE      := docker compose -f docker/docker-compose.yml --project-directory .
COMPOSE_TEST := docker compose -f docker/docker-compose.test.yml --project-directory .

# site-packages for `.venv` — works for any Python 3.x (CI uses 3.12, dev may use 3.13).
PYTHONPATH_VENV = $(shell test -x .venv/bin/python3 && .venv/bin/python3 -c 'import site; print(site.getsitepackages()[0])')

# DuckDB: on Linux (local dev, not CI), let libduckdb-sys download the prebuilt .so.
# CI installs libduckdb system-wide instead (see .github/workflows/ci.yml).
# On macOS use `brew install duckdb` (see .cargo/config.toml) or `export DUCKDB_DOWNLOAD_LIB=1`.
ifeq ($(shell uname -s),Linux)
  ifndef DUCKDB_LIB_DIR
    export DUCKDB_DOWNLOAD_LIB := 1
  endif
endif

# Trino `tpch` schema used when loading Iceberg tables (see docker/fixtures/init.sql + data-loader).
# tiny = default fast tests; sf1 ≈ 1.5M orders (long load, heavy E2E).
TPCH_SCALE ?= tiny
export TPCH_SCALE

.PHONY: dev stop logs build lint clippy check helm-check test benchmark benchmark-build benchmark-run test-e2e clean setup

## Create virtualenv and install Python dependencies (sqlglot etc.)
setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt
	@echo "Python env ready. Run: export PYO3_PYTHON=$$(pwd)/.venv/bin/python3"

## Start all services (Trino, StarRocks, Lakekeeper + MinIO, Postgres, observability),
## load TPC-H data into Iceberg, then run QueryFlux locally.
env:
	@test -f .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	@pkill -f "queryflux.*config.local.yaml" 2>/dev/null; true
	$(COMPOSE) up -d --wait trino starrocks postgres sentinel
	$(COMPOSE) run --rm -T data-loader
	$(COMPOSE) run --rm -T starrocks-catalog-setup


server:
	@test -x .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(PYTHONPATH_VENV) \
	RUST_LOG=queryflux=info,queryflux_frontend=info \
	$(CARGO) run --bin queryflux -- --config config.local.yaml
## Stop Docker services and any running QueryFlux process
stop:
	@pkill -f "queryflux.*config.local.yaml" 2>/dev/null; true
	$(COMPOSE) down

## Stream logs from Docker services
logs:
	$(COMPOSE) logs -f

## Build the proxy binary (release mode)
build:
	$(CARGO) build --release

## Run clippy lints (no external services needed).
lint: clippy
clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

## Validate the Helm chart structure and run helm lint/template.
helm-check:
	scripts/check-helm-chart.sh

## Run unit/integration tests (no external services needed).
## Same command as CI `.github/workflows/ci.yml` (`make test`).
## PYO3_PYTHON + PYTHONPATH: PyO3 (routing + translation). The venv must include `sqlglot`
## (`pip install -r requirements.txt` via `make setup`) for `queryflux-translation` transform tests.
test:
	@test -f .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(PYTHONPATH_VENV) \
	$(CARGO) test --tests --workspace --exclude queryflux-e2e-tests --exclude queryflux-bench

## Micro-benchmark: mock Trino + StarRocks backends vs QueryFlux (release build).
## Optional: QUERYFLUX_BENCH_WARMUP, QUERYFLUX_BENCH_ITERATIONS, QUERYFLUX_BENCH_TRINO_POLL.
benchmark: benchmark-build benchmark-run

benchmark-build:
	@test -f .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(PYTHONPATH_VENV) \
	$(CARGO) build --release --bin queryflux

benchmark-run:
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(PYTHONPATH_VENV) \
	$(CARGO) run --release -p queryflux-bench

## Run E2E tests. Spins up Trino + StarRocks + Lakekeeper stack via Docker.
## CI runs the same compose + `cargo test -p queryflux-e2e-tests` steps explicitly (see `e2e` job).
## Requires reachable engines; see docker/docker-compose.test.yml.
## Trino ADBC tests (`tests/trino_adbc_tests.rs`) need the Trino ADBC driver on the host (`dbc install trino`);
## `TRINO_ADBC_URI` matches the compose Trino port (same as TRINO_URL).
## `--test-threads=1`: StarRocks Iceberg is slow; default parallel libtest + `#[serial]` makes
## every test report libtest's 60s "slow test" spam while threads wait on the serial lock.
## Iceberg/Lakekeeper tables are created by the e2e crate (no TPC-H loader).
test-e2e:
	@test -f .venv/bin/python3 || (echo "Run 'make setup' first" && exit 1)
	@set -e; \
	trap '$(COMPOSE_TEST) down' EXIT; \
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(PYTHONPATH_VENV) \
	$(COMPOSE_TEST) up -d --wait trino starrocks sentinel; \
	PYO3_PYTHON=$(shell pwd)/.venv/bin/python3 \
	PYTHONPATH=$(PYTHONPATH_VENV) \
	TRINO_URL=http://localhost:18081 \
	TRINO_ADBC_URI=http://localhost:18081 \
	STARROCKS_URL=mysql://root@localhost:9030 \
	LAKEKEEPER_URL=http://localhost:18181 \
	MINIO_ENDPOINT=localhost:19000 \
	$(CARGO) test -p queryflux-e2e-tests --manifest-path Cargo.toml -- --test-threads=1 --include-ignored --nocapture

## Remove build artifacts and Docker volumes
clean:
	$(CARGO) clean
	$(COMPOSE) down -v
