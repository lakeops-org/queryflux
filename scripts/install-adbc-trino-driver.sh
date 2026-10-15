#!/usr/bin/env bash
# Install the Columnar `dbc` CLI and a pinned Trino ADBC driver shared library.
# Used by CI so `queryflux-e2e-tests` Trino ADBC integration tests can load `driver: trino`.
# Local dev: run this script, or install dbc yourself (https://docs.columnar.tech/dbc/).
#
# The driver version is pinned so a new upstream release can't change CI behind our back
# (#296, #297). Bump it here, in a reviewed PR.
#
# Requires: curl, sh
set -euo pipefail

DBC_VERSION="0.3.1"
TRINO_DRIVER_VERSION="${TRINO_DRIVER_VERSION:-0.6.0}"

curl -LsSf https://dbc.columnar.tech/install.sh | sh -s -- --version "${DBC_VERSION}"
export PATH="${HOME}/.local/bin:${PATH}"
if ! command -v dbc >/dev/null 2>&1; then
  echo "error: dbc not found after install (expected in ~/.local/bin)" >&2
  exit 1
fi
dbc install "trino=${TRINO_DRIVER_VERSION}"
