#!/usr/bin/env bash
# Install the Columnar `dbc` CLI and the Trino ADBC driver shared library.
# Used by CI so `queryflux-e2e-tests` Trino ADBC integration tests can load `driver: trino`.
# Local dev: install manually (https://docs.columnar.tech/dbc/) or `brew install dbc` where available.
#
# Requires: curl, sh
set -euo pipefail

curl -LsSf https://dbc.columnar.tech/install.sh | sh
export PATH="${HOME}/.local/bin:${PATH}"
if ! command -v dbc >/dev/null 2>&1; then
  echo "error: dbc not found after install (expected in ~/.local/bin)" >&2
  exit 1
fi
# Pinned: trino driver 0.6.0 no longer defaults a user (401 against an unauthenticated
# Trino) and its metadata calls fail without a session catalog. Bump deliberately.
TRINO_ADBC_DRIVER_VERSION="${TRINO_ADBC_DRIVER_VERSION:-0.5.3}"
dbc install "trino=${TRINO_ADBC_DRIVER_VERSION}"
