#!/usr/bin/env bash
# Tests docker/adbc-drivers.sh in throwaway containers built from the Dockerfile's `dbc` stage
# (Debian + the pinned dbc CLI), without building QueryFlux. Needs Docker and network access.
#
#   bash scripts/test-adbc-drivers.sh
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
image="queryflux-dbc-test:local"
vol="qf-adbc-test-$$"
fixtures="$(mktemp -d)"
failures=0

cleanup() {
  docker volume rm -f "$vol" >/dev/null 2>&1 || true
  rm -rf "$fixtures"
}
trap cleanup EXIT

docker build -q -f "$repo/docker/queryflux/Dockerfile" --target dbc -t "$image" "$repo" >/dev/null

# Runs the script as uid 1000, the way the entrypoint sources it, then prints ADBC_DRIVER_PATH.
# Usage: run <docker run args...> -- ; output in $out, exit code in $rc.
run() {
  set +e
  out=$(docker run --rm -u 1000:1000 \
    -v "$repo/docker/adbc-drivers.sh:/app/adbc-drivers.sh:ro" \
    -v "$vol:/var/lib/queryflux/adbc" \
    "$@" "$image" \
    sh -c '. /app/adbc-drivers.sh; echo "ADBC_DRIVER_PATH=${ADBC_DRIVER_PATH-<unset>}"' 2>&1)
  rc=$?
  set -e
}

fresh_volume() {
  docker volume rm -f "$vol" >/dev/null 2>&1 || true
  docker run --rm -v "$vol:/v" "$image" chown 1000:1000 /v
}

check() {
  local name="$1" want_rc="$2"
  shift 2
  local ok=1
  [ "$rc" = "$want_rc" ] || ok=0
  for pattern in "$@"; do
    case "$pattern" in
      '!'*) if grep -qF -- "${pattern#!}" <<<"$out"; then ok=0; fi ;;
      *) grep -qF -- "$pattern" <<<"$out" || ok=0 ;;
    esac
  done
  if [ "$ok" = 1 ]; then
    echo "ok   $name"
  else
    echo "FAIL $name (exit $rc, want $want_rc)"
    sed 's/^/     | /' <<<"$out"
    failures=$((failures + 1))
  fi
}

fresh_volume
run --network none
check "nothing declared: no dbc, no network, ADBC_DRIVER_PATH untouched" 0 \
  "ADBC_DRIVER_PATH=<unset>" "!adbc-drivers"

run --network none -e ADBC_DRIVER_PATH=/opt/mine
check "nothing declared: user's ADBC_DRIVER_PATH kept" 0 "ADBC_DRIVER_PATH=/opt/mine"

run -e QUERYFLUX_ADBC_DRIVERS=trino -e QUERYFLUX_ADBC_DRIVERS_FILE=/x/dbc.toml
check "both variables set: error" 2 "not both"

run -e QUERYFLUX_ADBC_DRIVERS=trino -e QUERYFLUX_ADBC_ON_FAILURE=maybe
check "bad QUERYFLUX_ADBC_ON_FAILURE: error" 2 "must be fail or warn"

run -e "QUERYFLUX_ADBC_DRIVERS=trino=0.6.0 postgresql>=1.10,<2"
check "env var: installs specs, comma stays inside a constraint" 0 \
  "driver trino 0.6.0 installed" "driver postgresql" \
  "warning: \"postgresql>=1.10,<2\" resolved to" "!warning: \"trino=0.6.0\"" \
  "ADBC_DRIVER_PATH=/var/lib/queryflux/adbc"

run --network none -e "QUERYFLUX_ADBC_DRIVERS=trino=0.6.0 postgresql>=1.10,<2"
check "env var: restart offline on the same volume skips dbc" 0 \
  "drivers already installed in /var/lib/queryflux/adbc: trino postgresql" \
  "ADBC_DRIVER_PATH=/var/lib/queryflux/adbc"

run -e "QUERYFLUX_ADBC_DRIVERS=trino=0.5.0" -e ADBC_DRIVER_PATH=/opt/mine
check "env var: changed version reinstalls; user path kept behind" 0 \
  "driver trino 0.5.0 installed (replaced trino (version: 0.6.0))" \
  "ADBC_DRIVER_PATH=/var/lib/queryflux/adbc:/opt/mine"

run -e "QUERYFLUX_ADBC_DRIVERS=nosuchdriver"
check "unknown driver: startup aborts with dbc's message" 1 \
  "nosuchdriver" "not found in driver registry" "aborting startup"

run -e "QUERYFLUX_ADBC_DRIVERS=nosuchdriver" -e QUERYFLUX_ADBC_ON_FAILURE=warn
check "unknown driver with warn: starts anyway" 0 \
  "continuing without the failed drivers" "ADBC_DRIVER_PATH=/var/lib/queryflux/adbc"

mkdir -p "$fixtures/adbc"
printf "[drivers]\n[drivers.trino]\nversion = '=0.6.0'\n" >"$fixtures/adbc/dbc.toml"
chmod -R a+rX "$fixtures"
fresh_volume
run -v "$fixtures/adbc:/etc/queryflux/adbc:ro" \
  -e QUERYFLUX_ADBC_DRIVERS_FILE=/etc/queryflux/adbc/dbc.toml
check "file: read-only mounted dbc.toml syncs" 0 \
  "driver trino 0.6.0 installed" "warning: no dbc.lock next to" \
  "ADBC_DRIVER_PATH=/var/lib/queryflux/adbc"

run --network none -v "$fixtures/adbc:/etc/queryflux/adbc:ro" \
  -e QUERYFLUX_ADBC_DRIVERS_FILE=/etc/queryflux/adbc/dbc.toml
check "file: restart offline on the same volume skips dbc" 0 "drivers already installed"

run -e QUERYFLUX_ADBC_DRIVERS_FILE=/etc/queryflux/adbc/missing.toml
check "file: missing path is an error" 2 "does not exist"

set +e
out=$(docker run --rm -u 1000:1000 -v "$repo/docker/adbc-drivers.sh:/app/adbc-drivers.sh:ro" \
  -e QUERYFLUX_ADBC_DRIVERS=trino -e QUERYFLUX_ADBC_DRIVER_DIR=/usr/lib/adbc "$image" \
  sh -c '. /app/adbc-drivers.sh' 2>&1)
rc=$?
set -e
check "install dir not writable: error points at a volume" 2 "is not writable; mount a volume"

if [ "$failures" -gt 0 ]; then
  echo "$failures test(s) failed"
  exit 1
fi
echo "all tests passed"
