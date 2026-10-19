#!/usr/bin/env bash
# Same-machine A/B of the proxy-overhead benchmark (crates/queryflux-bench).
#
# Runs ONE harness build against two queryflux binaries, alternating ABBA order per round so
# slow drift on a shared runner (thermal, noisy neighbours) hits both sides equally. Each run
# writes the harness's JSON array to $OUT/{base,head}-<round>.json; compare with
# scripts/bench/compare.py.
#
# Usage: scripts/bench/proxy-ab.sh <base-queryflux-bin|-> <head-queryflux-bin> [rounds] [out-dir]
#   base `-`  → head-only (main pushes: record numbers, nothing to compare against).
#   HARNESS=path/to/queryflux-bench   (default target/release/queryflux-bench)
#   QUERYFLUX_BENCH_WARMUP / QUERYFLUX_BENCH_ITERATIONS pass through to the harness.
#
# The harness writes the config both binaries load. If a PR changes the config schema, the
# base binary may reject it — base runs then stop and the report falls back to head-only.
set -euo pipefail

base_bin=${1:?base queryflux binary (or - for head-only)}
head_bin=${2:?head queryflux binary}
rounds=${3:-6}
out=${4:-bench-results}
harness=${HARNESS:-target/release/queryflux-bench}

mkdir -p "$out"
base_ok=1
[[ "$base_bin" == "-" ]] && base_ok=0

run_head() {
  echo "::group::round $1 — head"
  QUERYFLUX_BENCH_BIN="$head_bin" "$harness" > "$out/head-$1.json"
  echo "::endgroup::"
}

run_base() {
  ((base_ok)) || return 0
  echo "::group::round $1 — base"
  if ! QUERYFLUX_BENCH_BIN="$base_bin" "$harness" > "$out/base-$1.json"; then
    echo "::endgroup::"
    echo "::warning::base binary failed under the head harness (config change?) — reporting head only"
    rm -f "$out"/base-*.json
    base_ok=0
    return 0
  fi
  echo "::endgroup::"
}

for ((i = 1; i <= rounds; i++)); do
  if ((i % 2)); then
    run_base "$i"
    run_head "$i"
  else
    run_head "$i"
    run_base "$i"
  fi
done
