#!/usr/bin/env bash
# Install a pinned Columnar `dbc` CLI and a pinned Trino ADBC driver shared library.
# Used by CI so `queryflux-e2e-tests` Trino ADBC integration tests can load `driver: trino`.
# Local dev: run this script, or install dbc yourself (https://docs.columnar.tech/dbc/).
#
# Both versions are pinned so a new upstream release can't change CI behind our back (#296, #297).
# Bump them here, in a reviewed PR. DBC_VERSION and the checksums match docker/queryflux/Dockerfile.
#
# Requires: curl, tar, sha256sum or shasum
set -euo pipefail

DBC_VERSION="0.3.1"
TRINO_DRIVER_VERSION="${TRINO_DRIVER_VERSION:-0.6.0}"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) platform=linux-amd64; sha=d394fe15226c3db4a980568f2b3aff049c58ef2badea6cbb711bc4cf3b7b2ed7 ;;
  Linux-aarch64 | Linux-arm64) platform=linux-arm64; sha=1a875bb6b1c76c83141a6a3aa0ce8510b1715c220ddbe88399377871abbe6982 ;;
  Darwin-x86_64) platform=darwin-amd64; sha=1574c825122b2d4096cd17daa535ca151675fde499b976bb7f5c56425b149fe6 ;;
  Darwin-arm64) platform=darwin-arm64; sha=c7528a710bb4e4551681e322724cb0d9d656d913f4837f566b4ac5fdb90a7a15 ;;
  *) echo "error: unsupported platform $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

bin_dir="${HOME}/.local/bin"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

tarball="dbc-${platform}-${DBC_VERSION}.tar.gz"
curl -fsSLo "${tmp}/${tarball}" \
  "https://github.com/columnar-tech/dbc/releases/download/v${DBC_VERSION}/${tarball}"
if command -v sha256sum >/dev/null 2>&1; then
  echo "${sha}  ${tmp}/${tarball}" | sha256sum -c -
else
  echo "${sha}  ${tmp}/${tarball}" | shasum -a 256 -c -
fi
tar -xzf "${tmp}/${tarball}" -C "$tmp" dbc
mkdir -p "$bin_dir"
install -m 0755 "${tmp}/dbc" "${bin_dir}/dbc"

"${bin_dir}/dbc" --version
"${bin_dir}/dbc" install "trino=${TRINO_DRIVER_VERSION}"
