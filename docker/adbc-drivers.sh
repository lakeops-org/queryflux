#!/bin/sh
# Installs the ADBC drivers the user declared, at container start (#297).
#
# The image ships no ADBC drivers. Nothing happens unless one of these is set:
#   QUERYFLUX_ADBC_DRIVERS       Whitespace-separated `dbc install` specs, e.g.
#                                "trino=0.6.0 postgresql>=1.10,<2". Commas belong to the
#                                constraint syntax, so they do not separate items.
#   QUERYFLUX_ADBC_DRIVERS_FILE  Path to a dbc.toml driver list (`dbc sync`). A dbc.lock next to
#                                it pins the versions.
# Optional:
#   QUERYFLUX_ADBC_DRIVER_DIR    Writable install directory (default /var/lib/queryflux/adbc).
#   QUERYFLUX_ADBC_ON_FAILURE    fail (default): exit non-zero. warn: log and start anyway.
#
# When drivers are declared, the install directory is prepended to ADBC_DRIVER_PATH, so a
# user-supplied ADBC_DRIVER_PATH (drivers mounted without dbc) keeps working behind it.
#
# The entrypoints source this file so the ADBC_DRIVER_PATH export reaches QueryFlux; the Helm
# initContainer runs it directly.

qf_adbc_log() {
  echo "[adbc-drivers] $*" >&2
}

# Driver name from a `dbc install` spec: "postgresql>=1.10,<2" -> "postgresql".
qf_adbc_spec_name() {
  echo "$1" | sed 's/[<>=!~^ ].*$//'
}

# True when a spec pins one exact version: "trino=0.6.0" or "trino==0.6.0".
qf_adbc_spec_pinned() {
  echo "$1" | grep -Eq '^[A-Za-z0-9_.-]+==?[0-9][^,<>=!~^ ]*$'
}

# Driver names listed in a dbc.toml (`[drivers.<name>]` tables).
qf_adbc_toml_names() {
  sed -n 's/^[[:space:]]*\[drivers\.\([^]]*\)\][[:space:]]*$/\1/p' "$1"
}

# Value of a string field in one line of `dbc install --json` output.
qf_adbc_json_field() {
  echo "$2" | sed -n "s/.*\"$1\":\"\\([^\"]*\\)\".*/\\1/p"
}

qf_adbc_install_specs() {
  for spec in $QUERYFLUX_ADBC_DRIVERS; do
    out=$(ADBC_DRIVER_PATH="$qf_adbc_dir" dbc install --json "$spec" 2>&1)
    rc=$?
    if [ "$rc" -ne 0 ]; then
      msg=$(qf_adbc_json_field message "$out")
      qf_adbc_log "error: dbc install \"$spec\" failed: ${msg:-$out}"
      return 1
    fi
    name=$(qf_adbc_json_field driver "$out")
    version=$(qf_adbc_json_field version "$out")
    status=$(qf_adbc_json_field status "$out")
    conflict=$(qf_adbc_json_field conflict "$out")
    qf_adbc_log "driver ${name:-$spec} ${version:-?} ${status:-installed}${conflict:+ (replaced $conflict)}"
    if ! qf_adbc_spec_pinned "$spec"; then
      qf_adbc_log "warning: \"$spec\" resolved to ${version:-?} at startup; pin it (${name:-$spec}=${version:-<version>}) for reproducible restarts"
    fi
  done
}

qf_adbc_sync_file() {
  work="$qf_adbc_dir/.dbc"
  # dbc sync writes a lock file next to dbc.toml, so it cannot run against a read-only mount.
  cp "$QUERYFLUX_ADBC_DRIVERS_FILE" "$work/dbc.toml" || return 1
  rm -f "$work/dbc.lock"
  if [ -f "$qf_adbc_src_lock" ]; then
    cp "$qf_adbc_src_lock" "$work/dbc.lock" || return 1
  fi
  if ! out=$(ADBC_DRIVER_PATH="$qf_adbc_dir" dbc sync --path "$work/dbc.toml" 2>&1); then
    qf_adbc_log "error: dbc sync --path $QUERYFLUX_ADBC_DRIVERS_FILE failed: $out"
    return 1
  fi
  # One log line per driver, from the lock dbc just wrote.
  awk -F"'" '/^name = /{n=$2} /^version = /{if (n != "") {print n " " $2; n=""}}' "$work/dbc.lock" |
    while read -r name version; do
      qf_adbc_log "driver $name $version installed"
    done
  if [ ! -f "$qf_adbc_src_lock" ]; then
    qf_adbc_log "warning: no dbc.lock next to $QUERYFLUX_ADBC_DRIVERS_FILE; unpinned drivers resolve at startup. Copy $work/dbc.lock next to it for reproducible restarts"
  fi
}

qf_adbc_main() {
  if [ -n "${QUERYFLUX_ADBC_DRIVERS:-}" ] && [ -n "${QUERYFLUX_ADBC_DRIVERS_FILE:-}" ]; then
    qf_adbc_log "error: set QUERYFLUX_ADBC_DRIVERS or QUERYFLUX_ADBC_DRIVERS_FILE, not both"
    return 2
  fi
  if [ -z "${QUERYFLUX_ADBC_DRIVERS:-}" ] && [ -z "${QUERYFLUX_ADBC_DRIVERS_FILE:-}" ]; then
    return 0
  fi

  qf_adbc_dir="${QUERYFLUX_ADBC_DRIVER_DIR:-/var/lib/queryflux/adbc}"
  qf_adbc_on_failure="${QUERYFLUX_ADBC_ON_FAILURE:-fail}"
  case "$qf_adbc_on_failure" in
    fail | warn) ;;
    *)
      qf_adbc_log "error: QUERYFLUX_ADBC_ON_FAILURE must be fail or warn, got '$qf_adbc_on_failure'"
      return 2
      ;;
  esac

  if [ -n "${QUERYFLUX_ADBC_DRIVERS_FILE:-}" ]; then
    if [ ! -f "$QUERYFLUX_ADBC_DRIVERS_FILE" ]; then
      qf_adbc_log "error: QUERYFLUX_ADBC_DRIVERS_FILE=$QUERYFLUX_ADBC_DRIVERS_FILE does not exist"
      return 2
    fi
    qf_adbc_src_lock="$(dirname "$QUERYFLUX_ADBC_DRIVERS_FILE")/dbc.lock"
    names=$(qf_adbc_toml_names "$QUERYFLUX_ADBC_DRIVERS_FILE")
    fingerprint=$(cat "$QUERYFLUX_ADBC_DRIVERS_FILE" "$qf_adbc_src_lock" 2>/dev/null | sha256sum)
  else
    names=""
    for spec in $QUERYFLUX_ADBC_DRIVERS; do
      names="$names $(qf_adbc_spec_name "$spec")"
    done
    fingerprint=$(printf '%s' "$QUERYFLUX_ADBC_DRIVERS" | sha256sum)
  fi

  if ! mkdir -p "$qf_adbc_dir/.dbc" 2>/dev/null || [ ! -w "$qf_adbc_dir" ]; then
    qf_adbc_log "error: $qf_adbc_dir is not writable; mount a volume there (the Helm chart uses a read-only root filesystem) or set QUERYFLUX_ADBC_DRIVER_DIR"
    return 2
  fi

  # `dbc` contacts the registry even when nothing changed, so skip it when the same list is
  # already installed. This keeps restarts on a persistent volume working without network access.
  satisfied=1
  [ "$(cat "$qf_adbc_dir/.dbc/stamp" 2>/dev/null)" = "$fingerprint" ] || satisfied=0
  for name in $names; do
    [ -f "$qf_adbc_dir/$name.toml" ] || satisfied=0
  done

  if [ "$satisfied" -eq 1 ]; then
    qf_adbc_log "drivers already installed in $qf_adbc_dir: $(echo $names)"
  else
    if [ -n "${QUERYFLUX_ADBC_DRIVERS_FILE:-}" ]; then
      qf_adbc_sync_file
    else
      qf_adbc_install_specs
    fi
    rc=$?
    if [ "$rc" -eq 0 ]; then
      echo "$fingerprint" >"$qf_adbc_dir/.dbc/stamp"
    elif [ "$qf_adbc_on_failure" = "fail" ]; then
      qf_adbc_log "aborting startup (QUERYFLUX_ADBC_ON_FAILURE=fail)"
      return 1
    else
      rm -f "$qf_adbc_dir/.dbc/stamp"
      qf_adbc_log "continuing without the failed drivers (QUERYFLUX_ADBC_ON_FAILURE=warn)"
    fi
  fi

  ADBC_DRIVER_PATH="$qf_adbc_dir${ADBC_DRIVER_PATH:+:$ADBC_DRIVER_PATH}"
  export ADBC_DRIVER_PATH
  return 0
}

qf_adbc_main || exit $?
