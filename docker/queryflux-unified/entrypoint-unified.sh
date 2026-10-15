#!/bin/sh
# Runs QueryFlux (admin API + frontends) and QueryFlux Studio (Next.js) in one container.
# Studio talks to the admin API via ADMIN_API_URL (default http://127.0.0.1:9000).
set -e
export PYTHONPATH="$(/opt/venv/bin/python3 -c 'import site; print(site.getsitepackages()[0])')"

# Install the ADBC drivers declared in QUERYFLUX_ADBC_DRIVERS(_FILE), if any; exports ADBC_DRIVER_PATH.
. /app/adbc-drivers.sh

ADMIN_PORT="${QUERYFLUX_ADMIN_PORT:-9000}"
export ADMIN_API_URL="${ADMIN_API_URL:-http://127.0.0.1:${ADMIN_PORT}}"

cleanup() {
  kill "$QF_PID" 2>/dev/null || true
  kill "$STUDIO_PID" 2>/dev/null || true
}
trap cleanup INT TERM

/app/queryflux "$@" &
QF_PID=$!

i=0
while [ "$i" -lt 300 ]; do
  if curl -sf "http://127.0.0.1:${ADMIN_PORT}/health" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.2
done
if [ "$i" -ge 300 ]; then
  echo "queryflux admin did not become ready on port ${ADMIN_PORT}" >&2
  exit 1
fi

cd /app/studio
export PORT="${STUDIO_PORT:-3000}"
export HOSTNAME=0.0.0.0
export NODE_ENV=production
node server.js &
STUDIO_PID=$!

wait "$QF_PID"
STATUS=$?
cleanup
exit "$STATUS"
