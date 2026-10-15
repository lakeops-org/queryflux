#!/bin/sh
set -e
export PYTHONPATH="$(/opt/venv/bin/python3 -c 'import site; print(site.getsitepackages()[0])')"
# Install the ADBC drivers declared in QUERYFLUX_ADBC_DRIVERS(_FILE), if any; exports ADBC_DRIVER_PATH.
. /app/adbc-drivers.sh
exec /app/queryflux "$@"
