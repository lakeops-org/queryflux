---
title: ADBC drivers
description: Declare the ADBC drivers QueryFlux installs when the container starts — with an environment variable, a dbc.toml file, or your own driver files.
image: img/queryflux-hero-banner.png
---

# ADBC drivers

Clusters with `engine: adbc` load a driver shared library by name (`driver: trino`, `driver: snowflake`, …).
**The QueryFlux image does not include any ADBC drivers.** You declare the ones your clusters use, and the
container installs them with the [`dbc`](https://docs.columnar.tech/dbc/) CLI when it starts. If you
declare nothing, nothing is installed and startup makes no network calls, which is what you want when
you don't use ADBC clusters.

## Declare drivers

Use **one** of these. Setting both is a startup error.

| Variable | Example | What runs |
|---|---|---|
| `QUERYFLUX_ADBC_DRIVERS` | `trino=0.6.0 postgresql=1.12.0` | `dbc install` for each whitespace-separated item. Each item accepts dbc's [version constraints](https://docs.columnar.tech/dbc/guides/installing/#version-constraints), such as `postgresql>=1.10,<2`. |
| `QUERYFLUX_ADBC_DRIVERS_FILE` | `/etc/queryflux/adbc/dbc.toml` | `dbc sync` with that driver list. If a `dbc.lock` sits next to it, its versions are used. |

Pin exact versions (`trino=0.6.0`), or mount a `dbc.lock`. Otherwise a new upstream driver release changes
what your pods run the next time they start. QueryFlux logs a warning for every driver that resolved
without a pin.

Other settings:

| Variable | Default | Meaning |
|---|---|---|
| `QUERYFLUX_ADBC_DRIVER_DIR` | `/var/lib/queryflux/adbc` | Where drivers are installed. Must be writable by the `queryflux` user (uid 1000). |
| `QUERYFLUX_ADBC_ON_FAILURE` | `fail` | `fail`: the container exits if a driver can't be installed. `warn`: it logs the error and starts; clusters whose driver is missing fail to load. |

The startup log shows what was installed:

```text
[adbc-drivers] driver trino 0.6.0 installed
[adbc-drivers] driver postgresql 1.12.0 installed
```

### Restarts and offline starts

When the same list is already installed in the driver directory, startup skips `dbc` entirely. Mount a volume
at `/var/lib/queryflux/adbc` to keep drivers across container restarts. Restarts then need no network
access. Without a volume, each new container downloads its drivers again. That takes a few seconds per
driver.

## Docker

```bash
# Environment variable
docker run \
  -e QUERYFLUX_ADBC_DRIVERS="trino=0.6.0 snowflake=1.14.0" \
  -v qf-adbc:/var/lib/queryflux/adbc \
  -v ./config.yaml:/etc/queryflux/config.yaml \
  ghcr.io/lakeops-org/queryflux

# dbc.toml (+ optional dbc.lock) mounted read-only
docker run \
  -e QUERYFLUX_ADBC_DRIVERS_FILE=/etc/queryflux/adbc/dbc.toml \
  -v ./adbc:/etc/queryflux/adbc:ro \
  -v qf-adbc:/var/lib/queryflux/adbc \
  -v ./config.yaml:/etc/queryflux/config.yaml \
  ghcr.io/lakeops-org/queryflux
```

A `dbc.toml` looks like this. Create one with `dbc init` and `dbc add`, or write it by hand:

```toml
[drivers]
[drivers.trino]
version = '=0.6.0'
[drivers.snowflake]
version = '=1.14.0'
```

## Kubernetes (Helm)

```yaml
adbcDrivers:
  install:
    - trino=0.6.0
    - snowflake=1.14.0
```

By default (`mode: initContainer`), an init container installs the drivers into a shared volume. The server
container mounts that volume read-only, so it needs no registry access. See the
[chart README](https://github.com/lakeops-org/queryflux/blob/main/charts/queryflux/README.md#adbc-drivers) for
`dbcToml`, `dbcLock`, `mode` and `volume`.

## Your own drivers, without dbc

Point `ADBC_DRIVER_PATH` at a directory of ADBC driver manifests (`<name>.toml`) and libraries. Use this in
air-gapped environments, or for drivers that aren't in the dbc registry:

```bash
docker run -e ADBC_DRIVER_PATH=/opt/adbc -v ./my-drivers:/opt/adbc:ro \
  -v ./config.yaml:/etc/queryflux/config.yaml ghcr.io/lakeops-org/queryflux
```

If you also declare drivers, the install directory goes first and your path is searched after it.

Manifests written by `dbc` contain **absolute** library paths. If you prepare a directory with `dbc install`
on another machine or in another container, mount it at the same path it was installed to.

## Network access

`dbc` downloads from `dbc-cdn.columnar.tech` and `dbc-cdn-private.columnar.tech` over HTTPS. If you restrict
egress, for example with the chart's `networkPolicy`, allow port 443 to those hosts for whichever container
installs the drivers.

## Upgrading from images that bundled drivers

Earlier images bundled seven drivers: trino, postgresql, duckdb, snowflake, mysql, databricks and bigquery.
If a cluster now fails with `failed to load ADBC driver '<name>'`, declare that driver. For example:

```bash
QUERYFLUX_ADBC_DRIVERS="trino=0.6.0 snowflake=1.14.0"
```
