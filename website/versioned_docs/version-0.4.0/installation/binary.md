---
sidebar_position: 2
sidebar_label: Local (binary)
title: Install the QueryFlux binary
description: Build QueryFlux from source with Cargo, install the sqlglot Python dependency, validate a config, and run the server natively.
image: img/queryflux-hero-banner.png
---

# Binary / from source

Run QueryFlux natively when you don't want containers — on a VM, a bare-metal host, or your laptop.

## Prerequisites

- **Rust** (stable) and Cargo — [rustup.rs](https://rustup.rs)
- **Python 3.10+** — used by sqlglot for SQL dialect translation and by `pythonScript` routers
- A C toolchain (for the embedded DuckDB engine)

## Install

```bash
git clone https://github.com/lakeops-org/queryflux.git
cd queryflux

# Installs the `queryflux` binary into ~/.cargo/bin
cargo install --path crates/queryflux
```

If linking DuckDB fails, let the build download a prebuilt library:

```bash
export DUCKDB_DOWNLOAD_LIB=1
cargo install --path crates/queryflux
```

To build without installing, use `cargo build --release` (or `make build`); the binary lands in `./target/release/queryflux`.

## Install Python dependencies

```bash
queryflux --install-deps
```

This creates a `.venv` in the current directory and installs sqlglot. QueryFlux detects the `.venv` automatically at startup.

## Validate and run

```bash
# Check the config and the Python/sqlglot environment, then exit
queryflux --config examples/cli-quickstart/config.yaml --validate

# Start the server
queryflux --config examples/cli-quickstart/config.yaml
```

The `cli-quickstart` config uses the embedded **DuckDB** engine, so it works with no other services running.

## Commands

| Command | What it does |
| --- | --- |
| `queryflux --config <file>` | Run the server (same as `queryflux serve`). `--config` defaults to `config.yaml`. |
| `queryflux validate` / `--validate` | Validate config and the Python environment, then exit. |
| `queryflux install-deps` / `--install-deps` | Install sqlglot into a local `.venv`, then exit. |
| `queryflux migrate` | Apply pending Postgres schema migrations, then exit. |

## Run as a service

`cargo install` puts the binary in `~/.cargo/bin`. Copy it somewhere system-wide first:

```bash
sudo install -m 755 ~/.cargo/bin/queryflux /usr/local/bin/queryflux
```

Then create a minimal systemd unit:

```ini title="/etc/systemd/system/queryflux.service"
[Unit]
Description=QueryFlux SQL proxy
After=network-online.target

[Service]
ExecStart=/usr/local/bin/queryflux --config /etc/queryflux/config.yaml
WorkingDirectory=/var/lib/queryflux
Environment=RUST_LOG=queryflux=info
Environment=QUERYFLUX_ADMIN_USER=admin
EnvironmentFile=/etc/queryflux/secrets.env
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Run `queryflux --install-deps` once from the `WorkingDirectory` so the service finds its `.venv`, and put `QUERYFLUX_ADMIN_PASSWORD` in the environment file.

## Contributing?

For the full development loop — backing services, tests, and lints — see **[Development](/docs/development)**.
