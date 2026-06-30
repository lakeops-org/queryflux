# CLI Quickstart (No Docker)

This example demonstrates how to run QueryFlux as a plain CLI binary on your host machine, using an embedded in-memory **DuckDB** engine. No Docker or container runtime is required.

## Prerequisites

1. **Rust** (stable toolchain installed).
2. **Python 3.10+** (if you need SQL dialect translation via `sqlglot`).

## Step 1: Install Python Dependencies (Optional but recommended)

If you plan to use SQL dialect translation, install `sqlglot` in a virtual environment:

```bash
# From the repository root
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Tell PyO3 where your Python interpreter is located:

```bash
export PYO3_PYTHON="$(pwd)/.venv/bin/python3"
```

## Step 2: Build the QueryFlux Binary

Build the binary from the repository root:

```bash
cargo build --release
```

The compiled binary will be placed at `./target/release/queryflux`.

## Step 3: Validate the Configuration and Environment

Run the validator flag to ensure your configuration file and dependencies (Python/sqlglot) are correctly set up:

```bash
./target/release/queryflux --config examples/cli-quickstart/config.yaml --validate
```

You should see:
```
Validating configuration file: examples/cli-quickstart/config.yaml
✓ Configuration file is valid YAML and matches the schema.
Validating Python & sqlglot environment...
✓ Python interpreter and 'sqlglot' library are available and correctly configured.

✓ Validation successful! Configuration and runtime dependencies are correctly configured.
```

## Step 4: Run QueryFlux

Start the proxy server pointing to the quickstart configuration:

```bash
./target/release/queryflux --config examples/cli-quickstart/config.yaml
```

The proxy is now running:
* **Trino HTTP Frontend**: `http://localhost:8080`
* **Admin REST API**: `http://localhost:9000`

## Step 5: Test it

Send a query using `curl` against the Trino HTTP port:

```bash
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: dev" \
  -d "SELECT 42"
```

You will receive a standard Trino response containing the data returned by the local embedded DuckDB:

```json
{
  "id": "...",
  "infoUri": "...",
  "nextUri": "...",
  "stats": { ... },
  "data": [[42]],
  ...
}
```
