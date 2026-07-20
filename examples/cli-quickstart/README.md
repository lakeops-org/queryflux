# CLI Quickstart (No Docker)

This example demonstrates how to run QueryFlux locally on your host machine as a plain CLI binary. 
It uses the embedded in-process `duckdb` engine, meaning you do not need to run any external databases or Docker containers to test the proxy.

### 1. Install QueryFlux
Run this from the root of the repository to install the binary globally:
```bash
cargo install --path crates/queryflux
```

### 2. Auto-Install Dependencies (Optional)
If you plan to use SQL translation (which requires Python and `sqlglot`), you can have QueryFlux automatically set up a `.venv` for you:
```bash
queryflux --install-deps
```

### 3. Run the Proxy
Pass this example's configuration file to the binary:
```bash
queryflux --config examples/cli-quickstart/config.yaml
```

### 4. Smoke Test
In a new terminal, send a query via the Trino HTTP protocol. QueryFlux will execute it instantly using the embedded DuckDB engine:
```bash
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: admin" \
  -d "SELECT 42 AS answer, 'Hello from DuckDB' AS message;"
```
