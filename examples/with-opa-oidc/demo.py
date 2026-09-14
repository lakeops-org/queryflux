#!/usr/bin/env python3
"""Seed Lakekeeper Iceberg tables through QueryFlux and print alice vs bob results.

Same demo as examples/with-opa, except identity comes from Keycloak (OIDC)
instead of a static user list: each query fetches a real access token via a
password grant and sends it as `Authorization: Bearer ...`.

Requires:
  docker compose up -d --wait
  docker compose --profile seed run --rm data-seed
  cargo run -p queryflux -- --config examples/with-opa-oidc/config.yaml
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

TRINO = "http://127.0.0.1:8080"
ADMIN = "http://127.0.0.1:9000"
OPA = "http://127.0.0.1:8182"
LAKEKEEPER = "http://127.0.0.1:8181"
TRINO_DIRECT = "http://127.0.0.1:8081"
KEYCLOAK = "http://127.0.0.1:8180"
REALM = "queryflux"
CLIENT_ID = "queryflux"
HERE = Path(__file__).resolve().parent

CUSTOMERS = "lakekeeper.demo.customers"
PAYROLL = "lakekeeper.demo.payroll"

_token_cache: dict[str, tuple[str, float]] = {}


def _basic(user: str, password: str) -> str:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return f"Basic {token}"


def get_token(user: str, password: str) -> str:
    """Password-grant a Keycloak access token for `user`, cached until near expiry."""
    cached = _token_cache.get(user)
    if cached and cached[1] > time.time():
        return cached[0]
    body = urllib.parse.urlencode(
        {
            "grant_type": "password",
            "client_id": CLIENT_ID,
            "username": user,
            "password": password,
        }
    ).encode()
    req = urllib.request.Request(
        f"{KEYCLOAK}/realms/{REALM}/protocol/openid-connect/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Keycloak token request for {user!r} failed: {e.read().decode()}") from e
    token = payload["access_token"]
    _token_cache[user] = (token, time.time() + max(payload.get("expires_in", 60) - 5, 5))
    return token


def _request(url: str, *, data: bytes | None = None, headers: dict[str, str]) -> dict | str:
    req = urllib.request.Request(url, data=data, headers=headers, method="POST" if data is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode()
    except urllib.error.HTTPError as e:
        raw = e.read().decode() if e.fp else str(e)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            if e.code in (403, 401):
                return {"error": {"message": raw.strip() or f"HTTP {e.code}"}}
            raise SystemExit(f"HTTP {e.code} from {url}: {raw[:500]}") from e
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def wait_http(url: str, name: str, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                if 200 <= resp.status < 300:
                    return
                last = f"HTTP {resp.status}"
        except Exception as e:  # noqa: BLE001 — wait loop
            last = str(e)
        time.sleep(0.4)
    raise SystemExit(f"{name} not ready at {url}: {last}")


def trino_query(user: str, password: str, sql: str) -> tuple[list[str] | None, list[list[object]], str | None]:
    """Return (columns, rows, error_message)."""
    token = get_token(user, password)
    # No X-Trino-User: QueryFlux verifies the Bearer token itself and derives
    # the user/groups from its claims (contrast the static-auth with-opa demo).
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Trino-Source": "opa-demo",
    }
    body = _request(f"{TRINO}/v1/statement", data=sql.encode(), headers=headers)
    if not isinstance(body, dict):
        return None, [], f"unexpected response: {body!r}"

    columns: list[str] | None = None
    rows: list[list[object]] = []
    error: str | None = None

    while True:
        if "error" in body:
            err = body["error"]
            error = err.get("message") if isinstance(err, dict) else str(err)
            break
        if "columns" in body and columns is None:
            columns = [c["name"] for c in body["columns"]]
        rows.extend(body.get("data") or [])
        nxt = body.get("nextUri")
        if not nxt:
            break
        body = _request(nxt, headers=headers)
        if not isinstance(body, dict):
            error = f"unexpected poll response: {body!r}"
            break
    return columns, rows, error


def print_result(title: str, columns: list[str] | None, rows: list[list[object]], error: str | None) -> None:
    print(f"\n== {title}")
    if error:
        print(f"DENIED / ERROR: {error}")
        return
    if not columns:
        print("(no rows)")
        return
    print(" | ".join(columns))
    print("-" * 40)
    for row in rows:
        print(" | ".join("" if v is None else str(v) for v in row))


def statements_from_sql(text: str) -> list[str]:
    out = []
    for chunk in text.split(";"):
        lines = [
            ln
            for ln in chunk.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        ]
        stmt = "\n".join(lines).strip()
        if stmt:
            out.append(stmt)
    return out


def seed() -> None:
    sql = (HERE / "seed.sql").read_text()
    for stmt in statements_from_sql(sql):
        _, _, err = trino_query("alice", "alice", stmt)
        if err:
            raise SystemExit(
                f"seed failed on:\n{stmt}\n{err}\n"
                "Run: docker compose --profile seed run --rm data-seed"
            )
    print(f"Seeded {CUSTOMERS} + {PAYROLL}.")


def dry_run(user: str, groups: list[str], sql: str) -> None:
    # Dry-run takes identity directly in the request body (no token needed) —
    # it's a what-if preview behind the Admin API's own Basic auth, not the
    # authenticated query path OIDC protects.
    payload = json.dumps(
        {
            "sql": sql,
            "clusterGroup": "trino-opa",
            "dialect": "trino",
            "identity": {"user": user, "groups": groups},
        }
    ).encode()
    auth = _basic("admin", "admin")
    body = _request(
        f"{ADMIN}/admin/access-control/dry-run",
        data=payload,
        headers={
            "Authorization": auth,
            "Content-Type": "application/json",
        },
    )
    print(f"\n== dry-run as {user}")
    print(json.dumps(body, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-seed", action="store_true")
    parser.add_argument("--dry-run-only", action="store_true")
    args = parser.parse_args()

    wait_http(f"{KEYCLOAK}/realms/{REALM}/protocol/openid-connect/certs", "Keycloak")
    wait_http(f"{OPA}/health", "OPA")
    wait_http(f"{LAKEKEEPER}/health", "Lakekeeper")
    wait_http(f"{TRINO_DIRECT}/v1/info", "Trino")
    wait_http(f"{ADMIN}/health", "QueryFlux admin")

    if args.dry_run_only:
        dry_run("bob", ["analysts"], f"SELECT name, region, ssn FROM {CUSTOMERS}")
        dry_run("bob", ["analysts"], f"SELECT * FROM {PAYROLL}")
        return 0

    if not args.skip_seed:
        seed()

    print_result(
        f"alice: SELECT name, region, ssn FROM {CUSTOMERS}",
        *trino_query("alice", "alice", f"SELECT name, region, ssn FROM {CUSTOMERS} ORDER BY id"),
    )
    print_result(
        f"bob:   SELECT name, region, ssn FROM {CUSTOMERS}  (EU + masked SSN)",
        *trino_query("bob", "bob", f"SELECT name, region, ssn FROM {CUSTOMERS} ORDER BY id"),
    )
    print_result(
        f"alice: SELECT name, salary FROM {PAYROLL}",
        *trino_query("alice", "alice", f"SELECT name, salary FROM {PAYROLL} ORDER BY id"),
    )
    print_result(
        f"bob:   SELECT name, salary FROM {PAYROLL}  (should deny)",
        *trino_query("bob", "bob", f"SELECT name, salary FROM {PAYROLL}"),
    )

    dry_run("bob", ["analysts"], f"SELECT name, region, ssn FROM {CUSTOMERS}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
