# Local protocol extensions

These crates retain their upstream licenses and versions. Root Cargo.toml applies
local patches; remove those patches when equivalent upstream support is available.

- mysql_async 0.36.1: answer authentication_openid_connect_client with a bounded,
  length-encoded JWT; only allow encrypted or local socket transport. Password
  authentication is retained. JWT response framing has a unit test.
- opensrv-clickhouse 0.7.0: accept an abstract TLS-capable transport, honor the
  negotiated revision, bound untrusted strings/compressed blocks/dimensions, and
  require exact decompression size. QueryFlux provides authentication and dispatch.

Both modifications require integration validation before release.
