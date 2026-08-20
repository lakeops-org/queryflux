---
name: clippy-ci
description: >-
  Run QueryFlux Clippy the same way CI does (-D warnings, workspace, exclude
  queryflux-bench) and fix failures before opening or updating a PR. Use when
  writing or changing Rust, creating or pushing a PR, CI Clippy fails, or the
  user mentions clippy, lint, or make lint.
---

# Clippy (match CI)

CI job `Clippy` in `.github/workflows/ci.yml` treats warnings as errors. Do not push Rust changes until this command exits 0.

## Command

```bash
CARGO_TARGET_DIR=target cargo clippy --workspace --all-targets --all-features --exclude queryflux-bench -- -D warnings
```

- `CARGO_TARGET_DIR=target` avoids sandbox/DuckDB cache breakage (same as tests).
- `--all-targets` includes tests and examples. Tests in the middle of a file fail `clippy::items_after_test_module`.
- `--exclude queryflux-bench` matches CI (bench is built in `benchmark.yml`).
- Local shortcut: `make clippy` / `make lint` (must use the same flags as CI).

Also run `cargo fmt --all` and `cargo fmt --all -- --check` before push (see `.cursor/rules/pr-formatting.mdc`).

## Before every Rust PR

1. `cargo fmt --all`
2. Run the Clippy command above.
3. Fix every error. Do not `#[allow(clippy::…)]` unless the lint is a false positive and surrounding code already allows it.
4. Re-run Clippy until exit 0.
5. Include the fixes in the commit. Do not push with a red Clippy job.

## Common failure: `items_after_test_module`

`#[cfg(test)] mod tests { … }` must be the **last** item in the file. If you add helpers or impls after the test module, move the test module to the end.

## After a Clippy CI failure

1. Open the failed job log and copy the `error:` lines (`clippy::…`).
2. Fix on the PR branch, fmt, re-run Clippy locally, push.
3. Do not re-run the workflow hoping it is flaky; `-D warnings` failures are real.
