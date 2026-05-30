---
sidebar_position: 6
title: Contributing
description: Contribution guidelines for QueryFlux — PR conventions, code style, tests, documentation updates, and Apache 2.0 license.
image: img/queryflux-hero-banner.png
---
# Contributing to QueryFlux

Thank you for helping improve QueryFlux. This document describes how we expect contributions to look so reviews stay fast and the codebase stays consistent.

## License

By contributing, you agree that your contributions will be licensed under the same terms as the project ([Apache License 2.0](https://github.com/lakeops-org/queryflux/blob/main/LICENSE)).

## Before you start

- Skim the **[architecture documentation overview](/docs/architecture/overview)** (especially **[System map](/docs/architecture/system-map)**) so changes fit the existing layers: frontends → routing → cluster manager → translation → engine adapters → persistence.
- For local builds and tests, follow **[Development](/docs/development)**.

## Pull requests

1. **Keep changes focused.** One logical change per PR is easier to review than a large refactor mixed with a feature fix.
2. **Match existing style.** Rust: same patterns as surrounding code, `rustfmt`-compatible formatting, and no new Clippy warnings (see checks below).
3. **Test your change.**
   - Run `make lint` (Clippy with `-D warnings`) and `make test` (unit tests); no Docker required.
   - If you touch integration behavior (routing, Trino HTTP, adapters), run `make test-e2e` when you can (Docker required).
4. **Update docs when behavior changes.** Config keys, router types, or public HTTP/admin behavior should be reflected in `README.md`, `website/docs/`, or `config.example.yaml` as appropriate.

## Code review expectations

- PR description should state **what** changed and **why** in plain language.
- Link related issues if any.
- Breaking config or API changes should be called out explicitly.

## What we are likely to accept

- Bug fixes with a clear repro or failing test.
- Tests that lock in existing behavior.
- Small, well-scoped features that align with the architecture (new router, adapter improvement, metrics).
- Documentation fixes and operational notes.

## What needs discussion first

- Large structural rewrites, new dependencies, or changes that alter security boundaries (auth, TLS, multi-tenant behavior).
- Backward-incompatible configuration changes (prefer migration paths or deprecation when possible).

## Conduct

Be respectful and assume good intent. Technical disagreement should stay about the code and the problem, not the person.

If you are unsure whether an idea fits, open an issue with a short design sketch before investing in a big PR.

The same content is maintained in the repository as [`contribute.md`](https://github.com/lakeops-org/queryflux/blob/main/contribute.md).
