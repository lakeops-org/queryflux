# Contributing to QueryFlux

Thank you for helping improve QueryFlux. This document describes how we expect contributions to look so reviews stay fast and the codebase stays consistent.

## License

By contributing, you agree that your contributions will be licensed under the same terms as the project ([Apache License 2.0](LICENSE)).

## Before you start

- Skim [docs/README.md](docs/README.md) (especially [architecture.md](docs/architecture.md)) so changes fit the existing layers: frontends → routing → cluster manager → translation → engine adapters → persistence.
- For local builds and tests, follow [development.md](development.md).

## Pull requests

1. **Keep changes focused.** One logical change per PR is easier to review than a large refactor mixed with a feature fix.
2. **PR title:** use Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `ui:`, optional `(scope)` and `!`); CI checks [.github/pr-title-checker-config.json](.github/pr-title-checker-config.json). This repo **squash-merges**, and [Release Please](https://github.com/googleapis/release-please) uses that squash commit (the PR title) for [CHANGELOG.md](CHANGELOG.md) and GitHub Release notes. Use `feat:`, `fix:`, or `ui:` for user-facing changes so they appear; `docs:`, `chore:`, and `test:` are omitted.
3. **Match existing style.** Rust: same patterns as surrounding code, `rustfmt`-compatible formatting, and no new Clippy warnings (see checks below).
4. **Test your change.**
   - Run `make clippy` (same flags as CI: `-D warnings`, exclude `queryflux-bench`) and `make test`. Do not open or update a Rust PR until Clippy exits 0.
   - If you touch integration behavior (routing, Trino HTTP, adapters), run `make test-e2e` when you can (Docker required).
5. **Update docs when behavior changes.** Config keys, router types, or public HTTP/admin behavior should be reflected in `README.md`, `docs/`, or `config.example.yaml` as appropriate.

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

## Releases and container images
 

Releases publish **one** multi-arch QueryFlux server image to **GitHub Container Registry** ([GHCR](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)) and create entries on the GitHub [**Releases**](https://github.com/lakeops-org/queryflux/releases) page.

| Image | Build | Meaning |
|-------|-------|---------|
| **`ghcr.io/lakeops-org/queryflux`** | [`docker/queryflux/Dockerfile`](docker/queryflux/Dockerfile) (`runtime-server`) | QueryFlux server (frontends + Admin API); no UI |
| **`ghcr.io/lakeops-org/queryflux-studio`** | [lakeops-org/queryflux-studio](https://github.com/lakeops-org/queryflux-studio) | Studio admin UI, built and released in its own repo |

Studio is never built here. Run it next to the server with `ADMIN_API_URL` pointing at the admin port. The compose examples do this with a `queryflux-studio` service, and the Helm chart does it with a `studio` sidecar container (`studio.enabled`).

Release images are built for **linux/amd64** and **linux/arm64** only.

| Trigger | Workflow | Tags |
|---------|----------|------|
| Push to `main` | [release.yml](.github/workflows/release.yml) | `:latest` (alias `:latest-slim`) |
| Release Please release | [release.yml](.github/workflows/release.yml) | `:vX.Y.Z` (alias `:vX.Y.Z-slim`), plus `:latest` |
| Manual, any ref | [docker-publish-tag.yml](.github/workflows/docker-publish-tag.yml) | `:<image_tag>` (alias `:<image_tag>-slim`) |

The `-slim` aliases exist because `:latest` / `:vX.Y.Z` used to be a server + Studio image and `-slim` the server-only one. They point at the same image now, so existing `-slim` pins keep working. Images up to `v0.4.0` still have the old meaning.

**How builds run:** **linux/amd64** images build on **`ubuntu-latest`**; **linux/arm64** on **`ubuntu-24.04-arm`** (native ARM, so no QEMU for compiles). A manifest job then merges the two digests with `docker buildx imagetools create`, similar to [lakekeeper/lakekeeper’s release workflow](https://github.com/lakekeeper/lakekeeper/blob/main/.github/workflows/release.yml).

Release workflows use **`GITHUB_TOKEN`** with **`packages: write`** (push + manifests), **`actions: write`** (BuildKit **GHA cache**, per-arch scopes `queryflux-slim-amd64` / `queryflux-slim-arm64`), and **`contents: write`** on the publish job. Optional **`GHCR_IMAGE_NAME`**. PRs verify Dockerfiles in [docker-verify.yml](.github/workflows/docker-verify.yml).

**Cold or slow runs:** First push after dependency changes may still be heavy (Rust + PyO3 + DuckDB on both arches). Caches speed repeat runs. ARM64 requires **GitHub-hosted ARM runners** available for the repo (public repos typically have `ubuntu-24.04-arm64`).

After the first push, open the package in the org/repo **Packages** settings and set visibility to **public** if you want anonymous `docker pull`. Private packages require `docker login ghcr.io` with a PAT or `GITHUB_TOKEN` that has `read:packages`.

Local build then push to GHCR (replace `OWNER/REPO`):

```bash
echo "$GITHUB_TOKEN" | docker login ghcr.io -u OWNER --password-stdin
docker buildx build --platform linux/amd64,linux/arm64 -f docker/queryflux/Dockerfile -t ghcr.io/OWNER/REPO:latest --push .
```

Release notes come from Release Please ([`.github/workflows/release.yml`](.github/workflows/release.yml)): squash-merged `feat:` / `fix:` / `ui:` PR titles land in the next `CHANGELOG.md` and GitHub Release. `chore(deps):` and `docs:` do not.

To add or drop platforms, extend the `matrix.include` lists in the release workflows (and add/remove sources in the `imagetools create` steps).

## Conduct

Be respectful and assume good intent. Technical disagreement should stay about the code and the problem, not the person.

If you are unsure whether an idea fits, open an issue with a short design sketch before investing in a big PR.
