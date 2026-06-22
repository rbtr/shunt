# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0] - 2026-06-22

v0.2.0 is the first post-launch hardening release. It focuses on queue
correctness under real traffic, intentional batching, observability, and broader
Forgejo/Gitea merge-method support.

### Added
- Prometheus `/metrics` endpoint with queue depth, active batch presence,
  batches started, PR merges, bounces, staging conflicts, reconcile errors, and
  terminal gate outcome counters.
- Configurable batch accumulation via `SHUNT_BATCH_LINGER` and
  `SHUNT_BATCH_TARGET`.
- Configurable bisection fan-out via `SHUNT_BISECT_FANOUT`, allowing independent
  failing-batch subtrees to test concurrently while preserving ordered landing.
- `SHUNT_MERGE_STYLE=squash` and `SHUNT_MERGE_STYLE=rebase` support, alongside
  the existing merge-commit behavior.

### Changed
- Staging conflicts now preserve queue order: if a later PR conflicts with an
  earlier batch-mate, shunt tests the earlier prefix first and retries the
  conflicting suffix against the real current base before bouncing anything.
- PR heads are revalidated immediately before setting the required status and
  merging, closing the mid-test update race.
- Roadmap and design documentation now describe the current batching,
  bisection, metrics, and merge-method behavior.

## [0.1.0] - 2026-06-21

Initial release.

### Added
- Rollup-batch + **bisection** merge queue for Forgejo (Gitea-compatible API).
- Auto-merge-button trigger — PRs enter the queue via "Merge when checks
  succeed", detected from the PR timeline. No bot commands.
- Status-gated native merge landing (sets a required `merge-queue` commit status
  then merges via the API); bisection isolates the bad PR(s) on gate failure.
- Multi-repo discovery by topic, with automatic branch-protection setup.
- Single static binary + container image; `/healthz` endpoint.
- Release packaging for downloadable binaries/checksums, multi-arch GHCR
  images, an OCI Helm chart, and Helm/Kustomize install manifests.
- Mock-driven unit tests for the bisection state machine.

### Security
- Git remotes are kept credential-free; staging pushes authenticate through
  non-interactive Git credential prompts instead of embedding tokens in clone
  URLs.

[Unreleased]: https://github.com/rbtr/shunt/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/rbtr/shunt/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/rbtr/shunt/releases/tag/v0.1.0
