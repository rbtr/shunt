# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Structured JSON logging with stable fields for daemon lifecycle, webhook,
  manager, and queue events.
- JSON `/status` endpoint with safe process-local queue identity, depth, and
  active/pending PR-number batches, complementing `/metrics` and optional
  sticky PR comments.
- Optional managed repository webhook setup via `SHUNT_WEBHOOK_URL`, reusing the
  existing admin token to create or update shunt-owned hooks.
- Source PRs now receive explicit terminal queue feedback: landed, rejected,
  skipped/requeued, and merge-error outcomes include commit statuses, durable
  comments, and staging debug links when available.
- Optional bbolt queue checkpoint storage via `SHUNT_STATE_PATH`, preserving
  pending candidates, active batch metadata, linger state, and bisection counters
  across restarts; restored active batches are re-staged before landing.
- Process-local queue-age gauge and time-in-queue histogram metrics for PRs that
  merge, bounce, or drop out of the in-memory queue.

### Fixed
- Reconcile, checkpoint, forge API, and git staging operations now receive the
  process context so shutdown can cancel in-flight work cleanly.
- Forgejo Actions readiness now prefers the run-level aggregate status before
  falling back to task aggregation, avoiding early landings while dependent jobs
  are still being materialized.

### Changed
- Raised the minimum supported Go version to 1.25 and added CI race/vulnerability
  checks.

## [0.3.0] - 2026-06-23

v0.3.0 adds the first operational hardening layer on top of the v0.2 queue:
webhook-driven wakeups, per-repository configuration, safer staging cleanup,
and clearer queue visibility. It also fixes Forgejo task-status aggregation so
multi-job checks are interpreted correctly.

### Added
- `/webhook` endpoint support for auto-merge, pull-request, review, status, and
  push events. Webhooks wake reconciliation promptly while the poll loop remains
  as a backstop.
- Per-repository `.shunt.yml` overrides for status context, merge style, max
  batch size, batch linger/target, bisection fan-out, and managed base branch.
- Opt-in sticky queue-status PR comments via `SHUNT_QUEUE_COMMENTS=true`,
  updating one bot-owned comment per queued PR instead of spamming new comments.
- Startup cleanup for stale shunt-owned staging branches left behind by prior
  processes.
- Operator guidance for Forgejo/Gitea behaviors that commonly affect queue
  correctness.

### Fixed
- Forgejo action-task aggregation now treats a commit as successful only when
  all relevant task statuses have succeeded, avoiding false positives from
  mixed job results.

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

[Unreleased]: https://github.com/rbtr/shunt/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/rbtr/shunt/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/rbtr/shunt/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/rbtr/shunt/releases/tag/v0.1.0
