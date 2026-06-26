# Roadmap

shunt v0.4 is usable, but still intentionally small. This page tracks what it
does **not** yet do and the order we intend to fix it. Issues and PRs are
welcome.

## Recently completed

- **v0.2 release train.** The first post-launch hardening pass added pre-merge
  head revalidation, order-preserving staging-conflict handling, Prometheus
  metrics, configurable batch linger, configurable bisection fan-out, and
  `merge`/`squash`/`rebase` landing support.
- **v0.3 release train.** The next hardening pass added webhook wakeups,
  per-repository configuration, startup staging-branch cleanup, opt-in sticky
  queue-status comments, operator gotchas, and correct aggregation of Forgejo
  Actions run statuses.
- **Per-repository configuration.** Repos can add `.shunt.yml` to override safe
  queue tunables such as status context, merge style, batch sizing/linger,
  bisection fan-out, and managed base branch on top of process defaults.
- **Sticky PR queue-status comments.** Operators can opt in with
  `SHUNT_QUEUE_COMMENTS=true` to keep one updated status comment on each queued
  PR with base, position, state, and active batch details when known.
- **Source PR queue outcomes.** Landed, rejected, skipped, and merge-error PRs
  now get explicit source-head statuses and durable comments with staging
  run/commit links where available.
- **Order-preserving conflict handling.** A staging conflict splits the candidate
  at the conflict point: earlier PRs are tested first, then the conflicting
  suffix is retried on the current base. If the conflict is already first in a
  candidate, that PR is bounced and the remaining suffix is re-queued. This
  replaces the former coarse conflict handling that could bounce a PR which only
  conflicted with a batch-mate that ultimately did not land.
- **Startup staging-branch garbage collection.** When a queue is first managed,
  shunt deletes stale shunt-owned staging branches (`mq/<base>/staging` and
  `mq/<base>/staging-N`) left behind by earlier processes before it starts
  reconciling that `(repo, base)`.
- **Webhook wakeups.** Forgejo/Gitea webhooks now wake reconciliation promptly for
  auto-merge, pull-request, review, status, and push activity. The poll loop
  remains as a backstop for missed webhook deliveries.
- **Managed repository webhooks.** When `SHUNT_WEBHOOK_URL` is set, shunt uses
  its existing admin token to create or update matching repository webhooks,
  mirroring the branch-protection setup it already performs.
- **Forgejo Actions run readiness.** shunt now prefers Forgejo's run-level
  aggregate status for staging branches before falling back to task aggregation,
  so multi-job gates are not considered green while dependent jobs are still
  being materialized.
- **Env-gated forge integration harness.** Live Forgejo/Gitea client checks can
  now exercise PR listing, timeline auto-merge detection, status posting,
  workflow-run lookup, and optional branch-protection updates without requiring
  credentials in default CI.

## Current limitations

- **Durable state is opt-in.** By default, shunt still runs with in-memory state.
  Set `SHUNT_STATE_PATH` to enable the built-in bbolt checkpoint store so pending
  candidates, active batch metadata, linger state, and bisection counters survive
  restarts. Restored active batches are re-staged before landing. A crash
  mid-landing can still merge part of a batch and re-queue the rest next cycle.
- **Polling backstop.** shunt still reconciles on a timer (default 10s) to
  tolerate missed webhooks, so there is some steady API traffic even when webhook
  wakeups are configured.
- **Single replica.** One queue manager, no HA. If it's down, PRs simply wait.
  Same-host file locks are intentionally not supported as a leadership scheme;
  safe multi-replica operation needs a durable external lease/lock.
- **Serial initial batches.** One rollup batch is seeded at a time per
  `(repo, base)`. Bisection can fan out, but shunt still avoids speculative
  parallel batches from fresh queue entries.
- **No automated live-forge burn-in.** The default test suite now includes a
  local engine + native-git burn-in for bisection and landing, and the forge
  client has an env-gated live integration harness, but disposable-repo and
  busy-repo live queue burn-in are still manual.
- **Process-local observability only.** Prometheus text metrics, JSON status, and
  optional sticky PR queue-status comments cover process-local queue depth,
  active/pending PR numbers, oldest queued PR age, time-in-queue histograms, and
  key counters. There is still no persisted metrics history and no native queue
  UI (Forgejo has no plugin surface for one).

## Milestones

### v0.3 — Durability
- ~~Engine checkpoint/resume boundary and default local store: load and save the
  per-`(repo, base)` work queue, active batch metadata, linger timestamp, and
  bisection counters through a pluggable store.~~ Completed with an opt-in bbolt
  implementation via `SHUNT_STATE_PATH`; restored active batches are re-staged
  on restart.
- ~~Postgres checkpoint store: schema plus a tested implementation of the engine
  checkpoint boundary can persist the per-`(repo, base)` work queue, active batch
  metadata, linger timestamp, and bisection counters.~~ Completed with runtime
  configuration via `SHUNT_POSTGRES_DSN`; migrations are applied at startup.
- ~~Webhooks: react to `auto_merge_pull_request` (and `push`) to wake reconcile
  immediately, keeping the poll as a backstop.~~ Completed: `/webhook` wakes the
  reconcile loop for auto-merge, pull-request, review, status, and push events
  while retaining `SHUNT_POLL_INTERVAL` as a backstop.

### v0.4 — Per-repo configurability
- ~~**Per-repository configuration.** A mechanism for per-repo overrides on top
  of the global defaults (the natural carrier is a small config file in the
  repo, e.g. `.shunt.yml`, discovered alongside the existing topic opt-in).~~
  Completed: `.shunt.yml` supports status context, merge style, max batch,
  batch linger/target, bisection fan-out, and base-branch overrides.
- ~~**Parallelizable bisection.** When a batch fails and splits, test independent
  subtrees concurrently instead of strictly depth-first, bounded by a configurable
  fan-out limit.~~ Completed: `SHUNT_BISECT_FANOUT` controls process-wide
  bisection concurrency; a value of `1` preserves serial behavior. Ordered
  landing is still enforced, and later speculative runs are re-staged if an
  earlier candidate advances the base. Per-repository overrides are available via
  `.shunt.yml`.
- ~~**Configurable batch-linger window.** Before forming the first batch,
  optionally wait up to a duration *or* until a target number of PRs are ready
  (whichever comes first), so bursty and low-traffic repos batch intentionally.~~
  Completed: `SHUNT_BATCH_LINGER` and `SHUNT_BATCH_TARGET` provide a process-wide
  default; a linger duration of `0` preserves form-immediately behavior.
  Per-repository overrides are available via `.shunt.yml`.

### v0.5 — Correctness & safety
- ~~Re-validate a PR's head SHA immediately before the gated merge (close the
  mid-test-update race).~~ Completed: the engine refetches each PR and verifies
  it is still open, unmerged, auto-merge scheduled, and still at the tested head
  SHA immediately before marking it successful and merging.
- ~~Verify `rebase`/`squash` merge styles and `block_on_outdated_branch`
  interactions; document supported combinations.~~ Completed: `SHUNT_MERGE_STYLE`
  now accepts `merge`, `squash`, and `rebase`; shunt still pins the expected PR
  head SHA at merge time and documents keeping "block on outdated branch"
  disabled so queue validation remains authoritative.
- ~~Least-privilege bot token guidance (scope down from broad tokens; per-repo
  tokens).~~ Completed: README and design docs now describe the minimum
  repository permission categories and secret-storage guidance without relying on
  forge-version-specific token scope names.

### v0.6 — Observability & ops
- ~~Prometheus metrics (batches, runs, bounces, queue depth).~~ Completed:
  `/metrics` exposes process-local Prometheus text metrics for queue depth,
  active batch presence, batches started, PR merges, bounces, staging conflicts,
  reconcile errors, and terminal gate outcomes.
- ~~Process-local queue age and time-in-queue histograms.~~ Completed:
  `/metrics` now exposes the oldest PR age currently known in memory and
  time-in-queue histograms for merged, bounced, and dropped PRs. These reset on
  restart until durable queue state/metrics history exists.
- ~~A sticky per-repo PR queue-status comment.~~ Completed as an opt-in
  (`SHUNT_QUEUE_COMMENTS=true`) so write traffic remains an operator choice.
- ~~A queue status surface (a sticky per-repo PR comment and/or a small status
  page) since the forge can't host a native one.~~ Completed: `/status` exposes
  safe process-local JSON and `/status.html` renders a small human page with
  queue identity, depth, active batches, and pending PR-number batches.
- ~~PR-visible rejection/debug feedback.~~ Completed: terminal queue outcomes
  update source PR statuses and post durable comments with staging debug links
  where available.
- Deeper observability: persisted metrics history and historical status views to
  preserve operational history across restarts.
- ~~Staging-branch GC on startup.~~ Completed: stale shunt-owned staging branches
  are pruned on startup or first discovery before reconciliation begins for a
  managed `(repo, base)`.
- Optional leader-elected HA backed by a durable external lease/lock; same-host
  file-lock standby is not a supported HA mode.

### Validation
- ~~Local end-to-end burn-in through the engine and native git staging.~~
  Completed: the default test suite exercises a failed multi-PR batch, bisection,
  bad-PR bounce, and successful landing of the good PRs using a real temporary git
  repository.
- Live burn-in on a real, busy repository with a heavy end-to-end suite and
  concurrent contributors — the scenario shunt is built for, and the one that
  will surface forge-specific edges.

## Non-goals (for now)

- Bot chat commands (`/merge`, priority labels). The auto-merge button is the
  one and only entry point by design.
- A native forge UI. Not possible without a plugin system Forgejo/Gitea don't
  have; a separate status page can be built on the process-local `/status` API
  if operators need one.
