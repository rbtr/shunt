# Roadmap

shunt v0.3 is usable, but still intentionally small. This page tracks what it
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
  action task statuses.
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

## Current limitations

- **In-memory state.** The queue and the bisection frontier live in memory. A
  restart never double-merges or loses a PR (it re-derives the queue from the
  open auto-merge PRs), but it forgets an in-flight bisection and re-runs that
  CI. A crash mid-landing merges part of a batch and re-queues the rest next
  cycle.
- **Polling backstop.** shunt still reconciles on a timer (default 10s) to
  tolerate missed webhooks, so there is some steady API traffic even when webhook
  wakeups are configured.
- **Single replica.** One queue manager, no HA. If it's down, PRs simply wait.
- **Serial initial batches.** One rollup batch is seeded at a time per
  `(repo, base)`. Bisection can fan out, but shunt still avoids speculative
  parallel batches from fresh queue entries.
- **No automated forge-integration tests.** The bisection state machine is unit
  tested with a mock; live API coverage is still manual.
- **Basic observability only.** Prometheus text metrics cover process-local queue
  depth and key counters, and operators can enable sticky PR queue-status
  comments. There is still no persisted metrics history, no time-in-queue
  histogram, and no queue UI (Forgejo has no plugin surface for a native one).

## Milestones

### v0.3 — Durability
- Postgres-backed state: persist the per-`(repo, base)` work queue, the active
  batch (staging branch/SHA, members), and the bisection frontier; resume
  cleanly across restarts.
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
  earlier candidate advances the base. Per-repository overrides remain covered by
  the pending per-repository configuration item above.
- ~~**Configurable batch-linger window.** Before forming the first batch,
  optionally wait up to a duration *or* until a target number of PRs are ready
  (whichever comes first), so bursty and low-traffic repos batch intentionally.~~
  Completed: `SHUNT_BATCH_LINGER` and `SHUNT_BATCH_TARGET` provide a process-wide
  default; a linger duration of `0` preserves form-immediately behavior.
  Per-repository overrides remain covered by the pending per-repository
  configuration item above.

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
- Least-privilege bot tokens (scope down from broad tokens; per-repo tokens).

### v0.6 — Observability & ops
- ~~Prometheus metrics (batches, runs, bounces, queue depth).~~ Completed:
  `/metrics` exposes process-local Prometheus text metrics for queue depth,
  active batch presence, batches started, PR merges, bounces, staging conflicts,
  reconcile errors, and terminal gate outcomes.
- Deeper observability: time-in-queue histograms, a status surface, and any
  persistence needed to preserve operational history across restarts.
- ~~A sticky per-repo PR queue-status comment.~~ Completed as an opt-in
  (`SHUNT_QUEUE_COMMENTS=true`) so write traffic remains an operator choice.
  A small status page remains a possible follow-up since the forge can't host a
  native queue UI.
- ~~PR-visible rejection/debug feedback.~~ Completed: terminal queue outcomes
  update source PR statuses and post durable comments with staging debug links
  where available.
- ~~Staging-branch GC on startup.~~ Completed: stale shunt-owned staging branches
  are pruned on startup or first discovery before reconciliation begins for a
  managed `(repo, base)`.
- Optional leader-elected HA.

### Validation
- Burn-in on a real, busy repository with a heavy end-to-end suite and
  concurrent contributors — the scenario shunt is built for, and the one that
  will surface the edges above.

## Non-goals (for now)

- Bot chat commands (`/merge`, priority labels). The auto-merge button is the
  one and only entry point by design.
- A native forge UI. Not possible without a plugin system Forgejo/Gitea don't
  have; a status page is the planned substitute.
