# Roadmap

shunt v0.1 works, but it is young and deliberately minimal. This page tracks what
it does **not** yet do and the order we intend to fix it. Issues and PRs are
welcome.

## Recently completed

- **Order-preserving conflict handling.** A staging conflict splits the candidate
  at the conflict point: earlier PRs are tested first, then the conflicting
  suffix is retried on the current base. If the conflict is already first in a
  candidate, that PR is bounced and the remaining suffix is re-queued. This
  replaces the former coarse conflict handling that could bounce a PR which only
  conflicted with a batch-mate that ultimately did not land.

## Current limitations

- **In-memory state.** The queue and the bisection frontier live in memory. A
  restart never double-merges or loses a PR (it re-derives the queue from the
  open auto-merge PRs), but it forgets an in-flight bisection and re-runs that
  CI. A crash mid-landing merges part of a batch and re-queues the rest next
  cycle.
- **Polling, not webhooks.** shunt reconciles on a timer (default 10s), so there
  is up-to-one-interval latency and steady API traffic. The
  `auto_merge_pull_request` webhook is not wired yet.
- **Single replica.** One queue manager, no HA. If it's down, PRs simply wait.
- **Serial per branch, serial bisection.** One batch is in flight at a time per
  `(repo, base)`, and the bisection tree is walked depth-first one node at a time.
  When more than one PR in a batch is broken, the independent subtrees that
  isolate them are tested sequentially even though they could run concurrently —
  so time-to-merge grows with the number of broken PRs. No speculative parallel
  batches, so throughput is bounded by gate-CI latency.
- **Global-only configuration.** Every knob is a process-wide environment
  variable; there are no per-repository overrides yet. Batch accumulation can be
  tuned globally, but not per repo.
- **No automated forge-integration tests.** The bisection state machine is unit
  tested with a mock; live API coverage is still manual.
- **Basic observability only.** Prometheus text metrics cover process-local queue
  depth and key counters, but there is no persisted metrics history, no
  time-in-queue histogram, and no queue UI (Forgejo has no plugin surface for a
  native one).
- **Merge commits only.** `rebase` and `squash` merge styles are intentionally
  disabled until their branch-protection and tested-tree semantics are verified.

## Milestones

### v0.2 — Durability
- Postgres-backed state: persist the per-`(repo, base)` work queue, the active
  batch (staging branch/SHA, members), and the bisection frontier; resume
  cleanly across restarts.
- Webhooks: react to `auto_merge_pull_request` (and `push`) to wake reconcile
  immediately, keeping the poll as a backstop.

### v0.3 — Throughput & configurability
- **Per-repository configuration.** A mechanism for per-repo overrides on top of
  the global defaults (the natural carrier is a small config file in the repo,
  e.g. `.shunt.yml`, discovered alongside the existing topic opt-in). This is a
  prerequisite for the two items below, which both need to be tunable globally
  **and** per repo.
- **Parallelizable bisection.** When a batch fails and splits, test the
  independent subtrees concurrently instead of strictly depth-first, so isolating
  N>1 broken PRs costs closer to the depth of the tree than the sum of its nodes.
  Bounded by a configurable fan-out limit (global default + per-repo override),
  since each parallel branch consumes a runner and a staging branch. Must
  preserve the invariants in `docs/design.md`: ascending merge order, and
  "every batch is validated against the real base it lands on"
  (parallel subtrees are staged speculatively on the pre-merge base, so a winning
  subtree is re-validated or ordered before it lands rather than fast-tracked).
- ~~**Configurable batch-linger window.** Before forming the first batch,
  optionally wait up to a duration *or* until a target number of PRs are ready
  (whichever comes first), so bursty and low-traffic repos batch intentionally.~~
  Completed: `SHUNT_BATCH_LINGER` and `SHUNT_BATCH_TARGET` provide a process-wide
  default; a linger duration of `0` preserves form-immediately behavior.
  Per-repository overrides remain covered by the pending per-repository
  configuration item above.

### v0.4 — Correctness & safety
- ~~Re-validate a PR's head SHA immediately before the gated merge (close the
  mid-test-update race).~~ Completed: the engine refetches each PR and verifies
  it is still open, unmerged, auto-merge scheduled, and still at the tested head
  SHA immediately before marking it successful and merging.
- Verify `rebase`/`squash` merge styles and `block_on_outdated_branch`
  interactions; document supported combinations.
- Least-privilege bot tokens (scope down from broad tokens; per-repo tokens).

### v0.5 — Observability & ops
- ~~Prometheus metrics (batches, runs, bounces, queue depth).~~ Completed:
  `/metrics` exposes process-local Prometheus text metrics for queue depth,
  active batch presence, batches started, PR merges, bounces, staging conflicts,
  reconcile errors, and terminal gate outcomes.
- Deeper observability: time-in-queue histograms, a status surface, and any
  persistence needed to preserve operational history across restarts.
- A queue status surface (a sticky per-repo PR comment and/or a small status
  page) since the forge can't host a native one.
- Staging-branch GC on startup; optional leader-elected HA.

### Validation
- Burn-in on a real, busy repository with a heavy end-to-end suite and
  concurrent contributors — the scenario shunt is built for, and the one that
  will surface the edges above.

## Non-goals (for now)

- Bot chat commands (`/merge`, priority labels). The auto-merge button is the
  one and only entry point by design.
- A native forge UI. Not possible without a plugin system Forgejo/Gitea don't
  have; a status page is the planned substitute.
