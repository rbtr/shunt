# Roadmap

shunt v0.1 works, but it is young and deliberately minimal. This page tracks what
it does **not** yet do and the order we intend to fix it. Issues and PRs are
welcome.

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
- **Serial per branch.** One batch is in flight at a time per `(repo, base)`. No
  speculative parallel batches, so throughput is bounded by gate-CI latency.
- **Coarse conflict handling.** A PR that conflicts only with a *batch-mate*
  (not the base) is bounced, even though it would merge fine on its own.
- **No automated forge-integration tests.** The bisection state machine is unit
  tested with a mock; live API coverage is still manual.
- **Limited observability.** Structured logs only — no metrics, no queue UI
  (Forgejo has no plugin surface for a native one).
- **Single-arch image** is published per build; no multi-arch manifest yet.
- **Merge commits only.** `rebase` and `squash` merge styles are intentionally
  disabled until their branch-protection and tested-tree semantics are verified.

## Milestones

### v0.2 — Durability
- Postgres-backed state: persist the per-`(repo, base)` work queue, the active
  batch (staging branch/SHA, members), and the bisection frontier; resume
  cleanly across restarts.
- Webhooks: react to `auto_merge_pull_request` (and `push`) to wake reconcile
  immediately, keeping the poll as a backstop.

### v0.3 — Correctness & safety
- Retry a conflicting PR on its own before bouncing it.
- Re-validate a PR's head SHA immediately before the gated merge (close the
  mid-test-update race).
- Verify `rebase`/`squash` merge styles and `block_on_outdated_branch`
  interactions; document supported combinations.
- Least-privilege bot tokens (scope down from broad tokens; per-repo tokens).

### v0.4 — Observability & ops
- Prometheus metrics (batches, runs, bounces, queue depth, time-in-queue).
- A queue status surface (a sticky per-repo PR comment and/or a small status
  page) since the forge can't host a native one.
- Multi-arch container manifest; staging-branch GC on startup; optional
  leader-elected HA.

### Validation
- Burn-in on a real, busy repository with a heavy end-to-end suite and
  concurrent contributors — the scenario shunt is built for, and the one that
  will surface the edges above.

## Non-goals (for now)

- Bot chat commands (`/merge`, priority labels). The auto-merge button is the
  one and only entry point by design.
- A native forge UI. Not possible without a plugin system Forgejo/Gitea don't
  have; a status page is the planned substitute.
