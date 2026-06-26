# Design

shunt is a small external service that adds a merge queue to Forgejo (and
Gitea-compatible forges) using only their REST API. This document explains the
algorithm and — just as important — the forge mechanics it depends on, several
of which are non-obvious and were established empirically against Forgejo 15.x.

## Why an external bot (and not a plugin)

Forgejo and Gitea have **no in-process plugin system** and no GitHub App
equivalent — by deliberate design. The supported extension points are the REST
API, outbound webhooks, Actions (CI), and OAuth/OIDC. So a merge queue must run
*outside* the forge and drive it through the API. The consequence: shunt
communicates through commit statuses, PR comments, and branch names rather than
native UI. That's a platform ceiling, not a design choice.

The bot token should be limited to the repositories shunt manages. At minimum,
shunt needs API permission to read PR, timeline, commit-status, and workflow-run
state; manage branch protection for opted-in repos; create or update the
shunt-managed repository webhook when enabled; create source-head statuses; merge
PRs; write outcome/queue comments; cancel scheduled auto-merge; and, via Git,
fetch PR heads and push/delete only `mq/...` staging branches. Current
Forgejo/Gitea branch-protection and repository-hook APIs require repository
admin permission, so the admin grant is scoped by repository rather than avoided
entirely. Store the token in the runtime secret store, not in this repository.

## Forge mechanics (validated)

These are the facts shunt is built on. Several differ from how GitHub behaves.

1. **Auto-merge is detected from the PR timeline, not the PR object.** The PR
   API object does not expose a pending "merge when checks succeed". Instead the
   issue timeline (`GET /repos/{o}/{r}/issues/{n}/timeline`) contains
   `pull_scheduled_merge` / `pull_cancel_scheduled_merge` events. Scanning
   newest-first, the first of those wins.

2. **A fast-forward does NOT auto-mark PRs merged.** Pushing the base branch to
   a tested staging commit (the classic bors trick) leaves the PRs *open* even
   though their heads are ancestors of the base. So shunt does not fast-forward.
   Instead it lands each PR through the forge's own merge:
   - set the required `merge-queue` commit status to `success` on the PR head
     (`POST /repos/{o}/{r}/statuses/{sha}`), then
   - `POST /repos/{o}/{r}/pulls/{n}/merge`.
   The forge performs the configured merge method (`merge`, `squash`, or
   `rebase`) and records the PR as properly merged.

3. **The required status check is the gate.** With branch protection requiring
   the `merge-queue` status and restricting pushes to the bot, a PR merge is
   rejected (`405 "Not all required status checks successful"`) until shunt sets
   that status — which it only does after a batch passes. Nothing lands
   un-vetted, and humans can't bypass the queue. The bot must have repository
   admin permission, not only write permission, because Forgejo/Gitea require
   admin access for the branch-protection API shunt uses to read and reconcile
   this rule.

4. **CI result is the Actions run `status`.** Forgejo Actions don't publish a
   pollable commit *status*; shunt reads the newest matching workflow run from
   `GET /repos/{o}/{r}/actions/runs`, matched on `(commit_sha, prettyref)`, and
   uses that run's aggregate `status` (`success` / `failure` / `running` / …).
   This is intentionally run-level instead of task-level: Forgejo creates
   dependent job rows lazily, so a multi-job workflow can briefly show every
   materialized task as green while later jobs are not even present yet. shunt
   falls back to `GET /repos/{o}/{r}/actions/tasks` only when the run endpoint is
   unavailable on older compatible forges. Scope your gate workflow to
   `push: [mq/**]` so per-PR merges to the base don't re-trigger it.

## The algorithm

State, per `(repo, base)`:

- `pending [][]int` — a work queue of **candidate batches** (each a list of PR
  numbers, in order).
- `active` — candidates currently being tested (PRs + staging branch + SHA).
- `lingerSince` — when the idle engine first saw ready PRs while the optional
  batch-accumulation window was active.

The engine has a checkpoint boundary around that state: when configured with its
consumer-side `CheckpointStore`, it loads one queue snapshot before the first
reconcile tick, saves after each tick that leaves queue work in progress, and
deletes the snapshot once the queue is idle. The production command wires the
default bbolt store when `SHUNT_STATE_PATH` is set; otherwise releases keep the
historical process-local state.

Each `Reconcile()` tick advances one step. Ticks are driven by relevant
Forgejo/Gitea webhooks when available, with `SHUNT_POLL_INTERVAL` as the
backstop so a missed webhook only delays progress:

```
for active candidate whose earlier candidates are resolved:
    status = RunStatus(active.staging_sha, staging_branch)
    success            -> land(active.prs); requeue later speculative runs if base advanced
    failure            -> bisectOrBounce(active)
    running/unknown    -> wait

while active count < bisection fan-out:
    if pending empty:
        ready = ready auto-merge PRs
        if ready empty: clear linger; return
        if batch_linger > 0 and (batch_target == 0 or ready below batch_target)
           and window not expired:
            remember first-ready time; return
        pending = [[ ready ]]              # re-seed
        clear linger
    cand = pending.pop_front()
    prs  = resolve(cand)                 # drop closed / no-longer-auto-merge
    sha, conflictPR = BuildStaging(base, "mq/<base>/staging", prs)
    if conflictPR and conflictPR is first:
        bounce(conflictPR); requeue(items after conflictPR); return
    if conflictPR:
        prefix = items before conflictPR
        suffix = conflictPR and following items
        requeue(prefix, suffix)           # prefix keeps its queue position
        return
    active += { prs, staging_branch, staging_sha, base_generation }
```

`SHUNT_BATCH_LINGER` and `SHUNT_BATCH_TARGET` only apply when seeding the first
candidate batch from the ready queue. Bisection candidates already in `pending`
start immediately. A linger duration of `0` preserves form-immediately behavior;
with a non-zero duration, shunt waits until either the target count is reached or
the window expires. These knobs have process-wide defaults and can be overridden
per repository with `.shunt.yml`.

`SHUNT_BISECT_FANOUT` caps concurrent bisection staging runs per queue. A value
of `1` preserves serial bisection. Higher values use sibling staging branches
such as `mq/main/staging-1`, `mq/main/staging-2`, so the gate workflow must keep
matching `mq/**`.

`land` sets the source-head status and merges each PR in order. Terminal queue
outcomes are also written back to source PRs:

- landed PRs get a durable comment with the staging commit/run link;
- bounced PRs get auto-merge cancelled, a source-head `failure` status for gate
  failures or `error` for staging/infrastructure failures, and a durable
  rejection comment;
- skipped/requeued PRs get an `error` status plus a comment explaining why they
  were not landed.

If sticky queue comments are enabled, terminal outcomes update the sticky comment
too. Intermediate multi-PR batch failures are not broadcast to every source PR;
bisection first isolates the terminal culprit so innocent PRs are not blamed or
spammed. `bisectOrBounce`:

```
nums = active.prs
if len(nums) == 1:  bounce(nums[0])                      # the culprit
else:               mid = len/2
                    pending.push_front(nums[:mid], nums[mid:])   # test first half next
```

Because candidates are just lists of PR numbers and staging is always rebuilt
from the *current* base tip, a successful sub-batch that advances the base is
handled safely: any later speculative staging run from the old base generation is
deleted and re-queued, then re-staged before it can land.

The neutral `checkpoint` package defines the snapshot DTOs. The engine owns the
consumer-side store interface, and the concrete bbolt implementation lives in
the more specific `checkpoint/bolt` package. Restored active batches are
conservatively re-queued for fresh staging instead of resuming an old staging
branch/run, so startup staging-branch cleanup cannot delete a branch that the
restored engine still expects to observe. The production command wires the
default bbolt implementation when `SHUNT_STATE_PATH` is set. That store persists
one snapshot per `(owner, repo, base)` and keeps the binary static/CGO-free;
operators should place the database on persistent storage if they want queue
state to survive pod replacement or host reboots.

### Worked example

Four ready PRs `[1 2 3 4]`, where `3` is broken:

```
test [1 2 3 4]  -> fail  -> bisect [1 2] [3 4]
test [1 2]      -> pass  -> merge 1, 2
test [3 4]      -> fail  -> bisect [3] [4]
test [3]        -> fail  -> bounce 3            (culprit isolated)
test [4]        -> pass  -> merge 4
```

`1, 2, 4` land; `3` is bounced with a failed source-head status, auto-merge
cancelled, and a comment linking to the failing staging run/commit. Five CI runs
instead of four per-PR runs, but the common all-green case is a single run.

For staging conflicts, the split point is the PR that failed to apply:

```
stage [1 2 3] conflicts applying 2 -> queue [1] then [2 3]
test [1] passes and lands          -> [2 3] is re-staged on the new base
stage [2 3] still conflicts at 2   -> bounce 2, queue [3]
```

If `[1]` fails or is skipped and does not land, `[2 3]` is instead retried
against the unchanged current base, so `2` can still pass.

The default test suite includes a local burn-in scenario for this path. It uses a
temporary real git repository, the production git staging implementation, and a
test forge adapter to stage a multi-PR batch, fail the batch gate, bisect to the
bad PR, bounce it, and land the good PRs.

## Correctness

- **Nothing lands un-tested.** Branch protection blocks merges without the
  `merge-queue` status, which only shunt sets, and only after a green staging
  run that *included that exact PR*.
- **Innocent PRs survive a bad batch.** Only a PR that fails in a size-1 batch is
  bounced; everything else is re-tested in a sub-batch and merged.
- **Interaction failures** (A and B each pass alone but fail together) are
  attributed to whichever is tested second on top of the other — equivalent to
  bors, and acceptable in practice.
- **Staging conflicts preserve queue order.** If PR `B` conflicts after earlier
  PR `A` in the same candidate, shunt tests `A` first and retries `B` only after
  the real base reflects whether `A` landed. If `A` does not land, `B` can still
  pass on the unchanged base; if `A` lands and `B` is now first and still
  conflicts, `B` is bounced. A conflict on the first PR in a candidate means
  that PR conflicts with the current base, so it is bounced and the remaining
  suffix is re-queued.
- **Parallel bisection preserves ordered landing.** Later speculative subtrees may
  run before earlier ones finish, but their results cannot land or bounce ahead of
  lower-numbered candidates. If an earlier successful candidate advances the base,
  later speculative branches are discarded and re-staged on the new base before
  any PR lands.
- **Merge method is an API landing choice, not a staging shortcut.** shunt always
  tests the integration tree on a staging branch first, then asks the forge to
  land each PR with the configured `merge`, `squash`, or `rebase` method while
  pinning the expected PR head SHA. The configured method must be allowed by the
  repository; squash-only repositories need `SHUNT_MERGE_STYLE=squash`. Keep
  "block on outdated branch" disabled so the queue, not per-PR freshness checks,
  decides when a tested PR can land.
- **Crash safety.** By default, state is still in-memory; a restart re-derives
  the queue from open auto-merge PRs. It may repeat a staging run, but never
  double-merges and never loses a PR. With `SHUNT_STATE_PATH`, shunt persists the
  pending frontier, linger state, bisection counters, and active batch metadata
  in bbolt. Active batches are re-staged after restore so no PR lands from a
  pre-restart staging result that may have been invalidated by a base change.

## Observability

The same HTTP listener serves `/healthz`, `/metrics`, `/status`, and `/webhook`.
`/webhook` accepts Forgejo/Gitea events and wakes reconciliation for auto-merge,
pull-request, review, status, and push activity. A buffered wake channel coalesces
bursts so several webhook deliveries schedule one prompt reconcile; the polling
timer remains the correctness backstop.

When `SHUNT_WEBHOOK_URL` is set, shunt uses the same repository-admin token it
already needs for branch protection to create or update a matching repository
webhook for each managed repo. Hooks with different URLs are left alone; shunt
only manages a Forgejo/Gitea JSON webhook whose URL matches the configured
listener URL.

Metrics are Prometheus text format, intentionally dependency-free and
process-local: they are scoped per managed `(owner, repo, base)` queue and cover
queue depth, active batch presence, oldest queued PR age, batches started, PR
merges, bounces, staging conflicts, reconcile errors, terminal gate outcomes,
and time-in-queue histograms for PRs that merge, bounce, or drop out of the
in-memory queue. They reset on restart, so queue-age and time-in-queue
observations start when the current process first observes a PR; persisted
metrics history remains a roadmap item.

The JSON status endpoint complements those counters with safe queue membership:
owner, repo, base, depth, active/pending PR-number batches, and active-batch
presence. `/status.html` renders the same data for humans. Both omit staging
SHAs and runtime configuration. When `SHUNT_QUEUE_COMMENTS=true`, shunt also
keeps one sticky status comment on each queued PR and updates terminal queue
outcomes there.

## Running against a real instance (safely)

1. Create a disposable repo and a bot account with a token that has repository
   admin permission.
2. Protect the base branch: require the `merge-queue` status, restrict pushes to
   the bot, and turn **off** "block on outdated branch".
3. Optionally set `SHUNT_WEBHOOK_URL` so shunt configures the repository webhook
   itself.
4. Add `examples/mq-gate.yml` as `.forgejo/workflows/mq-gate.yml`.
5. Open a PR, enable "Merge when checks succeed", and run shunt with
   `SHUNT_REPO=owner/disposable-repo`. Watch it stage, gate, and land. Introduce a
   failing PR to watch a batch bisect.

Delete the disposable repo afterwards.
