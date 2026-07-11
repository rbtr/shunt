# shunt

<p align="center">
  <img src="docs/assets/shunt-punter.png" alt="A Go gopher punting a queue of boxes down a canal" width="420">
</p>

**A merge queue for [Forgejo](https://forgejo.org/) and Gitea: batch the stack,
test once, and bisect only when the batch fails.**

[![CI](https://github.com/rbtr/shunt/actions/workflows/ci.yml/badge.svg)](https://github.com/rbtr/shunt/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbtr/shunt)](https://goreportcard.com/report/github.com/rbtr/shunt)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

shunt keeps a protected branch green without testing every pull request one at a
time. It rolls ready PRs into a batch, tests the **whole batch in one CI run**,
and — only if that run fails — uses binary-search **bisection** to land the good
PRs and bounce the bad one. You get a self-hosted merge queue with a cheap common
path and a precise failure path.

There are **no bot commands**: a PR joins the queue when you enable Forgejo's
built-in *"Merge when checks succeed"*.

---

## Why

Per-PR required checks don't keep `main` green — two PRs that each pass alone can
break together once both land. The usual fix, a merge queue, tests each PR
against the state it will actually merge into. GitHub's does this by
speculatively testing stacks **in parallel**, which burns CI minutes. shunt
instead tests the **entire batch at once** and only pays for bisection when
something is actually broken:

- **Green path:** N PRs, **1** CI run.
- **Red path:** ~`log₂(N)` extra runs to isolate the culprit(s).

For a repo that's green most of the time, that's dramatically less CI than
per-PR or speculative queues — and it lets you move an expensive suite (big
end-to-end / visual tests) from *every PR* to *once per batch*.

## How it works

Per `(repo, base-branch)`, whenever a relevant webhook arrives (with the poll
interval as a backstop):

1. **Find the queue** — open PRs with auto-merge enabled (detected from the PR
   timeline; Forgejo doesn't expose this on the PR object).
2. **Stage** — create a fresh `mq/<base>/staging-*` branch from the base tip and
   merge each PR's head into it. A staging conflict splits the candidate at the
   conflict point so earlier PRs keep their place.
3. **Gate** — pushing the staging branch triggers your `on: push: [mq/**]`
   workflow. shunt reads that run's status.
4. **Resolve:**
   - **pass** → set the required `merge-queue` status on each PR and merge it via
     the API (Forgejo records a proper merge).
   - **fail, 1 PR** → bounce it (cancel auto-merge + comment).
   - **fail, >1 PR** → split in half and test sub-batches, up to the configured
     bisection fan-out. Innocent PRs are **never** bounced.

Branch protection requires the `merge-queue` status on the base branch and
restricts pushes to both the base and shunt-owned staging branches to the bot, so
nothing reaches the base branch except a batch shunt has tested. Full detail and
the validated Forgejo mechanics are in
[`docs/design.md`](docs/design.md).

## Quickstart

shunt needs a dedicated **bot account**, access only to the repositories it
manages, a token with the repository permissions described in
[Security posture](#security-posture), and a slot in each protected branch's
push allow-list. Current Forgejo/Gitea branch-protection APIs require repository
admin access even for read requests, so the bot must have that permission on
managed repositories to keep the required `merge-queue` gate in place.

```sh
go build -o shunt ./cmd/shunt

SHUNT_INSTANCE=https://forge.example.com \
SHUNT_TOKEN=<bot-token> \
SHUNT_TOPIC=merge-queue \
./shunt
```

That runs in **multi-repo mode**: every repo tagged with the `merge-queue` topic
is discovered and managed automatically. For a single repo, set
`SHUNT_REPO=owner/repo` instead of `SHUNT_TOPIC`.

## Opting a repo in

1. Add the **`merge-queue` topic** to the repo. shunt configures its branch
   protection on first sight (require the `merge-queue` status, restrict pushes
   to the bot).
2. Add a gate workflow scoped to the staging branches —
   [`examples/mq-gate.yml`](examples/mq-gate.yml) (`on: push: [mq/**]`). This is
   where your full suite runs, once per batch.
3. Enable **"Merge when checks succeed"** on an approved PR.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SHUNT_INSTANCE` | — (required) | Forge base URL, e.g. `https://forge.example.com` |
| `SHUNT_TOKEN` | — (required) | Bot API token; see [Security posture](#security-posture) |
| `SHUNT_TOPIC` | — | Manage every repo with this topic (multi-repo mode) |
| `SHUNT_REPO` | — | `owner/repo` for single-repo mode (use this *or* `SHUNT_TOPIC`) |
| `SHUNT_BASE` | `main` | Protected base branch (single-repo mode) |
| `SHUNT_BOT_USER` | `mq-bot` | Bot username (for git auth + push allow-list) |
| `SHUNT_BOT_EMAIL` | `<bot>@noreply.invalid` | Author email for staging commits |
| `SHUNT_STATUS_CONTEXT` | `merge-queue` | Required commit-status context |
| `SHUNT_MERGE_STYLE` | `merge` | Merge method passed to the forge: `merge`, `squash`, or `rebase`. Must be enabled in the repository settings; for squash-only repos, set this to `squash`. |
| `SHUNT_MAX_BATCH` | `0` | Cap the initial rollup size (0 = unlimited) |
| `SHUNT_BATCH_LINGER` | `0` | Optional duration to wait before forming the first batch, allowing more ready PRs to accumulate (0 = disabled) |
| `SHUNT_BATCH_TARGET` | `0` | Start a lingering batch early once this many ready PRs are present (0 = wait the full linger window). |
| `SHUNT_BISECT_FANOUT` | `1` | Maximum concurrent bisection staging runs per queue. `1` preserves serial bisection. |
| `SHUNT_QUEUE_COMMENTS` | `false` | When true, maintain one sticky queue-status comment on each queued PR. Disabled by default to avoid extra write traffic. |
| `SHUNT_STATE_PATH` | — | Optional path to a local bbolt database for durable queue checkpoints. Leave empty for in-memory state. |
| `SHUNT_POSTGRES_DSN` | — | Optional Postgres DSN for durable queue checkpoints. Mutually exclusive with `SHUNT_STATE_PATH`; migrations are applied at startup. |
| `SHUNT_POLL_INTERVAL` | `10s` | Reconcile cadence |
| `SHUNT_PUBLIC_URL` | = `SHUNT_INSTANCE` | Base URL for the links written into PR comments (set when the bot reaches the forge over an internal URL) |
| `SHUNT_LISTEN` | `:8080` | Address for the `/healthz`, `/metrics`, `/status`, and `/webhook` endpoints |
| `SHUNT_WEBHOOK_URL` | — | Public URL Forgejo/Gitea should call, usually `https://shunt.example.com/webhook`. When set, shunt creates/updates a repository webhook for each managed repo. |
| `SHUNT_WEBHOOK_SECRET` | — | Optional shared secret for Forgejo/Gitea HMAC-SHA256 webhook signature validation |

If `SHUNT_WEBHOOK_URL` is set, shunt uses the same admin token it already needs
for branch protection to create or update a repository webhook for each managed
repo. It leaves unrelated hooks alone and only manages a hook whose URL matches
`SHUNT_WEBHOOK_URL` and whose type is Forgejo/Gitea's JSON webhook type.
Without that setting, configure a repository or
organization webhook yourself. shunt wakes immediately for auto-merge,
pull-request, review, status, and push events, but still polls on
`SHUNT_POLL_INTERVAL` so missed webhooks only add latency.

shunt also creates bot-only protection for its staging ref pattern
`mq/<base>/staging*`. Staging refs are immutable per attempt and retained for
audit/debug links; do not point unrelated workflows or branches at that prefix.

Repos can override safe queue tunables with `.shunt.yml` in the repo root. In
multi-repo mode, shunt reads it from the discovered default branch before
choosing the managed base; in single-repo mode, it reads from `SHUNT_BASE`.
Missing files keep the global defaults. Invalid files are logged/rejected without
applying partial settings.

Set `SHUNT_STATE_PATH` to persist queue checkpoints in a local bbolt database, or
set `SHUNT_POSTGRES_DSN` to use Postgres. Leave both empty for in-memory state;
do not set both. Durable stores preserve pending candidates, active batch
metadata, linger state, and bisection counters across restarts. Active batches
are re-staged after restore rather than landed from pre-restart CI results. In
Kubernetes, mount a bbolt path on a persistent volume; an `emptyDir` path only
survives container restarts within the same pod lifetime. Keep Postgres DSNs in a
runtime secret store.

```yaml
base: trunk
status_context: shunt
merge_style: squash # merge, squash, or rebase
max_batch: 4
batch_linger: 30s
batch_target: 3
bisect_fanout: 2
```

## Observability

`GET /metrics` on `SHUNT_LISTEN` exposes dependency-free Prometheus text metrics
for each managed `(owner, repo, base)` queue. `POST /webhook` accepts
Forgejo/Gitea events and wakes reconciliation promptly.

- `shunt_queue_depth` — PRs currently known in the in-memory queue, including
  active batches and queued bisection candidates.
- `shunt_active_batch` — `1` while a queue has a staging batch under gate test.
- `shunt_queue_oldest_age_seconds` — process-local age of the oldest PR currently
  known in the in-memory queue.
- `shunt_batches_started_total`, `shunt_pr_merges_total`,
  `shunt_bounces_total`, `shunt_staging_conflicts_total`, and
  `shunt_reconcile_errors_total`.
- `shunt_gate_outcomes_total{outcome="success|failure|cancelled|error"}` for
  terminal gate results.
- `shunt_time_in_queue_seconds_bucket/_sum/_count{outcome="merged|bounced|dropped"}`
  histogram for process-local time from first queue observation until the PR
  leaves the queue.

Logs are structured JSON on stdout, with stable fields such as `component`,
`owner`, `repo`, and `base` where they apply. Metrics are process-local and
intentionally minimal in v0.4: queue-age and time-in-queue observations restart
from when the current process first sees a PR, and metrics do not include
persisted history.

`GET /status` exposes the process-local queue membership as JSON for lightweight
ops surfaces that need more detail than counters. `GET /status.html` serves the
same data as a small human-readable page. Both surfaces contain only queue
identity (`owner`, `repo`, `base`), depth, active/pending PR-number batches, and
whether any batch is active; they omit tokens, clone URLs, staging SHAs, and
other internal details.

Set `SHUNT_QUEUE_COMMENTS=true` to add a small PR-visible status surface. shunt
keeps one sticky comment per queued PR, identified by a stable hidden marker, and
edits it only when the displayed queue state changes. The comment shows the repo,
base branch, queue position, current state, and active batch when known. This is
off by default because it adds issue-comment API reads/writes on repositories that
opt in.

Terminal outcomes are always reported on the source PR, even when sticky queue
comments are disabled. Landed PRs receive a durable landed comment. Rejected PRs
receive a failed or errored source-head status, auto-merge is cancelled, and shunt
posts a durable comment with the rejection reason and a staging run/commit link
when one exists. PRs skipped before landing because their head changed, auto-merge
was cancelled, or the forge merge API did not complete receive an error status
and a requeue/skipped comment so the PR page explains what happened.

## Deploy

Download a binary from the GitHub release, run the GHCR image, or deploy with
Helm/Kustomize. The container intentionally includes native `git` rather than a
pure-Go git library so staging merges match real Git behavior.

```sh
cp examples/.env.example .env
$EDITOR .env
docker run --rm --env-file .env ghcr.io/rbtr/shunt:0.4.1

helm install shunt oci://ghcr.io/rbtr/charts/shunt \
  --version 0.4.1 \
  --set config.instance=https://forge.example.com \
  --set token.existingSecret=shunt-bot

kubectl apply -k deploy/kustomize/base
```

- Kubernetes manifest: [`examples/kubernetes.yaml`](examples/kubernetes.yaml)
- Docker Compose: [`examples/docker-compose.yml`](examples/docker-compose.yml)

## Compatibility

- **Forgejo** — validated end to end on v15.x (Gitea 1.22-compatible API).
- **Gitea** — shunt uses only the Gitea-compatible API surface (timeline
  comment types, branch protection, commit statuses, the merge endpoint), so it
  is **expected** to work on Gitea ≥ 1.22. Not yet validated there — reports
  welcome.

Runs entirely outside the forge via its REST API — Forgejo and Gitea have no
in-process plugin system, so a sidecar bot like this is the idiomatic way to add
a merge queue.

## Security posture

Run shunt as a dedicated bot identity and give it access only to repositories it
is allowed to manage. Prefer per-repository bot tokens for single-repo
deployments, or tightly scoped bot/org tokens for the opted-in repositories when
your Forgejo/Gitea version supports that model.

Current Forgejo/Gitea APIs still require repository admin permission on every
managed repository because shunt reads and reconciles branch-protection rules,
and because managed webhooks are repository settings. The token should still be
scoped to the minimum managed repositories and operations. Token models vary, so
map the token to these permission categories rather than assuming a specific
scope name:

- read repository, pull request, timeline, commit-status, and workflow-run state;
- discover opted-in repositories by topic in multi-repo mode;
- manage branch protection for opted-in repositories;
- create or update the shunt-managed repository webhook when
  `SHUNT_WEBHOOK_URL` is set;
- create commit statuses, merge pull requests, write PR comments, and cancel
  scheduled auto-merge when a PR lands, is bounced, or becomes stale;
- use Git to fetch PR heads and push only `mq/...` staging branches. Base branch
  changes go through the forge merge API after the queue status passes.

Keep tokens in your runtime secret store, never in the repository. The examples
use placeholders only; real tokens should be supplied through environment
variables, Docker secrets, Kubernetes Secrets, or an equivalent secret manager.

If `/webhook` is exposed beyond a trusted private network, set
`SHUNT_WEBHOOK_SECRET`; managed repository hooks use the same secret. Set
`SHUNT_PUBLIC_URL` when shunt reaches the forge over an internal URL so PR
comments link to a public-safe forge URL instead of private infrastructure names.

## Status & roadmap

v0.4 is functional but young. Known limitations (opt-in durable state, polling
as a webhook backstop, single replica) and the plan to address them are tracked in
[`ROADMAP.md`](ROADMAP.md).

## License

[MIT](LICENSE).
