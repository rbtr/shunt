# shunt

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

Per `(repo, base-branch)`, on a fixed interval:

1. **Find the queue** — open PRs with auto-merge enabled (detected from the PR
   timeline; Forgejo doesn't expose this on the PR object).
2. **Stage** — create `mq/<base>/staging` from the base tip and merge each PR's
   head into it. A staging conflict splits the candidate at the conflict point so
   earlier PRs keep their place.
3. **Gate** — pushing the staging branch triggers your `on: push: [mq/**]`
   workflow. shunt reads that run's status.
4. **Resolve:**
   - **pass** → set the required `merge-queue` status on each PR and merge it via
     the API (Forgejo records a proper merge).
   - **fail, 1 PR** → bounce it (cancel auto-merge + comment).
   - **fail, >1 PR** → split in half and test sub-batches, up to the configured
     bisection fan-out. Innocent PRs are **never** bounced.

Branch protection requires the `merge-queue` status and restricts pushes to the
bot, so nothing reaches the base branch except a batch shunt has tested. Full
detail and the validated Forgejo mechanics are in
[`docs/design.md`](docs/design.md).

## Quickstart

shunt needs a **bot account** with a token, repository admin access to the
managed repos, and a slot in each protected branch's push allow-list. Repository
admin is required because Forgejo/Gitea branch-protection APIs reject even read
requests from normal write collaborators; shunt must read and reconcile those
rules to keep the required `merge-queue` gate in place.

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
| `SHUNT_TOKEN` | — (required) | Bot API token (repo scope) |
| `SHUNT_TOPIC` | — | Manage every repo with this topic (multi-repo mode) |
| `SHUNT_REPO` | — | `owner/repo` for single-repo mode (use this *or* `SHUNT_TOPIC`) |
| `SHUNT_BASE` | `main` | Protected base branch (single-repo mode) |
| `SHUNT_BOT_USER` | `mq-bot` | Bot username (for git auth + push allow-list) |
| `SHUNT_BOT_EMAIL` | `<bot>@noreply.invalid` | Author email for staging commits |
| `SHUNT_STATUS_CONTEXT` | `merge-queue` | Required commit-status context |
| `SHUNT_MERGE_STYLE` | `merge` | Merge method passed to the forge: `merge`, `squash`, or `rebase`. Must be enabled in the repository settings; for squash-only repos, set this to `squash`. |
| `SHUNT_MAX_BATCH` | `0` | Cap the initial rollup size (0 = unlimited) |
| `SHUNT_BATCH_LINGER` | `0` | Optional duration to wait before forming the first batch, allowing more ready PRs to accumulate (0 = disabled) |
| `SHUNT_BATCH_TARGET` | `0` | Start a lingering batch early once this many ready PRs are present (0 = wait the full linger window). Process-wide until per-repo config lands. |
| `SHUNT_BISECT_FANOUT` | `1` | Maximum concurrent bisection staging runs per queue. `1` preserves serial bisection. |
| `SHUNT_POLL_INTERVAL` | `10s` | Reconcile cadence |
| `SHUNT_PUBLIC_URL` | = `SHUNT_INSTANCE` | Base URL for the links written into PR comments (set when the bot reaches the forge over an internal URL) |
| `SHUNT_LISTEN` | `:8080` | Address for the `/healthz` and `/metrics` endpoints |

## Observability

`GET /metrics` on `SHUNT_LISTEN` exposes dependency-free Prometheus text metrics
for each managed `(owner, repo, base)` queue:

- `shunt_queue_depth` — PRs currently known in the in-memory queue, including an
  active batch and queued bisection candidates.
- `shunt_active_batch` — `1` while a queue has a staging batch under gate test.
- `shunt_batches_started_total`, `shunt_pr_merges_total`,
  `shunt_bounces_total`, `shunt_staging_conflicts_total`, and
  `shunt_reconcile_errors_total`.
- `shunt_gate_outcomes_total{outcome="success|failure|cancelled|error"}` for
  terminal gate results.

Metrics are process-local and intentionally minimal in v0.1: they do not persist
across restarts and do not yet include time-in-queue histograms or a queue UI.

## Deploy

Download a binary from the GitHub release, run the GHCR image, or deploy with
Helm/Kustomize. The container intentionally includes native `git` rather than a
pure-Go git library so staging merges match real Git behavior.

```sh
cp examples/.env.example .env
$EDITOR .env
docker run --rm --env-file .env ghcr.io/rbtr/shunt:0.1.0

helm install shunt oci://ghcr.io/rbtr/charts/shunt \
  --version 0.1.0 \
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

shunt needs a bot token with repository admin access to the repositories it
manages so it can reconcile branch protection, set commit statuses, push staging
branches, and merge PRs. Keep that token in your runtime secret store, not in the
repository. The examples use placeholders only; real tokens should be supplied
through environment variables, Docker secrets, or Kubernetes Secrets.

## Status & roadmap

v0.2 is functional but young. Known limitations (in-memory state, polling instead
of webhooks, single replica) and the plan to address them are tracked in
[`ROADMAP.md`](ROADMAP.md).

## License

[MIT](LICENSE).
