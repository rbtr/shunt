# Agent Instructions

This repository is intended to be public. Treat every diff, prompt excerpt,
fixture, example, log line, and comment as publishable.

## Non-negotiables

- **Use a dedicated worktree.** Never edit, build, test, commit, or push from the
  shared repository root. Start from fresh `origin/main` unless the user gives a
  different base.
- **Protect private information.** Do not commit or quote secrets, tokens,
  private forge URLs, local filesystem paths, account IDs, private identities, or
  environment-specific operational details. Keep examples generic.
- **Keep the project dependency-light.** This is a small Go service with no
  external Go dependencies. Do not add one unless it clearly improves correctness
  or security.
- **Validate behavior before declaring done.** For code changes run
  `make lint test build`. Documentation-only changes do not need validation
  unless a docs-specific check exists.
- **Use independent review for code changes.** Before commit/push, run a
  rubber-duck or code-review pass over the diff. Security-sensitive changes also
  need a security-focused review.
- **Clean up worktrees.** Remove task worktrees and local branches after merge or
  abandonment.

## Worktree workflow

```bash
SLUG=<short-task-slug>
BRANCH=<branch-name>
WT="<path-outside-the-shared-repo>/$SLUG"

git fetch origin main --quiet
git worktree add -b "$BRANCH" "$WT" origin/main
cd "$WT"
```

If the user explicitly asks to amend the existing single commit for a
pre-publication cleanup, it is acceptable to amend and force-push that commit.
Otherwise use a normal branch and pull request.

## Project map

- `cmd/shunt` — entrypoint, environment configuration, health endpoint
- `internal/forge` — Forgejo/Gitea REST API client
- `internal/gitops` — staging-branch git plumbing
- `internal/engine` — reconcile loop, rollup batching, bisection
- `internal/manager` — topic discovery and one engine per managed repo
- `docs/design.md` — algorithm and Forgejo/Gitea behavior notes
- `examples/` — deploy and gate-workflow examples with placeholders only

## Validation

```bash
make lint test build
```

Use narrower commands while iterating (`go test ./internal/engine`, `go test
./...`, `go vet ./...`), but finish with the full command above for code changes.

## Public-copy and security checklist

Before committing, inspect the diff for:

- private hostnames, usernames, absolute paths, tokens, IDs, logs, or screenshots
- generated or placeholder text that reads like filler
- examples that could encourage committing secrets
- claims that exceed the tested behavior
- broad error swallowing around auth, git pushes, merges, or branch protection

Prefer precise, boring copy over hype. If behavior is unverified, say so plainly
in the docs or leave it on the roadmap.
