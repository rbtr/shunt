# Forge integration tests

The forge client has an env-gated live integration harness. It is skipped by
default, so `go test ./...` and CI never require forge credentials or a running
Forgejo/Gitea instance.

Run it explicitly against a test instance and disposable repository:

```sh
SHUNT_FORGE_INTEGRATION=1 \
SHUNT_FORGE_INSTANCE=https://forge.example.com \
SHUNT_FORGE_TOKEN=<bot-token> \
SHUNT_FORGE_OWNER=<owner> \
SHUNT_FORGE_REPO=<repo> \
go test ./internal/forge -run TestForgeIntegrationHarness -count=1 -v
```

Required variables:

| Variable | Description |
|----------|-------------|
| `SHUNT_FORGE_INTEGRATION` | Set to `1` to enable the live harness. |
| `SHUNT_FORGE_INSTANCE` | Forgejo/Gitea base URL. |
| `SHUNT_FORGE_TOKEN` | Bot API token with access to the test repo. |
| `SHUNT_FORGE_OWNER` | Repository owner or organization. |
| `SHUNT_FORGE_REPO` | Repository name. |

Optional flows:

| Variable | Description |
|----------|-------------|
| `SHUNT_FORGE_BASE` | Base-branch filter for open PRs; defaults to `main`. |
| `SHUNT_FORGE_PR_INDEX` | Fetch this PR and scan its issue timeline for scheduled auto-merge events. |
| `SHUNT_FORGE_STATUS_SHA` | Post a commit status to this SHA. Defaults to context `shunt-integration`, state `pending`. |
| `SHUNT_FORGE_STATUS_CONTEXT` | Override the status context used with `SHUNT_FORGE_STATUS_SHA`. |
| `SHUNT_FORGE_STATUS_STATE` | Override the posted status state. |
| `SHUNT_FORGE_STATUS_DESCRIPTION` | Override the posted status description. |
| `SHUNT_FORGE_STATUS_TARGET_URL` | Optional target URL for the posted status. |
| `SHUNT_FORGE_RUN_SHA` | Look up the latest Actions task status for this SHA. |
| `SHUNT_FORGE_RUN_BRANCH` | Optional branch filter for run-status lookup. |
| `SHUNT_FORGE_ALLOW_BRANCH_PROTECTION_WRITE` | Set to `1` to allow the branch-protection subtest to mutate repo settings. |
| `SHUNT_FORGE_BRANCH_PROTECTION_BRANCH` | Branch to pass to `EnsureBranchProtection` when writes are enabled. |
| `SHUNT_FORGE_BRANCH_PROTECTION_STATUS_CONTEXT` | Required status context for branch protection; defaults to `merge-queue`. |
| `SHUNT_FORGE_BRANCH_PROTECTION_BOT_USER` | Bot username to add to the push allow-list when writes are enabled. |

Use a disposable repo for write-enabled subtests. The harness intentionally does
not call the merge endpoint because that flow is destructive; merge request
payloads are covered by unit tests.
