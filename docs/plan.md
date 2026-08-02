# MQ Staging Branch Cleanup

## Problem

Since v0.5.0, staging branches (`mq/<base>/staging-<timestamp>-<seq>`) are created on the remote when a batch is staged but never deleted after the batch completes. The v0.5.0 commit deliberately removed startup pruning and branch deletion, intending branches to be retained as "audit/debug links." In practice, this causes unbounded accumulation of branches on the remote.

## Scope

### In scope
- Delete staging branch after all PRs in a batch have landed (successful merge)
- Delete staging branch when a batch fails/bounces/bisects

### Out of scope
- Cleanup of branches created by `BuildStaging()` that fail before an `activeBatch` is formed (e.g., merge conflicts, git errors) — fixing this would require `BuildStaging` to return branch names on error
- Branch protection changes
- Cleanup of checkpoint state (already handled separately)

## Architecture

### Current lifecycle

```
startNext() → BuildStaging() → push mq/main/staging-<ts>-<n>
                                           ↓
checkActive() → waits for CI gate → sets a.outcome
                                           ↓
    ┌──── success ────┐    failure/cancel/error
    ↓                  ↓
  land()         bisectOrBounce()
    ↓                  ↓
removeActive()     removeActive()  ← in-memory only
    ↓
Reconcile() → saveCheckpoint()
    (branch still on remote forever)
```

### Target lifecycle

Same flow, with `DeleteBranch()` calls added at the end of:
1. `land()` — after `removeActive(a)`, when all PRs have landed
2. `bisectOrBounce()` — after `removeActive(a)`, when the batch has failed

## Constraints

1. **Delete timing in `land()`:** `Reconcile()` calls `checkActive()`, which calls `land()`, then `checkActive()` returns, then `Reconcile()` calls `saveCheckpoint()`. The branch can be deleted inside `land()` before `removeActive()` because the checkpoint is saved synchronously after `checkActive()` returns — no crash window exists.

2. **Error handling:** Best-effort. A failure to delete the branch must never affect the merge outcome. Log a warning; continue.

3. **404 = success:** The branch may already be deleted (e.g., manual deletion, or the same batch is retried after a crash). Treat `ErrNotFound` as success.

4. **`BisectFanout > 1`:** Multiple active batches can exist simultaneously. Each `activeBatch` has its own `a.stagingBranch`. Delete `a.stagingBranch` directly — no shared field needed.

5. **`ForgeAPI` interface:** Adding a method requires updating the mock in `engine_test.go`.

6. **Branch protection:** The `mq/<base>/staging*` protection rule whitelists push access for the bot. `DELETE /repos/{owner}/{repo}/branches/{branch}` is a different API operation authenticated with the same bot token — push protection does not block it.

## Implementation

### Files changed

| File | Changes |
|------|---------|
| `internal/forge/client.go` | Add `DeleteBranch()` method |
| `internal/engine/engine.go` | Add `DeleteBranch` to `ForgeAPI` interface (1 line); call it in `land()` and `bisectOrBounce()` |
| `internal/engine/engine_test.go` | Add `DeleteBranch` to mock struct and its implementation |

### Step 1: Add `DeleteBranch()` to forge client

**File:** `internal/forge/client.go`
**Location:** After `CancelAutomerge()` method (~line 507)

```go
// DeleteBranch removes the named branch from the repository.
// A 404 response is treated as success (branch already deleted).
func (c *Client) DeleteBranch(ctx context.Context, owner, repo, branch string) error {
    _, err := c.doRaw(ctx, http.MethodDelete,
        fmt.Sprintf("/repos/%s/branches/%s", repoPath(owner, repo), url.PathEscape(branch)), nil)
    if errors.Is(err, ErrNotFound) {
        return nil
    }
    return err
}
```

**Notes:**
- Uses `doRaw` to get the HTTP status code (needed for 404 handling)
- Uses `url.PathEscape` for safety (branch names like `main` are safe, but be correct)
- Returns `nil` on 404 (branch already deleted — retry-safe)
- Matches the pattern of `CancelAutomerge()` which also uses `doRaw`

**Imports to verify:** `errors`, `fmt`, `net/url` — check if all are already imported. If `errors` is missing, add it.

### Step 2: Add `DeleteBranch` to `ForgeAPI` interface

**File:** `internal/engine/engine.go`
**Location:** Inside the `ForgeAPI` interface (~line 65-77), after `CancelAutomerge`:

```go
DeleteBranch(ctx context.Context, owner, repo, branch string) error
```

### Step 3: Delete branch in `land()` — successful merge path

**File:** `internal/engine/engine.go`
**Location:** End of `land()` function (~line 735), after `e.removeActive(a)`, before `return true, merged, nil`:

```go
e.removeActive(a)

// ponytail: delete the staging branch now that all PRs have landed.
// Best-effort: 404 = already deleted, other errors = warning.
if err := e.fc.DeleteBranch(ctx, e.cfg.Owner, e.cfg.Repo, a.stagingBranch); err != nil {
    e.logger.Warn("failed to delete staging branch",
        "branch", a.stagingBranch, "error", err)
}

return true, merged, nil
```

**Crash recovery scenario:** If the process crashes between `removeActive(a)` and `return`, the next `Reconcile()` loads the stale checkpoint. `checkActive()` calls `land()` again, the loop over PRs finds them already merged, iterates through with `continue`, then reaches the same cleanup code. `DeleteBranch` returns 404 → success. Then `removeActive(a)` again. Then `saveCheckpoint()` saves the empty snapshot. Self-healing.

### Step 4: Delete branch in `bisectOrBounce()` — failed batch path

**File:** `internal/engine/engine.go`
**Location:** Inside `bisectOrBounce()`, after `e.removeActive(a)`, before the `if len(nums) == 1` check:

```go
func (e *Engine) bisectOrBounce(ctx context.Context, a *activeBatch, status string) (bool, error) {
    nums := numbersOf(a.prs)
    e.removeActive(a)

    // ponytail: failed batch — clean up its staging branch.
    // Best-effort: 404 = already deleted, other errors = warning.
    if err := e.fc.DeleteBranch(ctx, e.cfg.Owner, e.cfg.Repo, a.stagingBranch); err != nil {
        e.logger.Warn("failed to delete staging branch after gate failure",
            "branch", a.stagingBranch, "error", err)
    }

    if len(nums) == 1 {
        bounced := e.bounce(ctx, nums[0], a.prs[0].Head.Sha,
            fmt.Sprintf("merge-queue gate **%s**", status),
            gateOutcomeStatus(status), a.debugURL)
        return bounced, nil
    }
    mid := len(nums) / 2
    first := append([]int(nil), nums[:mid]...)
    second := append([]int(nil), nums[mid:]...)
    e.markBisectOrigins(first[0], second[0])
    e.enqueueWithState("retrying after gate "+status+"; isolating the batch", first, second)
    e.logger.Info("batch failed; bisecting", "prs", nums, "status", status,
        "first", first, "second", second)
    return true, nil
}
```

**What happens here:**
- Size-1 batch: branch deleted, PR bounced, old branch gone
- Larger batch: branch deleted, batch split into two sub-batches, each sub-batch creates its own new staging branch later via `startNext()`

### Step 5: Update mock in engine_test.go

**File:** `internal/engine/engine_test.go`
**Location:** Inside the mock struct and its methods

Add to the mock struct:
```go
func (m *mock) DeleteBranch(_ context.Context, _, _, branch string) error {
    m.calls = append(m.calls, "delete:"+branch)
    return nil
}
```

## Acceptance Criteria

1. **Successful merge deletes branch:** After a batch of N PRs fully lands (all N released and merged), the staging branch is deleted from the remote. Verified by the mock recording `delete:<branch-name>`.

2. **Bounced batch deletes branch:** After a size-1 batch fails and the PR is bounced, its staging branch is deleted.

3. **Bisected batch deletes branch:** After a multi-PR batch fails and is bisected, the original staging branch is deleted. The new sub-batch branches (created later by `startNext()`) are deleted when those sub-batches complete (either successfully or on further failure).

4. **404 is non-fatal:** If the branch is already deleted, `DeleteBranch` returns nil and no warning is logged.

5. **Delete failure doesn't block merge:** If `DeleteBranch` returns a non-404 error, a warning is logged but the merge continues (for successful path) or the bounce/bisect continues (for failure path).

6. **Existing tests pass:** All existing tests pass after the changes.

7. **Mock implements interface:** The mock struct compiles and implements `ForgeAPI`.

## Verification

```bash
cd /tmp/pi-worktrees/mq-cleanup
make lint test build
```
