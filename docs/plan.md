# Approval Gate — Delegate to Forgejo

## Problem

The `queueEligibility()` function checks whether a PR is eligible for the merge queue. It verifies:
1. Auto-merge is scheduled (via `AutomergeState` — reads timeline comments)
2. CI status is not failure/older than schedule (via `LatestCommitStatus`)

It does NOT check whether the PR has enough approvals. This matters because:
- A user can click "Merge when checks succeed" on a PR that hasn't met the repository's required approval threshold
- Forgejo will accept the `pull_scheduled_merge` event (setting `AutomergeState.Scheduled = true`)
- The PR enters the queue, gets staged, tested
- Forgejo blocks the actual merge at landing time because approvals are missing
- The batch wastes time testing against a PR that can't land

The Forgejo UI shows: `"This pull request doesn't have enough approvals yet. 0 of 1 approvals granted."` — this state IS discoverable via the API, through the `ScheduleAutomerge` endpoint which rejects with 422 when approvals are insufficient.

## Design Principle

**Delegate, don't reimplement.** Do not add:
- A `RequiredApprovals` config field
- Approval counting logic (latest-review-wins, etc.)
- A new `PRReview` API method
- A `BranchProtection` read endpoint

Instead: attempt to schedule the merge during the eligibility check. Forgejo returns:
- `201` → accepted, PR is eligible
- `409` → already scheduled, PR is eligible (this is the current happy path)
- `422` → rejected by Forgejo (missing approvals, conflicts, etc.), PR is NOT eligible
- Other error → propagate

This uses Forgejo's own approval knowledge as the admission gate.

## Architecture

### Current flow

```
queueEligibility()
  → AutomergeState()  ← reads timeline for pull_scheduled_merge events
  → LatestCommitStatus()  ← checks CI status
  → return (true/false)

land()
  → ScheduleAutomerge()  ← only called here, AFTER PR is already in queue
```

### Target flow

```
queueEligibility()
  → ScheduleAutomerge()  ← NEW: ask Forgejo if this PR can merge
    → 201/409 → eligible
    → 422 → NOT eligible (Forgejo says no — missing approvals, etc.)
    → error → propagate
  → AutomergeState()  ← existing: verify schedule was recorded
  → LatestCommitStatus()  ← existing: verify CI
  → return (true/false)

land()
  → ScheduleAutomerge()  ← still called; now gets result type
    → if not eligible → skipLand (approval was revoked during testing)
```

## Constraints

1. **`ScheduleAutomerge` return type change:** Currently returns `(error)`. Must change to return `(ScheduleAutomergeResult, error)` so callers can distinguish "accepted" from "rejected" from "error."

2. **409 handling unchanged:** A 409 means the PR is already scheduled (by the user clicking "Merge when checks succeed"). This is NOT a failure — the PR is eligible. The current code already handles this specially. Keep this behavior.

3. **422 handling:** Forgejo returns HTTP 422 when the PR doesn't meet merge requirements. This includes:
   - Insufficient approvals (the new use case)
   - Merge conflicts (already handled elsewhere)
   - Other branch protection violations
   Treat all 422s as "not eligible" — we don't need to distinguish the reason.

4. **Other errors propagate:** Network errors, auth failures, etc. should still propagate as errors.

5. **Webhook handling:** The existing `pull_request_review_rejected` webhook already wakes `Reconcile()`. On the next tick, `queueEligibility()` re-runs `ScheduleAutomerge()`. If Forgejo unscheduled the merge, scheduling fails and the PR is dropped. No new webhook handling needed.

6. **No config needed:** The approval threshold is a per-repo Forgejo setting. We don't need a shunt config field.

## Implementation

### Files changed

| File | Changes |
|------|---------|
| `internal/forge/client.go` | Add `ScheduleAutomergeResult` type; change `ScheduleAutomerge` to return `(ScheduleAutomergeResult, error)` |
| `internal/engine/engine.go` | Update `scheduleAutomerge()` helper; add scheduling check in `queueEligibility()`; update `land()` callers |

### Step 1: Add `ScheduleAutomergeResult` type and update `ScheduleAutomerge`

**File:** `internal/forge/client.go`
**Location:** Near other result types (~line 160), after `AutomergeState`:

```go
// ScheduleAutomergeResult captures the outcome of a ScheduleAutomerge attempt.
type ScheduleAutomergeResult struct {
    // Eligible is true when Forgejo accepted the schedule request.
    // false means Forgejo rejected it (e.g., missing approvals, conflicts).
    Eligible bool
}
```

**Location:** Replace the existing `ScheduleAutomerge` function (~line 487):

```go
// ScheduleAutomerge attempts to schedule a PR for merge.
// Returns a result indicating whether Forgejo accepted or rejected the request.
// A 409 (already scheduled) is treated as success — the PR is eligible.
// A 422 means the PR doesn't meet merge requirements (e.g., missing approvals).
func (c *Client) ScheduleAutomerge(ctx context.Context, owner, repo string, index int, style, headSHA string) (ScheduleAutomergeResult, error) {
    data, status, err := c.doRaw(ctx, http.MethodPost,
        fmt.Sprintf("/repos/%s/pulls/%d/merge", repoPath(owner, repo), index),
        map[string]any{
            "Do":                        style,
            "head_commit_id":            headSHA,
            "merge_when_checks_succeed": true,
        })
    if err != nil {
        if status == http.StatusConflict && strings.Contains(strings.ToLower(string(data)), "already scheduled") {
            // Already scheduled — the user clicked "merge when checks succeed".
            // This is a valid eligibility signal.
            return ScheduleAutomergeResult{Eligible: true}, nil
        }
        // Other errors (422, network, auth) — pass through.
        return ScheduleAutomergeResult{Eligible: false}, err
    }
    // 201 Created — schedule accepted.
    return ScheduleAutomergeResult{Eligible: true}, nil
}
```

**Changes from existing code:**
- Uses `doRaw` instead of `do` to get the HTTP status code
- Returns `ScheduleAutomergeResult{Eligible: true}` on 201 and 409
- Returns `ScheduleAutomergeResult{Eligible: false}` on other non-error status codes (unlikely but correct)
- Returns `{Eligible: false}, err` for unexpected errors (422, 500, etc.)

### Step 2: Update `engine.scheduleAutomerge()` helper

**File:** `internal/engine/engine.go`
**Location:** Line ~739

```go
func (e *Engine) scheduleAutomerge(ctx context.Context, pr forge.PullRequest) error {
    result, err := e.fc.ScheduleAutomerge(ctx, e.cfg.Owner, e.cfg.Repo, pr.Number, e.cfg.MergeStyle, pr.Head.Sha)
    if err != nil {
        return err
    }
    if !result.Eligible {
        return fmt.Errorf("forge rejected auto-merge for PR #%d", pr.Number)
    }
    return nil
}
```

### Step 3: Add scheduling check in `queueEligibility()`

**File:** `internal/engine/engine.go`
**Location:** Top of `queueEligibility()` (~line 319), BEFORE the existing `AutomergeState` check:

```go
func (e *Engine) queueEligibility(ctx context.Context, pr forge.PullRequest) (bool, error) {
    // Try to schedule — Forgejo will reject if the PR doesn't meet merge
    // requirements (e.g., missing approvals).  This is the admission gate;
    // we delegate to Forgejo rather than reimplementing approval counting.
    result, err := e.fc.ScheduleAutomerge(ctx, e.cfg.Owner, e.cfg.Repo, pr.Number, e.cfg.MergeStyle, pr.Head.Sha)
    if err != nil {
        return false, err
    }
    if !result.Eligible {
        return false, nil
    }

    // Existing checks: auto-merge state + CI status.
    state, err := e.fc.AutomergeState(ctx, e.cfg.Owner, e.cfg.Repo, pr.Number)
    if err != nil {
        return false, err
    }
    // ... rest of existing function unchanged ...
}
```

**Notes:**
- This is the admission gate. If Forgejo says "no" here, the PR never enters the queue.
- If Forgejo says "yes" here but something changes during testing (approval revoked), the `land()` check catches it.
- The existing `AutomergeState` check is kept as a secondary verification (timeline consistency).

### Step 4: Update `land()` callers to handle result

**File:** `internal/engine/engine.go`
**Location:** `land()` function — there are two places where `scheduleAutomerge` is called inside the retry recovery path (~lines 656 and 727):

**Location 1 (~line 656, after native merge timeout recovery):**
```go
// Before:
if err := e.scheduleAutomerge(ctx, current); err != nil {
    return false, merged, fmt.Errorf("restore auto-merge for PR #%d: %w", staged.Number, err)
}

// After:
if err := e.scheduleAutomerge(ctx, current); err != nil {
    return false, merged, fmt.Errorf("restore auto-merge for PR #%d: %w", staged.Number, err)
}
```
No change needed — `scheduleAutomerge` already wraps the result and returns an error if not eligible.

**Location 2 (~line 727, same recovery path, second call):**
Same — `scheduleAutomerge` is a wrapper that already handles the result.

**No other callers of `scheduleAutomerge` need changes** — the wrapper handles the result internally.

### Step 5: Verify `land()` handles Forgejo rejection during testing

If a PR enters the queue (passes admission gate), gets staged, passes CI, but then loses an approval during testing — when `land()` calls `scheduleAutomerge`, Forgejo will reject. The wrapper returns an error, which propagates up from `land()`. The `checkActive()` caller handles this:

```go
// From checkActive():
resolved, merged, err := e.land(ctx, a)
if err != nil {
    return merged > 0, err
}
```

The error propagates to `Reconcile()` which logs it and increments `ReconcileError`. The batch state is stale and will be requeued on the next tick. This is correct behavior — the PR should not land.

### Step 6: Update tests in engine_test.go

**File:** `internal/engine/engine_test.go`

The mock's `ScheduleAutomerge` method needs to return `(forge.ScheduleAutomergeResult, error)` instead of just `error`. Update the mock:

```go
func (m *mock) ScheduleAutomerge(_ context.Context, _, _ string, index int, style, headSHA string) (forge.ScheduleAutomergeResult, error) {
    m.calls = append(m.calls, "schedule:"+strconv.Itoa(index))
    if m.scheduleRejected {
        return forge.ScheduleAutomergeResult{Eligible: false}, nil
    }
    return forge.ScheduleAutomergeResult{Eligible: true}, nil
}
```

Add test:
```go
func TestQueueEligibilityRejectedByForgejo(t *testing.T) {
    // Schedule a PR that Forgejo rejects → not eligible.
}
```

Add test:
```go
func TestQueueEligibilityAcceptedByForgejo(t *testing.T) {
    // Schedule a PR that Forgejo accepts → check existing flow.
}
```

## Acceptance Criteria

1. **Missing approvals blocks admission:** When Forgejo rejects `ScheduleAutomerge` with 422 (e.g., missing approvals), `queueEligibility()` returns `false` and the PR is not added to the queue.

2. **Successful scheduling passes admission:** When Forgejo accepts `ScheduleAutomerge` (201) or finds it already scheduled (409), `queueEligibility()` proceeds to the existing CI/status checks.

3. **`land()` handles post-admission rejection:** If Forgejo accepted scheduling during admission but rejects it later during landing (e.g., approval revoked), `scheduleAutomerge` returns an error and the batch is not landed.

4. **Existing 409 behavior preserved:** PRs that are already scheduled (409) are treated as eligible — this is the current happy path and must not change.

5. **Network errors propagate:** Non-422/non-409 errors (network, auth, 500) still propagate as errors from `queueEligibility()`.

6. **Webhook-based invalidation works:** When a `pull_request_review_rejected` webhook fires, the next `Reconcile()` tick re-runs `queueEligibility()`. If Forgejo has unscheduled the merge, scheduling fails and the PR is dropped.

7. **All existing tests pass.**

## Verification

```bash
cd /tmp/pi-worktrees/approval-gate
make lint test build
```
