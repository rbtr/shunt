package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/rbtr/shunt/internal/checkpoint"
	"github.com/rbtr/shunt/internal/forge"
)

// CheckpointStore persists queue snapshots for one engine-managed queue.
// Alias of the public mq/checkpoint.Store so hosts can implement it.
type CheckpointStore = checkpoint.Store

func (e *Engine) loadCheckpoint(ctx context.Context) error {
	if e.checkpointLoaded {
		return nil
	}
	if e.cfg.Checkpoint == nil {
		e.checkpointLoaded = true
		return nil
	}
	snapshot, ok, err := e.cfg.Checkpoint.LoadQueue(ctx, e.queueKey())
	if err != nil {
		return fmt.Errorf("load queue checkpoint: %w", err)
	}
	if !ok {
		e.checkpointLoaded = true
		return nil
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	if snapshot.Key != e.queueKey() {
		return fmt.Errorf("queue checkpoint key mismatch: got %s/%s@%s", snapshot.Key.Owner, snapshot.Key.Repo, snapshot.Key.Base)
	}
	if err := e.applySnapshot(ctx, snapshot); err != nil {
		return err
	}
	e.checkpointLoaded = true
	e.checkpointExists = true
	return nil
}

func (e *Engine) saveCheckpoint(ctx context.Context) error {
	if e.cfg.Checkpoint == nil || !e.checkpointLoaded {
		return nil
	}
	if e.emptyCheckpoint() {
		if !e.checkpointExists {
			return nil
		}
		if err := e.cfg.Checkpoint.DeleteQueue(ctx, e.queueKey()); err != nil {
			return fmt.Errorf("delete queue checkpoint: %w", err)
		}
		e.checkpointExists = false
		return nil
	}
	snapshot := e.snapshot()
	if err := snapshot.Validate(); err != nil {
		return err
	}
	if err := e.cfg.Checkpoint.SaveQueue(ctx, snapshot); err != nil {
		return fmt.Errorf("save queue checkpoint: %w", err)
	}
	e.checkpointExists = true
	return nil
}

func (e *Engine) emptyCheckpoint() bool {
	return len(e.pending) == 0 && len(e.active) == 0 && len(e.trees) == 0 &&
		len(e.outbox) == 0 && e.lingerSince.IsZero()
}

func (e *Engine) queueKey() checkpoint.QueueKey {
	return checkpoint.QueueKey{Owner: e.cfg.Owner, Repo: e.cfg.Repo, Base: e.cfg.Base}
}

func (e *Engine) snapshot() checkpoint.QueueSnapshot {
	active := make([]checkpoint.ActiveBatchSnapshot, len(e.active))
	for i, a := range e.active {
		active[i] = e.snapshotBatch(a)
	}
	pendingNodes := make([]checkpoint.PendingNodeSnapshot, len(e.pending))
	for i, candidate := range e.pending {
		pendingNodes[i] = checkpoint.PendingNodeSnapshot{
			PRs:   append([]int(nil), candidate...),
			RunID: e.lineageRunID[candidate[0]],
			Path:  e.lineage[candidate[0]],
		}
	}
	trees := make([]checkpoint.BisectionTreeSnapshot, 0, len(e.trees))
	for runID, tree := range e.trees {
		ts := checkpoint.BisectionTreeSnapshot{
			RunID:  runID,
			Anchor: e.rootAnchor[runID],
			// Open is derived on load from the restored active/pending nodes
			// (treeHasUnresolvedNode); it is persisted only as an at-a-glance
			// count for anyone inspecting the checkpoint.
			Open:     e.unresolvedNodeCount(runID),
			Cursor:   tree.cursor,
			Accepted: snapshotPRs(tree.accepted),
			Results:  tree.results,
		}
		for _, leaf := range tree.held {
			ts.Held = append(ts.Held, checkpoint.HeldLeafSnapshot{
				Batch:   e.snapshotBatch(leaf.batch),
				Outcome: leaf.outcome,
			})
		}
		trees = append(trees, ts)
	}
	outbox := make([]checkpoint.OutboxTransitionSnapshot, len(e.outbox))
	for i, entry := range e.outbox {
		outbox[i] = checkpoint.OutboxTransitionSnapshot{
			Kind:          entry.t.Kind,
			PRs:           append([]int(nil), entry.t.PRs...),
			StagingBranch: entry.t.StagingBranch,
			RunID:         entry.t.RunID,
			LineagePath:   entry.t.LineagePath,
			Reason:        entry.t.Reason,
			EventID:       entry.t.EventID,
			Attempts:      entry.attempts,
		}
	}
	return checkpoint.QueueSnapshot{
		FormatVersion:    checkpoint.CurrentFormatVersion,
		Key:              e.queueKey(),
		Pending:          clonePending(e.pending),
		PendingNodes:     pendingNodes,
		Active:           active,
		LingerSince:      e.lingerSince,
		BaseGeneration:   e.baseGen,
		StagingSequence:  e.stagingSeq,
		Trees:            trees,
		TransitionOutbox: outbox,
	}
}

// snapshotBatch is the single place an in-memory activeBatch is projected into
// its durable shape — used for both the live active list and a tree's held
// leaves, so a new field is added in exactly one spot.
func (e *Engine) snapshotBatch(a *activeBatch) checkpoint.ActiveBatchSnapshot {
	return checkpoint.ActiveBatchSnapshot{
		PRs:                snapshotPRs(a.prs),
		StagingBranch:      a.stagingBranch,
		StagingSHA:         a.stagingSHA,
		BaseGeneration:     a.baseGen,
		Outcome:            a.outcome,
		PhaseSince:         a.phaseSince,
		MissingGateRetries: a.missingGateRetries,
		RunID:              a.runID,
		LineagePath:        a.lineagePath,
		ExactKey:           a.exactKey,
		BaseAnchor:         e.rootAnchor[a.runID],
		DebugURL:           a.debugURL,
		Speculative:        a.speculative,
	}
}

// restoreBatch is the inverse of snapshotBatch: the field mapping only, with
// the caller supplying the re-fetched PRs.
func (e *Engine) restoreBatch(snap checkpoint.ActiveBatchSnapshot, prs []forge.PullRequest) *activeBatch {
	return &activeBatch{
		prs:                prs,
		stagingBranch:      snap.StagingBranch,
		stagingSHA:         snap.StagingSHA,
		baseGen:            snap.BaseGeneration,
		outcome:            snap.Outcome,
		phase:              phaseForOutcome(snap.Outcome),
		phaseSince:         orNow(snap.PhaseSince, e.now()),
		missingGateRetries: snap.MissingGateRetries,
		runID:              snap.RunID,
		lineagePath:        snap.LineagePath,
		exactKey:           snap.ExactKey,
		debugURL:           snap.DebugURL,
		speculative:        snap.Speculative,
	}
}

func snapshotPRs(prs []forge.PullRequest) []checkpoint.PullRequestSnapshot {
	out := make([]checkpoint.PullRequestSnapshot, len(prs))
	for i, pr := range prs {
		out[i] = checkpoint.PullRequestSnapshot{Number: pr.Number, HeadSHA: pr.Head.Sha}
	}
	return out
}

// refetchPRs re-reads each persisted PR so a resumed batch carries a full
// PullRequest. With strict=false (a live active batch) a PR that closed or
// merged during the downtime is dropped — land() observes the merge. With
// strict=true (accepted / held-leaf evidence) any state or head change is an
// error: the evidence was gathered against a PR that no longer exists as
// tested, and this snapshot cannot be safely resumed.
func (e *Engine) refetchPRs(ctx context.Context, snaps []checkpoint.PullRequestSnapshot, strict bool, what string) ([]forge.PullRequest, error) {
	out := make([]forge.PullRequest, 0, len(snaps))
	for _, ps := range snaps {
		pr, err := e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, ps.Number)
		if err != nil {
			return nil, fmt.Errorf("resume %s PR #%d: %w", what, ps.Number, err)
		}
		if strict {
			if pr.State != "open" || pr.Merged || pr.Head.Sha != ps.HeadSHA {
				return nil, fmt.Errorf("resume %s PR #%d changed", what, ps.Number)
			}
		} else if pr.State != "open" || pr.Merged {
			continue
		}
		out = append(out, pr)
	}
	return out, nil
}

// applySnapshot restores queue state from a durable snapshot. Unlike the
// previous behavior (which re-queued staged batches for fresh staging), it
// RESUMES the staged branch: the staging branch, SHA, gate outcome and PR
// identities are durable, so a restart mid-cycle continues landing the
// already-gated batch instead of abandoning it (and orphaning the branch).
// Each PR is re-fetched so the active batch carries a full PullRequest; PRs
// that closed or merged during the downtime are dropped (a merged PR is
// observed in land()), and a fully-drained branch is deleted.
func (e *Engine) applySnapshot(ctx context.Context, snapshot checkpoint.QueueSnapshot) error {
	if snapshot.FormatVersion > checkpoint.CurrentFormatVersion {
		// A newer engine wrote this. Refuse rather than mis-read it (rollback
		// protection). Validate() normally catches this first.
		return fmt.Errorf("queue checkpoint format %d is newer than this engine (%d)", snapshot.FormatVersion, checkpoint.CurrentFormatVersion)
	}
	if snapshot.FormatVersion < checkpoint.CurrentFormatVersion && (len(snapshot.Pending) > 0 || len(snapshot.Active) > 0) {
		// A legacy checkpoint lacks the base anchor, accepted accumulator and
		// lineage the ordered frontier needs to resume a partly-tested queue
		// exactly. Do not error forever (that wedges the queue): discard the
		// in-flight state and re-derive from the forge. Staged branches from
		// the old attempt orphan and are cleaned up by the stale-branch sweep.
		e.logger.Warn("discarding a legacy queue checkpoint with in-flight work; re-deriving the queue from the forge",
			"format_version", snapshot.FormatVersion, "current", checkpoint.CurrentFormatVersion,
			"pending", len(snapshot.Pending), "active", len(snapshot.Active))
		snapshot = checkpoint.QueueSnapshot{FormatVersion: checkpoint.CurrentFormatVersion, Key: snapshot.Key}
	}
	e.pending = clonePending(snapshot.Pending)
	e.lineage = map[int]string{}
	e.lineageRunID = map[int]string{}
	e.rootAnchor = map[string]string{}
	for _, node := range snapshot.PendingNodes {
		if node.RunID == "" {
			continue
		}
		e.lineage[node.PRs[0]] = node.Path
		e.lineageRunID[node.PRs[0]] = node.RunID
	}
	e.lingerSince = snapshot.LingerSince
	e.baseGen = snapshot.BaseGeneration
	e.stagingSeq = snapshot.StagingSequence

	active := make([]*activeBatch, 0, len(snapshot.Active))
	for _, snap := range snapshot.Active {
		prs, err := e.refetchPRs(ctx, snap.PRs, false, "staged batch")
		if err != nil {
			return err
		}
		if len(prs) == 0 {
			// Every PR landed during the downtime — remove the empty branch.
			if err := e.fc.DeleteBranch(ctx, e.cfg.Owner, e.cfg.Repo, snap.StagingBranch); err != nil {
				e.logger.Warn("resume: failed to delete drained staging branch", "branch", snap.StagingBranch, "error", err)
			}
			continue
		}
		active = append(active, e.restoreBatch(snap, prs))
		if snap.RunID != "" && snap.BaseAnchor != "" {
			e.rootAnchor[snap.RunID] = snap.BaseAnchor
		}
	}
	e.active = active

	e.trees = map[string]*bisectionTree{}
	for _, ts := range snapshot.Trees {
		results := make(map[string]string, len(ts.Results))
		for key, outcome := range ts.Results {
			results[key] = outcome
		}
		tree := &bisectionTree{cursor: ts.Cursor, results: results}
		accepted, err := e.refetchPRs(ctx, ts.Accepted, true, "accepted")
		if err != nil {
			return err
		}
		tree.accepted = accepted
		for _, held := range ts.Held {
			prs, err := e.refetchPRs(ctx, held.Batch.PRs, true, "held leaf")
			if err != nil {
				return err
			}
			tree.held = append(tree.held, heldLeaf{batch: e.restoreBatch(held.Batch, prs), outcome: held.Outcome})
		}
		e.trees[ts.RunID] = tree
		if ts.Anchor != "" {
			e.rootAnchor[ts.RunID] = ts.Anchor
		}
	}

	e.outbox = e.outbox[:0]
	for _, ob := range snapshot.TransitionOutbox {
		e.outbox = append(e.outbox, outboxEntry{
			t: Transition{
				Kind:          ob.Kind,
				PRs:           append([]int(nil), ob.PRs...),
				StagingBranch: ob.StagingBranch,
				RunID:         ob.RunID,
				LineagePath:   ob.LineagePath,
				Reason:        ob.Reason,
				EventID:       ob.EventID,
			},
			attempts: ob.Attempts,
		})
	}
	return nil
}

// phaseForOutcome derives the batch phase from a persisted gate outcome so a
// resumed batch flows through checkActive correctly.
func orNow(t, fallback time.Time) time.Time {
	if t.IsZero() {
		return fallback
	}
	return t
}

func phaseForOutcome(outcome string) string {
	switch outcome {
	case "success":
		return "waiting_merge"
	case "failure", "cancelled", "error":
		return "bisecting"
	default:
		return "waiting_gate"
	}
}

func clonePending(in [][]int) [][]int {
	if in == nil {
		return nil
	}
	out := make([][]int, len(in))
	for i, cand := range in {
		out[i] = append([]int(nil), cand...)
	}
	return out
}
