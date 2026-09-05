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
	return len(e.pending) == 0 && len(e.active) == 0 && e.lingerSince.IsZero()
}

func (e *Engine) queueKey() checkpoint.QueueKey {
	return checkpoint.QueueKey{Owner: e.cfg.Owner, Repo: e.cfg.Repo, Base: e.cfg.Base}
}

func (e *Engine) snapshot() checkpoint.QueueSnapshot {
	active := make([]checkpoint.ActiveBatchSnapshot, len(e.active))
	for i, a := range e.active {
		prs := make([]checkpoint.PullRequestSnapshot, len(a.prs))
		for j, pr := range a.prs {
			prs[j] = checkpoint.PullRequestSnapshot{Number: pr.Number, HeadSHA: pr.Head.Sha}
		}
		active[i] = checkpoint.ActiveBatchSnapshot{
			PRs:                prs,
			StagingBranch:      a.stagingBranch,
			StagingSHA:         a.stagingSHA,
			BaseGeneration:     a.baseGen,
			Outcome:            a.outcome,
			PhaseSince:         a.phaseSince,
			MissingGateRetries: a.missingGateRetries,
		}
	}
	return checkpoint.QueueSnapshot{
		Key:             e.queueKey(),
		Pending:         clonePending(e.pending),
		Active:          active,
		LingerSince:     e.lingerSince,
		BaseGeneration:  e.baseGen,
		StagingSequence: e.stagingSeq,
	}
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
	e.pending = clonePending(snapshot.Pending)
	e.lingerSince = snapshot.LingerSince
	e.baseGen = snapshot.BaseGeneration
	e.stagingSeq = snapshot.StagingSequence

	active := make([]*activeBatch, 0, len(snapshot.Active))
	for _, snap := range snapshot.Active {
		prs := make([]forge.PullRequest, 0, len(snap.PRs))
		for _, ps := range snap.PRs {
			pr, err := e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, ps.Number)
			if err != nil {
				return fmt.Errorf("resume staged batch: re-fetch PR #%d: %w", ps.Number, err)
			}
			if pr.State != "open" || pr.Merged {
				continue // merged or closed during downtime
			}
			prs = append(prs, pr)
		}
		if len(prs) == 0 {
			// Every PR landed during the downtime — remove the empty branch.
			if err := e.fc.DeleteBranch(ctx, e.cfg.Owner, e.cfg.Repo, snap.StagingBranch); err != nil {
				e.logger.Warn("resume: failed to delete drained staging branch", "branch", snap.StagingBranch, "error", err)
			}
			continue
		}
		active = append(active, &activeBatch{
			prs:                prs,
			stagingBranch:      snap.StagingBranch,
			stagingSHA:         snap.StagingSHA,
			baseGen:            snap.BaseGeneration,
			outcome:            snap.Outcome,
			phase:              phaseForOutcome(snap.Outcome),
			phaseSince:         orNow(snap.PhaseSince, e.now()),
			missingGateRetries: snap.MissingGateRetries,
		})
	}
	e.active = active
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
