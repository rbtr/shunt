package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/rbtr/shunt/internal/checkpoint"
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
			PRs:            prs,
			StagingBranch:  a.stagingBranch,
			StagingSHA:     a.stagingSHA,
			BaseGeneration: a.baseGen,
			Outcome:        a.outcome,
			Phase:          a.phase,
			PhaseSince:     a.phaseSince,
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

// applySnapshot restores unresolved candidates. In-flight staging attempts are
// deliberately not resumed: after a restart, Shunt cannot prove their gate
// evidence still includes the current base and source heads. It removes each
// old staging branch and re-queues its PRs for fresh staging instead.
func (e *Engine) applySnapshot(ctx context.Context, snapshot checkpoint.QueueSnapshot) error {
	e.pending = clonePending(snapshot.Pending)
	for _, active := range snapshot.Active {
		if err := e.fc.DeleteBranch(ctx, e.cfg.Owner, e.cfg.Repo, active.StagingBranch); err != nil {
			e.logger.Warn("failed to delete restored staging branch", "branch", active.StagingBranch, "error", err)
		}
		nums := make([]int, len(active.PRs))
		for i, pr := range active.PRs {
			nums[i] = pr.Number
		}
		e.pending = append(e.pending, nums)
	}
	sort.SliceStable(e.pending, func(i, j int) bool {
		return e.pending[i][0] < e.pending[j][0]
	})
	e.active = nil
	e.lingerSince = snapshot.LingerSince
	e.baseGen = snapshot.BaseGeneration
	e.stagingSeq = snapshot.StagingSequence
	return nil
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
