// Package checkpoint defines durable queue snapshot types.
package checkpoint

import (
	"fmt"
	"time"
)

// QueueKey identifies a single merge queue.
type QueueKey struct {
	Owner string `json:"Owner"`
	Repo  string `json:"Repo"`
	Base  string `json:"Base"`
}

// QueueSnapshot is the durable shape of queue state.
type QueueSnapshot struct {
	Key             QueueKey              `json:"Key"`
	Pending         [][]int               `json:"Pending"`
	Active          []ActiveBatchSnapshot `json:"Active"`
	LingerSince     time.Time             `json:"LingerSince"`
	BaseGeneration  int                   `json:"BaseGeneration"`
	StagingSequence int                   `json:"StagingSequence"`
}

// ActiveBatchSnapshot records a staging branch currently waiting on its gate.
// Restores re-queue active batches for fresh staging instead of trusting an old
// run from before the process restart.
type ActiveBatchSnapshot struct {
	PRs            []PullRequestSnapshot `json:"PRs"`
	StagingBranch  string                `json:"StagingBranch"`
	StagingSHA     string                `json:"StagingSHA"`
	BaseGeneration int                   `json:"BaseGeneration"`
	Outcome        string                `json:"Outcome"`
}

// PullRequestSnapshot is the PR identity needed to resume a staged batch.
type PullRequestSnapshot struct {
	Number  int    `json:"Number"`
	HeadSHA string `json:"HeadSHA"`
}

// Validate checks the snapshot before it is persisted or restored.
func (s QueueSnapshot) Validate() error {
	if err := s.Key.Validate(); err != nil {
		return err
	}
	if s.BaseGeneration < 0 {
		return fmt.Errorf("queue checkpoint has negative base generation")
	}
	if s.StagingSequence < 0 {
		return fmt.Errorf("queue checkpoint has negative staging sequence")
	}
	for i, cand := range s.Pending {
		if len(cand) == 0 {
			return fmt.Errorf("queue checkpoint pending candidate %d is empty", i)
		}
		for _, n := range cand {
			if n <= 0 {
				return fmt.Errorf("queue checkpoint pending candidate %d has invalid PR number %d", i, n)
			}
		}
	}
	for i, active := range s.Active {
		if active.StagingBranch == "" {
			return fmt.Errorf("queue checkpoint active batch %d missing staging branch", i)
		}
		if active.StagingSHA == "" {
			return fmt.Errorf("queue checkpoint active batch %d missing staging SHA", i)
		}
		if active.BaseGeneration < 0 {
			return fmt.Errorf("queue checkpoint active batch %d has negative base generation", i)
		}
		if active.Outcome != "" && active.Outcome != "success" && active.Outcome != "failure" && active.Outcome != "cancelled" && active.Outcome != "error" {
			return fmt.Errorf("queue checkpoint active batch %d has invalid outcome %q", i, active.Outcome)
		}
		if len(active.PRs) == 0 {
			return fmt.Errorf("queue checkpoint active batch %d has no PRs", i)
		}
		for j, pr := range active.PRs {
			if pr.Number <= 0 {
				return fmt.Errorf("queue checkpoint active batch %d PR %d has invalid number %d", i, j, pr.Number)
			}
			if pr.HeadSHA == "" {
				return fmt.Errorf("queue checkpoint active batch %d PR %d missing head SHA", i, j)
			}
		}
	}
	return nil
}

// Validate checks that the queue key can safely address one storage row.
func (k QueueKey) Validate() error {
	if k.Owner == "" {
		return fmt.Errorf("queue checkpoint owner is required")
	}
	if k.Repo == "" {
		return fmt.Errorf("queue checkpoint repo is required")
	}
	if k.Base == "" {
		return fmt.Errorf("queue checkpoint base is required")
	}
	return nil
}

// Clone returns a deep copy of the snapshot.
func (s QueueSnapshot) Clone() QueueSnapshot {
	out := s
	out.Pending = clonePending(s.Pending)
	out.Active = make([]ActiveBatchSnapshot, len(s.Active))
	for i, active := range s.Active {
		out.Active[i] = active
		out.Active[i].PRs = append([]PullRequestSnapshot(nil), active.PRs...)
	}
	return out
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
