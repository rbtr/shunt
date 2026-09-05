// Package checkpoint defines the public queue snapshot types and the store
// interface for persisting in-flight batch state across restarts.
//
// The engine's CheckpointStore was previously internal (shunt/internal/...);
// hosts wire it for durable restart survival, so the types and the interface
// are exposed here.
package checkpoint

import (
	"context"
	"fmt"
	"time"
)

// QueueKey identifies a single merge queue.
type QueueKey struct {
	Owner string `json:"Owner"`
	Repo  string `json:"Repo"`
	Base  string `json:"Base"`
}

// CurrentFormatVersion is written by the current engine. Version 1 was the
// compatibility release; version 2 carries durable bisection-tree state.
const CurrentFormatVersion = 2

// QueueSnapshot is the durable shape of queue state.
type QueueSnapshot struct {
	FormatVersion   int                     `json:"FormatVersion"`
	Key             QueueKey                `json:"Key"`
	Pending         [][]int                 `json:"Pending"`
	PendingNodes    []PendingNodeSnapshot   `json:"PendingNodes"`
	Active          []ActiveBatchSnapshot   `json:"Active"`
	LingerSince     time.Time               `json:"LingerSince"`
	BaseGeneration  int                     `json:"BaseGeneration"`
	StagingSequence int                     `json:"StagingSequence"`
	Trees           []BisectionTreeSnapshot `json:"Trees"`
	// TransitionOutbox carries terminal lifecycle records (a merge counter, a
	// bounce notification) that have not yet been re-sent enough times to be
	// certain a dropped reconcile response did not lose them. Redelivery is
	// safe: the consumer de-dups by EventID.
	TransitionOutbox []OutboxTransitionSnapshot `json:"TransitionOutbox,omitempty"`
}

// OutboxTransitionSnapshot is one pending terminal lifecycle record plus how
// many reconcile responses have already carried it.
type OutboxTransitionSnapshot struct {
	Kind          string `json:"Kind"`
	PRs           []int  `json:"PRs"`
	StagingBranch string `json:"StagingBranch"`
	RunID         string `json:"RunID"`
	LineagePath   string `json:"LineagePath"`
	Reason        string `json:"Reason"`
	EventID       string `json:"EventID"`
	Attempts      int    `json:"Attempts"`
}

// PendingNodeSnapshot preserves bisection lineage before a node is staged.
type PendingNodeSnapshot struct {
	PRs   []int  `json:"PRs"`
	RunID string `json:"RunID"`
	Path  string `json:"Path"`
}

// BisectionTreeSnapshot persists a root's held leaves and finalization cursor.
type BisectionTreeSnapshot struct {
	RunID string `json:"RunID"`
	// Anchor is the base-branch commit SHA pinned when this root opened. Every
	// exact key in Results is scoped to it, so it must be restored verbatim.
	Anchor   string                `json:"Anchor"`
	Open     int                   `json:"Open"`
	Cursor   int                   `json:"Cursor"`
	Accepted []PullRequestSnapshot `json:"Accepted"`
	Results  map[string]string     `json:"Results"`
	Held     []HeldLeafSnapshot    `json:"Held"`
}

// HeldLeafSnapshot is a gate-terminal leaf whose source action is deferred.
type HeldLeafSnapshot struct {
	Batch   ActiveBatchSnapshot `json:"Batch"`
	Outcome string              `json:"Outcome"`
}

// ActiveBatchSnapshot records a staging branch currently waiting on its gate.
// The engine resumes the staged branch after restart, preserving its gate
// wait and missing-gate retry state.
type ActiveBatchSnapshot struct {
	PRs                []PullRequestSnapshot `json:"PRs"`
	StagingBranch      string                `json:"StagingBranch"`
	StagingSHA         string                `json:"StagingSHA"`
	BaseGeneration     int                   `json:"BaseGeneration"`
	Outcome            string                `json:"Outcome"`
	PhaseSince         time.Time             `json:"PhaseSince"`
	MissingGateRetries int                   `json:"MissingGateRetries"`
	RunID              string                `json:"RunID"`
	LineagePath        string                `json:"LineagePath"`
	ExactKey           string                `json:"ExactKey"`
	// BaseAnchor is the root's pinned base-branch SHA, carried on a root batch
	// that has not split yet (and so has no BisectionTreeSnapshot).
	BaseAnchor string `json:"BaseAnchor"`
	// DebugURL is the staging gate's run link, shown in a later bounce comment;
	// Speculative marks a fanout node staged ahead of the frontier. Both are
	// restored so a checkpoint reload between the gate result and the bounce /
	// promotion keeps the link and the metric accurate.
	DebugURL    string `json:"DebugURL,omitempty"`
	Speculative bool   `json:"Speculative,omitempty"`
}

// PullRequestSnapshot is the PR identity needed to re-queue a batch.
type PullRequestSnapshot struct {
	Number  int    `json:"Number"`
	HeadSHA string `json:"HeadSHA"`
}

// Store persists queue snapshots for one engine-managed queue.
type Store interface {
	// LoadQueue retrieves the stored snapshot for the key. ok is false when
	// no snapshot exists for the key.
	LoadQueue(ctx context.Context, key QueueKey) (QueueSnapshot, bool, error)
	// SaveQueue stores the snapshot.
	SaveQueue(ctx context.Context, snapshot QueueSnapshot) error
	// DeleteQueue removes the stored snapshot for the key.
	DeleteQueue(ctx context.Context, key QueueKey) error
}

// Validate checks the snapshot shape so a host can reject malformed persisted
// data before handing it to the engine (a bad snapshot must not wedge it).
func (s QueueSnapshot) Validate() error {
	if s.FormatVersion != 0 && s.FormatVersion != 1 && s.FormatVersion != CurrentFormatVersion {
		return fmt.Errorf("unsupported queue checkpoint format version %d", s.FormatVersion)
	}
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
	for i, node := range s.PendingNodes {
		if len(node.PRs) == 0 || (node.RunID == "") != (node.Path == "") {
			return fmt.Errorf("queue checkpoint pending node %d is invalid", i)
		}
		for _, n := range node.PRs {
			if n <= 0 {
				return fmt.Errorf("queue checkpoint pending node %d has invalid PR number %d", i, n)
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
		if active.MissingGateRetries < 0 {
			return fmt.Errorf("queue checkpoint active batch %d has negative missing-gate retries", i)
		}
		if (active.RunID == "") != (active.LineagePath == "") {
			return fmt.Errorf("queue checkpoint active batch %d has incomplete lineage", i)
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
	for i, tree := range s.Trees {
		if tree.RunID == "" || tree.Open < 0 || tree.Cursor < 0 || tree.Cursor > len(tree.Held) {
			return fmt.Errorf("queue checkpoint tree %d is invalid", i)
		}
		for key, outcome := range tree.Results {
			if key == "" || (outcome != "success" && outcome != "failure" && outcome != "cancelled" && outcome != "error") {
				return fmt.Errorf("queue checkpoint tree %d has invalid cached outcome", i)
			}
		}
		for j, accepted := range tree.Accepted {
			if accepted.Number <= 0 || accepted.HeadSHA == "" {
				return fmt.Errorf("queue checkpoint tree %d accepted PR %d is invalid", i, j)
			}
		}
		for j, leaf := range tree.Held {
			if leaf.Outcome != "success" && leaf.Outcome != "failure" && leaf.Outcome != "cancelled" && leaf.Outcome != "error" {
				return fmt.Errorf("queue checkpoint tree %d leaf %d has invalid outcome %q", i, j, leaf.Outcome)
			}
			if err := validActive(leaf.Batch); err != nil {
				return fmt.Errorf("queue checkpoint tree %d leaf %d: %w", i, j, err)
			}
		}
	}
	for i, ob := range s.TransitionOutbox {
		if ob.Kind == "" || ob.EventID == "" || ob.Attempts < 0 {
			return fmt.Errorf("queue checkpoint transition outbox entry %d is invalid", i)
		}
	}
	return nil
}

func validActive(active ActiveBatchSnapshot) error {
	if active.StagingBranch == "" || active.StagingSHA == "" || active.BaseGeneration < 0 || active.MissingGateRetries < 0 || len(active.PRs) == 0 {
		return fmt.Errorf("invalid active batch")
	}
	for _, pr := range active.PRs {
		if pr.Number <= 0 || pr.HeadSHA == "" {
			return fmt.Errorf("invalid active batch PR")
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
	out.PendingNodes = make([]PendingNodeSnapshot, len(s.PendingNodes))
	for i, node := range s.PendingNodes {
		out.PendingNodes[i] = node
		out.PendingNodes[i].PRs = append([]int(nil), node.PRs...)
	}
	out.Active = make([]ActiveBatchSnapshot, len(s.Active))
	for i, active := range s.Active {
		out.Active[i] = active
		out.Active[i].PRs = append([]PullRequestSnapshot(nil), active.PRs...)
	}
	out.Trees = make([]BisectionTreeSnapshot, len(s.Trees))
	for i, tree := range s.Trees {
		out.Trees[i] = tree
		out.Trees[i].Accepted = append([]PullRequestSnapshot(nil), tree.Accepted...)
		out.Trees[i].Results = make(map[string]string, len(tree.Results))
		for key, outcome := range tree.Results {
			out.Trees[i].Results[key] = outcome
		}
		out.Trees[i].Held = append([]HeldLeafSnapshot(nil), tree.Held...)
		for j := range out.Trees[i].Held {
			out.Trees[i].Held[j].Batch.PRs = append([]PullRequestSnapshot(nil), tree.Held[j].Batch.PRs...)
		}
	}
	out.TransitionOutbox = make([]OutboxTransitionSnapshot, len(s.TransitionOutbox))
	for i, ob := range s.TransitionOutbox {
		out.TransitionOutbox[i] = ob
		out.TransitionOutbox[i].PRs = append([]int(nil), ob.PRs...)
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
