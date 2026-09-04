package engine

import (
	"context"
	"testing"
	"time"

	"github.com/rbtr/shunt/internal/forge"
)

// A bisection root has one held success and one node still under test. The open
// node never gets a gate result and is abandoned after the missing-gate
// retries. Regression: the abandon used to bounce the node's PR and drop the
// batch without accounting for it, so the root waited forever on a slot
// nothing would fill and its held success was stranded. Now the whole root is
// torn down and its candidates are re-queued — no wedge, no wrongful bounce.
func TestMissingGateAbandonDoesNotStrandRoot(t *testing.T) {
	m := newMock(-1, 1, 2)
	m.runStatusSet = true
	m.runStatus = "" // no gate result for anything: the missing-gate condition
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	clock := time.Now()
	e.now = func() time.Time { return clock }
	e.checkpointLoaded = true

	runID := "run-liveness"
	e.rootAnchor = map[string]string{runID: "anchor-main"} // matches mock BranchHead

	held := &activeBatch{
		prs:           []forge.PullRequest{*m.prs[1]},
		stagingBranch: "mq/main/staging-run-liveness-r0",
		stagingSHA:    "sha-held",
		runID:         runID,
		lineagePath:   "r0",
		outcome:       "success",
	}
	open := &activeBatch{
		prs:                []forge.PullRequest{*m.prs[2]},
		stagingBranch:      "mq/main/staging-run-liveness-r1",
		stagingSHA:         "sha-open",
		runID:              runID,
		lineagePath:        "r1",
		phase:              "waiting_gate",
		phaseSince:         clock,
		missingGateRetries: missingGateMaxRetries,
	}
	e.trees[runID] = &bisectionTree{
		accepted: []forge.PullRequest{*m.prs[1]},
		results:  map[string]string{},
		held:     []heldLeaf{{batch: held, outcome: "success"}},
	}
	e.active = []*activeBatch{open}
	clock = clock.Add(2 * time.Hour)

	for i := 0; i < 40; i++ {
		if _, err := e.checkActive(context.Background()); err != nil {
			t.Fatalf("checkActive: %v", err)
		}
		m.advanceNative()
		clock = clock.Add(10 * time.Minute)
	}

	if _, ok := e.trees[runID]; ok {
		t.Fatalf("root %s was not torn down after its open node's gate never ran", runID)
	}
	// Infra failure must not be published as a PR rejection.
	if m.bounced[1] || m.bounced[2] {
		t.Fatalf("no PR should be bounced for a missing gate, bounced=%v", m.bounced)
	}
	// Both candidates go back to pending as a fresh root.
	if len(e.pending) == 0 {
		t.Fatalf("candidates were not re-queued: pending=%v", e.pending)
	}
}

// Same shape, but the open node's PR head changes mid-test. Regression: the
// head-change requeue used to detach the node from its tree under a fresh run
// id, stranding the root and its held prefix. Now
// invalidateRootsOnCandidateChange sees the active node's PR and re-roots (or
// tears down) the whole tree — nothing is left waiting.
func TestActiveNodeHeadChangeReRootsInsteadOfStranding(t *testing.T) {
	m := newMock(-1, 1, 2)
	m.runStatusSet = true
	m.runStatus = ""
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)
	clock := time.Now()
	e.now = func() time.Time { return clock }
	e.checkpointLoaded = true

	runID := "run-liveness2"
	e.rootAnchor = map[string]string{runID: "anchor-main"}
	held := &activeBatch{
		prs: []forge.PullRequest{*m.prs[1]}, stagingBranch: "b-r0", stagingSHA: "s0",
		runID: runID, lineagePath: "r0", outcome: "success",
	}
	open := &activeBatch{
		prs: []forge.PullRequest{*m.prs[2]}, stagingBranch: "b-r1", stagingSHA: "s1",
		runID: runID, lineagePath: "r1", phase: "waiting_gate", phaseSince: clock,
	}
	e.trees[runID] = &bisectionTree{
		accepted: []forge.PullRequest{*m.prs[1]},
		results:  map[string]string{}, held: []heldLeaf{{batch: held, outcome: "success"}},
	}
	e.active = []*activeBatch{open}

	m.prs[2].Head.Sha = "head-2-v2" // someone pushes to PR #2

	for i := 0; i < 30; i++ {
		if _, err := e.checkActive(context.Background()); err != nil {
			t.Fatalf("checkActive: %v", err)
		}
		clock = clock.Add(30 * time.Minute)
	}
	if _, ok := e.trees[runID]; ok {
		t.Fatalf("root %s survived a mid-test head change on an active node", runID)
	}
	// Both candidates are re-queued as a fresh root; nothing is stranded.
	if got := len(e.pending); got == 0 && len(e.active) == 0 {
		t.Fatalf("candidates were dropped, not re-queued: pending=%v active=%d", e.pending, len(e.active))
	}
}

// treeHasUnresolvedNode is the finalization gate; make sure it counts both
// staged and pending nodes and nothing else.
func TestTreeHasUnresolvedNode(t *testing.T) {
	e := New(Config{Owner: "o", Repo: "r", Base: "main"}, newMock(-1), nil)
	e.trees["R"] = &bisectionTree{results: map[string]string{}}
	if e.treeHasUnresolvedNode("R") {
		t.Fatal("empty tree should be ready")
	}
	e.active = []*activeBatch{{prs: []forge.PullRequest{{Number: 1}}, runID: "R"}}
	if !e.treeHasUnresolvedNode("R") {
		t.Fatal("an active node makes the tree unresolved")
	}
	e.active = nil
	e.pending = [][]int{{2}}
	e.lineageRunID = map[int]string{2: "R"}
	if !e.treeHasUnresolvedNode("R") {
		t.Fatal("a pending node with matching lineage makes the tree unresolved")
	}
	e.lineageRunID[2] = "OTHER"
	if e.treeHasUnresolvedNode("R") {
		t.Fatal("a pending node for a different root must not count")
	}
}
