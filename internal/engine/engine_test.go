package engine

import (
	"fmt"
	"sort"
	"testing"

	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
)

// mock implements both ForgeAPI and Stager. A staged batch "fails" iff it
// contains badPR; merges/bounces are recorded.
type mock struct {
	prs         map[int]*forge.PullRequest
	automerge   map[int]bool
	batchOf     map[string][]int // staging sha -> PR numbers
	badPR       int
	failMerge   int
	statuses    []string
	merged      []int
	bounced     map[int]bool
	comments    map[int][]string
	mergeHeads  map[int][]string
	beforeMerge func(int)
}

func newMock(badPR int, prNums ...int) *mock {
	m := &mock{
		prs: map[int]*forge.PullRequest{}, automerge: map[int]bool{},
		batchOf: map[string][]int{}, badPR: badPR, bounced: map[int]bool{},
		comments: map[int][]string{}, mergeHeads: map[int][]string{},
	}
	for _, n := range prNums {
		pr := &forge.PullRequest{Number: n, State: "open"}
		pr.Head.Sha = fmt.Sprintf("head-%d", n)
		pr.Base.Ref = "main"
		m.prs[n] = pr
		m.automerge[n] = true
	}
	return m
}

func (m *mock) ListOpenPRs(_, _, _ string) ([]forge.PullRequest, error) {
	var out []forge.PullRequest
	for _, pr := range m.prs {
		if pr.State == "open" {
			out = append(out, *pr)
		}
	}
	return out, nil
}
func (m *mock) GetPR(_, _ string, n int) (forge.PullRequest, error) { return *m.prs[n], nil }
func (m *mock) AutomergeScheduled(_, _ string, n int) (bool, error) { return m.automerge[n], nil }
func (m *mock) SetCommitStatus(_, _, sha, _, state, _, _ string) error {
	m.statuses = append(m.statuses, sha+":"+state)
	return nil
}
func (m *mock) Comment(_, _ string, n int, body string) error {
	m.comments[n] = append(m.comments[n], body)
	return nil
}
func (m *mock) DeleteBranch(_, _, _ string) error { return nil }

func (m *mock) RunStatus(_, _, sha, _ string) (string, error) {
	for _, n := range m.batchOf[sha] {
		if n == m.badPR {
			return "failure", nil
		}
	}
	return "success", nil
}

func (m *mock) MergePR(_, _ string, n int, _, headSHA string) error {
	m.mergeHeads[n] = append(m.mergeHeads[n], headSHA)
	if m.beforeMerge != nil {
		m.beforeMerge(n)
	}
	if m.prs[n].Head.Sha != headSHA {
		return fmt.Errorf("head changed: expected %s got %s", headSHA, m.prs[n].Head.Sha)
	}
	if n == m.failMerge {
		return fmt.Errorf("merge failed")
	}
	m.merged = append(m.merged, n)
	m.prs[n].State = "closed"
	m.prs[n].Merged = true
	m.automerge[n] = false
	return nil
}

func (m *mock) CancelAutomerge(_, _ string, n int) error {
	m.bounced[n] = true
	m.automerge[n] = false
	return nil
}

func (m *mock) BuildStaging(_, _ string, refs []gitops.MergedRef) (string, int, error) {
	var nums []int
	for _, r := range refs {
		nums = append(nums, r.PR)
	}
	sort.Ints(nums)
	sha := fmt.Sprintf("stage-%v", nums)
	m.batchOf[sha] = nums
	return sha, 0, nil
}

func drive(e *Engine, n int) {
	for i := 0; i < n; i++ {
		_ = e.Reconcile()
	}
}

// A 4-PR batch with one bad PR must land the 3 good PRs and isolate the bad one.
func TestBisectionIsolatesBadPR(t *testing.T) {
	m := newMock(3, 1, 2, 3, 4)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)
	drive(e, 30)

	sort.Ints(m.merged)
	if got := fmt.Sprint(m.merged); got != "[1 2 4]" {
		t.Errorf("merged = %s, want [1 2 4]", got)
	}
	if !m.bounced[3] {
		t.Error("PR 3 (the culprit) should have been bounced")
	}
	if m.prs[3].Merged {
		t.Error("PR 3 must not be merged")
	}
}

// An all-green batch lands in a single CI run (the efficiency property).
func TestAllGreenBatchLandsInOneRun(t *testing.T) {
	m := newMock(-1, 1, 2, 3)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging"}, m, m)
	drive(e, 10)

	sort.Ints(m.merged)
	if got := fmt.Sprint(m.merged); got != "[1 2 3]" {
		t.Errorf("merged = %s, want [1 2 3]", got)
	}
	if len(m.batchOf) != 1 {
		t.Errorf("all-green batch should take exactly 1 staging run, took %d", len(m.batchOf))
	}
}

func TestFailedMergeClearsSuccessStatus(t *testing.T) {
	m := newMock(-1, 1, 2)
	m.failMerge = 2
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)
	drive(e, 2)

	if got := fmt.Sprint(m.merged); got != "[1]" {
		t.Errorf("merged = %s, want [1]", got)
	}
	want := "head-2:error"
	if got := m.statuses[len(m.statuses)-1]; got != want {
		t.Errorf("last status = %s, want %s", got, want)
	}
}

func TestLandRevalidatesHappyPath(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(); err != nil {
		t.Fatalf("land batch: %v", err)
	}

	if got := fmt.Sprint(m.statuses); got != "[head-1:success]" {
		t.Errorf("statuses = %s, want [head-1:success]", got)
	}
	if got := fmt.Sprint(m.merged); got != "[1]" {
		t.Errorf("merged = %s, want [1]", got)
	}
	if got := fmt.Sprint(m.mergeHeads[1]); got != "[head-1]" {
		t.Errorf("merge head SHA = %s, want [head-1]", got)
	}
}

func TestLandSkipsChangedHead(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	m.prs[1].Head.Sha = "head-1-new"
	if err := e.Reconcile(); err != nil {
		t.Fatalf("land batch: %v", err)
	}

	assertNoLand(t, m)
	if got := fmt.Sprint(m.comments[1]); got != "[Skipped by the merge queue: head changed from head-1 to head-1-new.]" {
		t.Errorf("comments = %s, want changed-head skip comment", got)
	}
}

func TestLandSkipsCancelledAutomerge(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	m.automerge[1] = false
	if err := e.Reconcile(); err != nil {
		t.Fatalf("land batch: %v", err)
	}

	assertNoLand(t, m)
	if got := fmt.Sprint(m.comments[1]); got != "[Skipped by the merge queue: auto-merge is no longer scheduled.]" {
		t.Errorf("comments = %s, want cancelled-auto-merge skip comment", got)
	}
}

func TestLandSkipsClosedOrMergedPR(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  string
		merged bool
	}{
		{name: "closed", state: "closed"},
		{name: "merged", state: "closed", merged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMock(-1, 1)
			e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

			if err := e.Reconcile(); err != nil {
				t.Fatalf("start batch: %v", err)
			}
			m.prs[1].State = tc.state
			m.prs[1].Merged = tc.merged
			if err := e.Reconcile(); err != nil {
				t.Fatalf("land batch: %v", err)
			}

			assertNoLand(t, m)
			if got := len(m.comments[1]); got != 0 {
				t.Errorf("comments = %d, want 0", got)
			}
		})
	}
}

func TestLandHandlesHeadChangeBetweenRevalidationAndMerge(t *testing.T) {
	m := newMock(-1, 1)
	changed := false
	m.beforeMerge = func(n int) {
		if n == 1 && !changed {
			m.prs[1].Head.Sha = "head-1-new"
			changed = true
		}
	}
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(); err != nil {
		t.Fatalf("land batch: %v", err)
	}

	if got := fmt.Sprint(m.mergeHeads[1]); got != "[head-1]" {
		t.Errorf("merge head SHA = %s, want [head-1]", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1:success head-1:error]" {
		t.Errorf("statuses = %s, want success then error on tested head", got)
	}
	if got := fmt.Sprint(m.merged); got != "[]" {
		t.Errorf("merged = %s, want []", got)
	}
	if got := fmt.Sprint(m.comments[1]); got != "[Skipped by the merge queue: head changed from head-1 to head-1-new.]" {
		t.Errorf("comments = %s, want changed-head skip comment", got)
	}
}

func TestRemoveNum(t *testing.T) {
	if got := fmt.Sprint(removeNum([]int{1, 2, 3, 4}, 3)); got != "[1 2 4]" {
		t.Errorf("removeNum = %s, want [1 2 4]", got)
	}
}

func assertNoLand(t *testing.T, m *mock) {
	t.Helper()
	if got := fmt.Sprint(m.statuses); got != "[]" {
		t.Errorf("statuses = %s, want []", got)
	}
	if got := fmt.Sprint(m.merged); got != "[]" {
		t.Errorf("merged = %s, want []", got)
	}
}
