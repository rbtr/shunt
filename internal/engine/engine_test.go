package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rbtr/shunt/internal/checkpoint"
	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
	"github.com/rbtr/shunt/internal/metrics"
)

// mock implements both ForgeAPI and Stager. A staged batch "fails" iff it
// contains badPR; merges/bounces are recorded.
type mock struct {
	prs            map[int]*forge.PullRequest
	automerge      map[int]bool
	batchOf        map[string][]int // staging sha -> PR numbers
	badPR          int
	failMerge      int
	conflictPR     int
	conflictBasePR int
	conflictFirst  bool
	statuses       []string
	runStatus      string
	staged         [][]int
	merged         []int
	bounced        map[int]bool
	comments       map[int][]string
	queueComments  map[int][]string
	mergeHeads     map[int][]string
	runURLs        map[string]string
	beforeMerge    func(int)
}

func newMock(badPR int, prNums ...int) *mock {
	m := &mock{
		prs: map[int]*forge.PullRequest{}, automerge: map[int]bool{},
		batchOf: map[string][]int{}, badPR: badPR, bounced: map[int]bool{},
		comments: map[int][]string{}, queueComments: map[int][]string{}, mergeHeads: map[int][]string{}, runURLs: map[string]string{},
	}
	for _, n := range prNums {
		m.addPR(n)
	}
	return m
}

func (m *mock) addPR(n int) {
	pr := &forge.PullRequest{Number: n, State: "open"}
	pr.Head.Sha = fmt.Sprintf("head-%d", n)
	pr.Base.Ref = "main"
	m.prs[n] = pr
	m.automerge[n] = true
}

func (m *mock) ListOpenPRs(_ context.Context, _, _, _ string) ([]forge.PullRequest, error) {
	var out []forge.PullRequest
	for _, pr := range m.prs {
		if pr.State == "open" {
			out = append(out, *pr)
		}
	}
	return out, nil
}
func (m *mock) GetPR(_ context.Context, _, _ string, n int) (forge.PullRequest, error) {
	return *m.prs[n], nil
}
func (m *mock) AutomergeScheduled(_ context.Context, _, _ string, n int) (bool, error) {
	return m.automerge[n], nil
}
func (m *mock) SetCommitStatus(_ context.Context, _, _, sha, _, state, _, _ string) error {
	m.statuses = append(m.statuses, sha+":"+state)
	return nil
}
func (m *mock) Comment(_ context.Context, _, _ string, n int, body string) error {
	m.comments[n] = append(m.comments[n], body)
	return nil
}
func (m *mock) UpsertComment(_ context.Context, _, _ string, n int, _, _, body string) error {
	m.queueComments[n] = append(m.queueComments[n], body)
	return nil
}
func (m *mock) DeleteBranch(_ context.Context, _, _, _ string) error { return nil }

func (m *mock) RunStatus(_ context.Context, _, _, sha, _ string) (string, error) {
	if m.runStatus != "" {
		return m.runStatus, nil
	}
	for _, n := range m.batchOf[sha] {
		if n == m.badPR {
			return "failure", nil
		}
	}
	return "success", nil
}
func (m *mock) RunTargetURL(_ context.Context, _, _, sha, _ string) (string, error) {
	return m.runURLs[sha], nil
}

func (m *mock) MergePR(_ context.Context, _, _ string, n int, _, headSHA string) error {
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

func (m *mock) CancelAutomerge(_ context.Context, _, _ string, n int) error {
	m.bounced[n] = true
	m.automerge[n] = false
	return nil
}

func (m *mock) BuildStaging(_ context.Context, _, _ string, refs []gitops.MergedRef) (string, int, error) {
	var nums []int
	for _, r := range refs {
		nums = append(nums, r.PR)
	}
	m.staged = append(m.staged, append([]int(nil), nums...))
	baseMerged := m.conflictBasePR > 0 && m.prs[m.conflictBasePR].Merged
	if idx := indexOfNum(nums, m.conflictPR); idx > 0 || (idx == 0 && (m.conflictFirst || baseMerged)) {
		return "", m.conflictPR, fmt.Errorf("staging conflict")
	}
	sha := fmt.Sprintf("stage-%v", nums)
	m.batchOf[sha] = nums
	return sha, 0, nil
}

func drive(e *Engine, n int) {
	for i := 0; i < n; i++ {
		_ = e.Reconcile(context.Background())
	}
}

func TestBatchLingerDisabledByDefaultStartsImmediately(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if len(e.active) == 0 {
		t.Fatal("default config should start a batch immediately")
	}
	if got := len(m.batchOf); got != 1 {
		t.Errorf("staging runs = %d, want 1", got)
	}
}

func TestQueueStatusCommentsDisabledByDefault(t *testing.T) {
	m := newMock(-1, 1)
	m.runStatus = "running"
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if got := len(m.queueComments); got != 0 {
		t.Fatalf("queue comments = %d, want 0", got)
	}
}

func TestQueueStatusCommentsAreStickyAndConcise(t *testing.T) {
	m := newMock(-1, 1, 2)
	m.runStatus = "running"
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", QueueComments: true, BotUser: "mq-bot"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}

	if got := len(m.queueComments[1]); got != 1 {
		t.Fatalf("PR 1 queue comment updates = %d, want 1", got)
	}
	body := m.queueComments[1][0]
	for _, want := range []string{
		queueCommentMarker,
		"Repository: `o/r`",
		"Base: `main`",
		"Position: 1/2",
		"State: testing in active batch",
		"Active batch: #1, #2 on `mq/main/staging`",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("queue comment missing %q in:\n%s", want, body)
		}
	}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("unchanged running batch: %v", err)
	}
	if got := len(m.queueComments[1]); got != 1 {
		t.Fatalf("unchanged queue comment updates = %d, want 1", got)
	}
}

func TestQueueStatusCommentMarksCachedPRNotQueued(t *testing.T) {
	m := newMock(-1, 1)
	m.runStatus = "running"
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", QueueComments: true}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	m.runStatus = "success"
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("land batch: %v", err)
	}

	if got := len(m.queueComments[1]); got != 2 {
		t.Fatalf("PR 1 queue comment updates = %d, want 2", got)
	}
	if body := m.queueComments[1][1]; !strings.Contains(body, "State: Landed via merge queue") {
		t.Fatalf("final queue comment did not mark PR as landed:\n%s", body)
	}
}

func TestBatchLingerWaitsWhileUnderTargetAndWindow(t *testing.T) {
	m := newMock(-1, 1, 2)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", BatchLinger: 10 * time.Second, BatchTarget: 3}, m, m)
	now := time.Unix(100, 0)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start linger: %v", err)
	}
	now = now.Add(9 * time.Second)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("continue linger: %v", err)
	}

	if len(e.active) != 0 {
		t.Fatal("batch should not start before target or linger window")
	}
	if got := len(m.batchOf); got != 0 {
		t.Errorf("staging runs = %d, want 0", got)
	}
}

func TestBatchLingerStartsWhenTargetReached(t *testing.T) {
	m := newMock(-1, 1, 2)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", BatchLinger: 10 * time.Second, BatchTarget: 3}, m, m)
	now := time.Unix(100, 0)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start linger: %v", err)
	}
	m.addPR(3)
	now = now.Add(time.Second)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("target reached: %v", err)
	}

	if len(e.active) == 0 {
		t.Fatal("batch should start once target is reached")
	}
	if got := fmt.Sprint(m.batchOf[e.active[0].stagingSHA]); got != "[1 2 3]" {
		t.Errorf("staged batch = %s, want [1 2 3]", got)
	}
	if !e.lingerSince.IsZero() {
		t.Fatal("linger state should reset after batch starts")
	}
}

func TestBatchLingerStartsWhenWindowExpires(t *testing.T) {
	m := newMock(-1, 1, 2)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", BatchLinger: 10 * time.Second, BatchTarget: 3}, m, m)
	now := time.Unix(100, 0)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start linger: %v", err)
	}
	now = now.Add(10 * time.Second)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("window expired: %v", err)
	}

	if len(e.active) == 0 {
		t.Fatal("batch should start once linger window expires")
	}
	if got := fmt.Sprint(m.batchOf[e.active[0].stagingSHA]); got != "[1 2]" {
		t.Errorf("staged batch = %s, want [1 2]", got)
	}
}

func TestBatchLingerResetsAfterBatchStarts(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging", BatchLinger: 10 * time.Second, BatchTarget: 2}, m, m)
	now := time.Unix(100, 0)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start first linger: %v", err)
	}
	m.addPR(2)
	now = now.Add(time.Second)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start first batch: %v", err)
	}
	if !e.lingerSince.IsZero() {
		t.Fatal("linger state should reset when first batch starts")
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("land first batch: %v", err)
	}

	m.addPR(3)
	now = now.Add(10 * time.Second)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start second linger: %v", err)
	}

	if len(e.active) != 0 {
		t.Fatal("new ready PR should get a fresh linger window after prior batch")
	}
	if got := len(m.batchOf); got != 1 {
		t.Errorf("staging runs = %d, want still 1", got)
	}
	if e.lingerSince.IsZero() || !e.lingerSince.Equal(now) {
		t.Fatalf("second linger started at %v, want %v", e.lingerSince, now)
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
	if got := fmt.Sprint(m.statuses); !strings.Contains(got, "head-3:failure") {
		t.Errorf("statuses = %s, want bounced PR failure status", got)
	}
	if got := strings.Join(m.comments[3], "\n"); !strings.Contains(got, "Bounced from merge queue") || !strings.Contains(got, "staging run/commit") {
		t.Errorf("bounce comment missing reason/debug link:\n%s", got)
	}
}

func TestTerminalGateOutcomeStatusState(t *testing.T) {
	for _, tc := range []struct {
		outcome string
		want    string
	}{
		{outcome: "failure", want: "head-1:failure"},
		{outcome: "cancelled", want: "head-1:error"},
		{outcome: "error", want: "head-1:error"},
	} {
		t.Run(tc.outcome, func(t *testing.T) {
			m := newMock(-1, 1)
			m.runStatus = tc.outcome
			e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

			drive(e, 2)

			if !m.bounced[1] {
				t.Fatal("PR 1 should have been bounced")
			}
			if got := m.statuses[len(m.statuses)-1]; got != tc.want {
				t.Errorf("last status = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestParallelBisectionStartsSplitSubtreesTogether(t *testing.T) {
	m := newMock(1, 1, 2, 3, 4)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging", BisectFanout: 2}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start root batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("split root batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start split subtrees: %v", err)
	}

	if got := fmt.Sprint(m.staged); got != "[[1 2 3 4] [1 2] [3 4]]" {
		t.Fatalf("staged = %s, want root plus both split subtrees", got)
	}
	if got := len(e.active); got != 2 {
		t.Fatalf("active batches = %d, want 2", got)
	}

	drive(e, 30)
	sort.Ints(m.merged)
	if got := fmt.Sprint(m.merged); got != "[2 3 4]" {
		t.Errorf("merged = %s, want [2 3 4]", got)
	}
	if !m.bounced[1] {
		t.Error("PR 1 should have been bounced")
	}
}

func TestCheckpointRestoresActiveBatchByRestaging(t *testing.T) {
	m := newMock(-1, 1, 2)
	store := &memoryCheckpointStore{}
	cfg := Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging", Checkpoint: store}
	e := New(cfg, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if got := len(m.staged); got != 1 {
		t.Fatalf("staged batches before restart = %d, want 1", got)
	}
	if store.saved == nil || len(store.saved.Active) != 1 {
		t.Fatalf("checkpoint active batches = %v, want 1 active batch", store.saved)
	}

	restarted := New(cfg, m, m)
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatalf("restage restored batch: %v", err)
	}
	if got := len(m.staged); got != 2 {
		t.Fatalf("restored active batch should be restaged; staged = %d, want 2", got)
	}
	if got := fmt.Sprint(m.merged); got != "[]" {
		t.Fatalf("merged after restage = %s, want []", got)
	}
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatalf("land restaged batch: %v", err)
	}
	if got := fmt.Sprint(m.merged); got != "[1 2]" {
		t.Errorf("merged after restore = %s, want [1 2]", got)
	}
	if !store.deleted {
		t.Error("empty queue should delete checkpoint after restored batch lands")
	}
}

func TestCheckpointRestoresPendingBisectionFrontier(t *testing.T) {
	m := newMock(3, 1, 2, 3, 4)
	store := &memoryCheckpointStore{}
	cfg := Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging", Checkpoint: store}
	e := New(cfg, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start root batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("split root batch: %v", err)
	}
	if store.saved == nil {
		t.Fatal("expected pending frontier checkpoint")
	}
	if got := fmt.Sprint(store.saved.Pending); got != "[[1 2] [3 4]]" {
		t.Fatalf("checkpoint pending = %s, want [[1 2] [3 4]]", got)
	}

	restarted := New(cfg, m, m)
	drive(restarted, 30)

	if got := fmt.Sprint(m.staged); got != "[[1 2 3 4] [1 2] [3 4] [3] [4]]" {
		t.Errorf("staged after restoring frontier = %s, want resumed bisection without root rerun", got)
	}
	sort.Ints(m.merged)
	if got := fmt.Sprint(m.merged); got != "[1 2 4]" {
		t.Errorf("merged after restore = %s, want [1 2 4]", got)
	}
	if !m.bounced[3] {
		t.Error("PR 3 should be bounced after restored bisection")
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

func TestForgeCompletedMergeDoesNotOverwriteSuccessStatus(t *testing.T) {
	m := newMock(-1, 1)
	m.failMerge = 1
	m.beforeMerge = func(n int) {
		m.prs[n].State = "closed"
		m.prs[n].Merged = true
		m.automerge[n] = false
	}
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)
	drive(e, 2)

	if got := fmt.Sprint(m.statuses); got != "[head-1:success]" {
		t.Errorf("statuses = %s, want only success", got)
	}
	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, "Landed via merge queue") {
		t.Errorf("comments missing landed outcome:\n%s", got)
	}
}

func TestLandRevalidatesHappyPath(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}

	if err := e.Reconcile(context.Background()); err != nil {
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

func TestTerminalCommentsUseRunTargetURLWhenAvailable(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	m.runURLs[e.active[0].stagingSHA] = "https://forge.example.com/o/r/actions/runs/7"
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("land batch: %v", err)
	}

	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, "https://forge.example.com/o/r/actions/runs/7") {
		t.Fatalf("terminal comment did not use run target URL:\n%s", got)
	}
}

func TestLandSkipsChangedHead(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	m.prs[1].Head.Sha = "head-1-new"
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("land batch: %v", err)
	}

	if got := fmt.Sprint(m.merged); got != "[]" {
		t.Errorf("merged = %s, want []", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1-new:error]" {
		t.Errorf("statuses = %s, want error on current head", got)
	}
	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, "Skipped by merge queue") || !strings.Contains(got, "head changed from head-1 to head-1-new") {
		t.Errorf("comments = %s, want changed-head skip comment", got)
	}
}

func TestLandSkipsCancelledAutomerge(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	m.automerge[1] = false
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("land batch: %v", err)
	}

	if got := fmt.Sprint(m.merged); got != "[]" {
		t.Errorf("merged = %s, want []", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1:error]" {
		t.Errorf("statuses = %s, want error on skipped head", got)
	}
	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, "Skipped by merge queue") || !strings.Contains(got, "auto-merge is no longer scheduled") {
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

			if err := e.Reconcile(context.Background()); err != nil {
				t.Fatalf("start batch: %v", err)
			}
			m.prs[1].State = tc.state
			m.prs[1].Merged = tc.merged
			if err := e.Reconcile(context.Background()); err != nil {
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

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("land batch: %v", err)
	}

	if got := fmt.Sprint(m.mergeHeads[1]); got != "[head-1]" {
		t.Errorf("merge head SHA = %s, want [head-1]", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1:success head-1:error head-1-new:error]" {
		t.Errorf("statuses = %s, want success/error on tested head and error on current head", got)
	}
	if got := fmt.Sprint(m.merged); got != "[]" {
		t.Errorf("merged = %s, want []", got)
	}
	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, "Skipped by merge queue") || !strings.Contains(got, "head changed from head-1 to head-1-new") {
		t.Errorf("comments = %s, want changed-head skip comment", got)
	}
}

func TestStagingConflictOnSecondPRQueuesPrefixBeforeSuffix(t *testing.T) {
	m := newMock(-1, 1, 2, 3)
	m.conflictPR = 2
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := fmt.Sprint(m.staged); got != "[[1 2 3]]" {
		t.Fatalf("staged = %s, want [[1 2 3]]", got)
	}
	if got := fmt.Sprint(e.pending); got != "[[1] [2 3]]" {
		t.Fatalf("pending after conflict = %s, want [[1] [2 3]]", got)
	}
	if m.bounced[2] {
		t.Fatal("conflicting PR should not bounce before earlier prefix is tested")
	}
}

func TestStagingConflictAfterPrefixLandsBouncesConflicter(t *testing.T) {
	m := newMock(-1, 1, 2, 3)
	m.conflictPR = 2
	m.conflictBasePR = 1
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(m.merged); got != "[1]" {
		t.Fatalf("merged after prefix = %s, want [1]", got)
	}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !m.bounced[2] {
		t.Fatal("conflicter should bounce after it still conflicts on the base that includes the prefix")
	}
	if got := fmt.Sprint(m.statuses); !strings.Contains(got, "head-2:error") {
		t.Errorf("statuses = %s, want staging conflict error status", got)
	}
	if got := fmt.Sprint(e.pending); got != "[[3]]" {
		t.Fatalf("pending after conflicter bounce = %s, want [[3]]", got)
	}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	sort.Ints(m.merged)
	if got := fmt.Sprint(m.merged); got != "[1 3]" {
		t.Errorf("merged = %s, want [1 3]", got)
	}
	if m.bounced[1] || m.bounced[3] {
		t.Fatalf("only PR 2 should bounce, bounced = %v", m.bounced)
	}
}

func TestStagingConflictAfterPrefixFailsCanLandConflicter(t *testing.T) {
	m := newMock(1, 1, 2, 3)
	m.conflictPR = 2
	m.conflictBasePR = 1
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !m.bounced[1] {
		t.Fatal("failing prefix should bounce")
	}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.bounced[2] {
		t.Fatal("conflicter should not bounce when prefix did not land")
	}
	sort.Ints(m.merged)
	if got := fmt.Sprint(m.merged); got != "[2 3]" {
		t.Errorf("merged = %s, want [2 3]", got)
	}
	if got := fmt.Sprint(m.staged); got != "[[1 2 3] [1] [2 3]]" {
		t.Errorf("staged = %s, want [[1 2 3] [1] [2 3]]", got)
	}
}

func TestStagingConflictKeepsChangedPrefixBeforeSuffix(t *testing.T) {
	m := newMock(-1, 1, 2)
	m.conflictPR = 2
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.prs[1].Head.Sha = "head-1-new"
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(e.pending); got != "[[1] [2]]" {
		t.Fatalf("pending after changed prefix = %s, want [[1] [2]]", got)
	}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(m.staged); got != "[[1 2] [1] [1]]" {
		t.Errorf("staged = %s, want changed prefix retried before suffix", got)
	}
}

func TestStagingConflictOnFirstPRBouncesAndRequeuesRest(t *testing.T) {
	m := newMock(-1, 1, 2, 3)
	m.conflictPR = 1
	m.conflictFirst = true
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !m.bounced[1] {
		t.Fatal("first PR conflict should bounce")
	}
	if got := fmt.Sprint(e.pending); got != "[[2 3]]" {
		t.Fatalf("pending after first conflict = %s, want [[2 3]]", got)
	}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	sort.Ints(m.merged)
	if got := fmt.Sprint(m.merged); got != "[2 3]" {
		t.Errorf("merged = %s, want [2 3]", got)
	}
	if m.bounced[2] || m.bounced[3] {
		t.Fatalf("rest of suffix should not bounce, bounced = %v", m.bounced)
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

func TestMetricsTrackQueueActivity(t *testing.T) {
	m := newMock(-1, 1, 2)
	c := metrics.New()
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging", Metrics: c}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start reconcile: %v", err)
	}
	assertMetric(t, c, `shunt_queue_depth{owner="o",repo="r",base="main"} 2`)
	assertMetric(t, c, `shunt_active_batch{owner="o",repo="r",base="main"} 1`)
	assertMetric(t, c, `shunt_batches_started_total{owner="o",repo="r",base="main"} 1`)
	if got, want := c.StatusSnapshot().Queues[0].ActiveBatches, [][]int{{1, 2}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("active status batches = %v, want %v", got, want)
	}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("land reconcile: %v", err)
	}
	assertMetric(t, c, `shunt_queue_depth{owner="o",repo="r",base="main"} 0`)
	assertMetric(t, c, `shunt_active_batch{owner="o",repo="r",base="main"} 0`)
	assertMetric(t, c, `shunt_pr_merges_total{owner="o",repo="r",base="main"} 2`)
	assertMetric(t, c, `shunt_gate_outcomes_total{owner="o",repo="r",base="main",outcome="success"} 1`)
	snap := c.StatusSnapshot().Queues[0]
	if snap.ActiveBatch || len(snap.ActiveBatches) != 0 || len(snap.PendingBatches) != 0 {
		t.Fatalf("status after landing = %#v, want no active or pending batches", snap)
	}
}

func TestMetricsTrackStagingConflictAndBounce(t *testing.T) {
	m := newMock(-1, 1, 2)
	m.conflictPR = 1
	m.conflictFirst = true
	c := metrics.New()
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", Metrics: c}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !m.bounced[1] {
		t.Fatal("conflicting PR should be bounced")
	}
	assertMetric(t, c, `shunt_staging_conflicts_total{owner="o",repo="r",base="main"} 1`)
	assertMetric(t, c, `shunt_bounces_total{owner="o",repo="r",base="main"} 1`)
	assertMetric(t, c, `shunt_queue_depth{owner="o",repo="r",base="main"} 1`)
}

func assertMetric(t *testing.T, c *metrics.Collector, want string) {
	t.Helper()
	var out strings.Builder
	c.WritePrometheus(&out)
	if !strings.Contains(out.String(), want) {
		t.Fatalf("metrics output missing %q in:\n%s", want, out.String())
	}
}

type memoryCheckpointStore struct {
	saved   *checkpoint.QueueSnapshot
	deleted bool
}

func (s *memoryCheckpointStore) LoadQueue(_ context.Context, _ checkpoint.QueueKey) (checkpoint.QueueSnapshot, bool, error) {
	if s.saved == nil {
		return checkpoint.QueueSnapshot{}, false, nil
	}
	return s.saved.Clone(), true, nil
}

func (s *memoryCheckpointStore) SaveQueue(_ context.Context, snapshot checkpoint.QueueSnapshot) error {
	clone := snapshot.Clone()
	s.saved = &clone
	s.deleted = false
	return nil
}

func (s *memoryCheckpointStore) DeleteQueue(_ context.Context, _ checkpoint.QueueKey) error {
	s.saved = nil
	s.deleted = true
	return nil
}
