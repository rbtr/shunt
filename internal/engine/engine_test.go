package engine

import (
	"context"
	"errors"
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
	prs                  map[int]*forge.PullRequest
	automerge            map[int]bool
	scheduleLive         map[int]bool
	automergeAt          map[int]time.Time
	latestStatus         map[int]forge.CommitStatus
	nativePending        map[int]string
	batchOf              map[string][]int // staging sha -> PR numbers
	badPR                int
	failNativeMerge      int
	conflictPR           int
	conflictBasePR       int
	conflictFirst        bool
	statuses             []string
	runStatus            string
	staged               [][]int
	stagingBranches      []string
	merged               []int
	bounced              map[int]bool
	comments             map[int][]string
	queueComments        map[int][]string
	runURLs              map[string]string
	scheduled            []int
	calls                []string
	eventSeq             int64
	beforeNative         func(int)
	beforeAutomergeState func(int)
	beforeGetPR          func(int)
	beforeRunStatus      func(string)
	beforeRunURL         func(string)
	failStatusState      string
	failStatusCount      int
	getPRErrors          map[int]int
	listCalls            int
	listErr              error
	runStatusErr         error
	upsertErr            error
}

func newMock(badPR int, prNums ...int) *mock {
	m := &mock{
		prs: map[int]*forge.PullRequest{}, automerge: map[int]bool{}, scheduleLive: map[int]bool{}, automergeAt: map[int]time.Time{},
		latestStatus: map[int]forge.CommitStatus{}, nativePending: map[int]string{},
		batchOf: map[string][]int{}, badPR: badPR, bounced: map[int]bool{},
		comments: map[int][]string{}, queueComments: map[int][]string{}, runURLs: map[string]string{},
		getPRErrors: map[int]int{}, eventSeq: 100,
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
	m.scheduleLive[n] = true
	m.automergeAt[n] = m.nextEventTime()
}

func (m *mock) ListOpenPRs(_ context.Context, _, _, _ string) ([]forge.PullRequest, error) {
	m.listCalls++
	if m.listErr != nil {
		return nil, m.listErr
	}
	var out []forge.PullRequest
	for _, pr := range m.prs {
		if pr.State == "open" {
			out = append(out, *pr)
		}
	}
	return out, nil
}
func (m *mock) GetPR(_ context.Context, _, _ string, n int) (forge.PullRequest, error) {
	if m.beforeGetPR != nil {
		m.beforeGetPR(n)
	}
	if m.getPRErrors[n] > 0 {
		m.getPRErrors[n]--
		return forge.PullRequest{}, fmt.Errorf("get PR #%d", n)
	}
	return *m.prs[n], nil
}
func (m *mock) AutomergeState(_ context.Context, _, _ string, n int) (forge.AutomergeState, error) {
	if m.beforeAutomergeState != nil {
		m.beforeAutomergeState(n)
	}
	return forge.AutomergeState{Scheduled: m.automerge[n], UpdatedAt: m.automergeAt[n]}, nil
}
func (m *mock) LatestCommitStatus(_ context.Context, _, _, sha, statusContext string) (forge.CommitStatus, bool, error) {
	for n, pr := range m.prs {
		if pr.Head.Sha == sha {
			status, ok := m.latestStatus[n]
			if ok && status.Context == statusContext {
				return status, true, nil
			}
		}
	}
	return forge.CommitStatus{}, false, nil
}
func (m *mock) SetCommitStatus(_ context.Context, _, _, sha, statusContext, state, desc, _ string) error {
	if state == m.failStatusState && m.failStatusCount > 0 {
		m.failStatusCount--
		return fmt.Errorf("set %s status", state)
	}
	m.statuses = append(m.statuses, sha+":"+state)
	m.calls = append(m.calls, "status:"+state)
	for n, pr := range m.prs {
		if pr.Head.Sha == sha {
			m.latestStatus[n] = forge.CommitStatus{
				ID:          m.eventSeq,
				Status:      state,
				Description: desc,
				Context:     statusContext,
				CreatedAt:   m.nextEventTime(),
			}
			if state == "success" && m.scheduleLive[n] {
				m.nativePending[n] = sha
			}
		}
	}
	return nil
}
func (m *mock) Comment(_ context.Context, _, _ string, n int, body string) error {
	m.comments[n] = append(m.comments[n], body)
	return nil
}
func (m *mock) UpsertComment(_ context.Context, _, _ string, n int, marker, _, body string) error {
	if m.upsertErr != nil {
		return m.upsertErr
	}
	if marker == outcomeCommentMarker {
		m.comments[n] = append(m.comments[n], body)
		if strings.Contains(body, "Bounced from merge queue") {
			m.bounced[n] = true
		}
		return nil
	}
	m.queueComments[n] = append(m.queueComments[n], body)
	return nil
}

func (m *mock) RunStatus(_ context.Context, _, _, sha, _ string) (string, error) {
	if m.beforeRunStatus != nil {
		m.beforeRunStatus(sha)
	}
	if m.runStatusErr != nil {
		return "", m.runStatusErr
	}
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
	if m.beforeRunURL != nil {
		m.beforeRunURL(sha)
	}
	return m.runURLs[sha], nil
}

func (m *mock) ScheduleAutomerge(_ context.Context, _, _ string, n int, _, _ string) error {
	m.calls = append(m.calls, fmt.Sprintf("schedule:%d", n))
	if m.scheduleLive[n] {
		return nil
	}
	m.scheduled = append(m.scheduled, n)
	m.automerge[n] = true
	m.scheduleLive[n] = true
	m.automergeAt[n] = m.nextEventTime()
	return nil
}

func (m *mock) CancelAutomerge(_ context.Context, _, _ string, n int) (bool, error) {
	m.calls = append(m.calls, fmt.Sprintf("cancel:%d", n))
	if !m.scheduleLive[n] {
		return false, nil
	}
	m.scheduleLive[n] = false
	m.automerge[n] = false
	m.automergeAt[n] = m.nextEventTime()
	return true, nil
}

func (m *mock) nextEventTime() time.Time {
	m.eventSeq++
	return time.Now().Add(time.Duration(m.eventSeq) * time.Nanosecond)
}

func (m *mock) advanceNative() {
	nums := make([]int, 0, len(m.nativePending))
	for n := range m.nativePending {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	for _, n := range nums {
		headSHA := m.nativePending[n]
		delete(m.nativePending, n)
		if !m.scheduleLive[n] || m.prs[n].Head.Sha != headSHA {
			continue
		}
		if m.beforeNative != nil {
			m.beforeNative(n)
		}
		if m.prs[n].Merged {
			m.scheduleLive[n] = false
			continue
		}
		if n == m.failNativeMerge {
			m.scheduleLive[n] = false
			continue
		}
		m.scheduleLive[n] = false
		m.merged = append(m.merged, n)
		m.prs[n].State = "closed"
		m.prs[n].Merged = true
	}
}

func (m *mock) BuildStaging(_ context.Context, _, stagingBranch string, refs []gitops.MergedRef) (string, int, error) {
	var nums []int
	for _, r := range refs {
		nums = append(nums, r.PR)
	}
	m.staged = append(m.staged, append([]int(nil), nums...))
	m.stagingBranches = append(m.stagingBranches, stagingBranch)
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
		if m, ok := e.fc.(*mock); ok {
			m.advanceNative()
		}
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

func TestStagingBranchesAreUniquePerAttempt(t *testing.T) {
	m := newMock(-1, 1)
	m.runStatus = "running"
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging"}, m, m)
	e.now = func() time.Time { return time.Unix(100, 0) }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	m.prs[1].Head.Sha = "head-1-updated"
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("restack updated head: %v", err)
	}

	if got := len(m.stagingBranches); got != 2 {
		t.Fatalf("staging branches = %d, want 2", got)
	}
	if m.stagingBranches[0] == m.stagingBranches[1] {
		t.Fatalf("staging branch reused: %q", m.stagingBranches[0])
	}
	for _, branch := range m.stagingBranches {
		if !strings.HasPrefix(branch, "mq/main/staging-") {
			t.Fatalf("staging branch = %q, want shunt staging prefix", branch)
		}
	}
}

func TestQueueStatusCommentsAreStickyAndConcise(t *testing.T) {
	m := newMock(-1, 1, 2)
	m.runStatus = "running"
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", QueueComments: true, BotUser: "mq-bot"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}

	if got := len(m.queueComments[1]); got != 2 {
		t.Fatalf("PR 1 queue comment updates = %d, want 2", got)
	}
	if body := m.queueComments[1][0]; !strings.Contains(body, "State: queued; acknowledged by shunt") {
		t.Fatalf("initial queue acknowledgement missing queued state:\n%s", body)
	}
	body := m.queueComments[1][1]
	for _, want := range []string{
		queueCommentMarker,
		"Repository: `o/r`",
		"Base: `main`",
		"Position: 1/2",
		"State: testing in active batch",
		"Active batch: #1, #2 on `mq/main/staging-",
		"separate durable outcome comment",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("queue comment missing %q in:\n%s", want, body)
		}
	}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("unchanged running batch: %v", err)
	}
	if got := len(m.queueComments[1]); got != 2 {
		t.Fatalf("unchanged queue comment updates = %d, want 2", got)
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
		t.Fatalf("release PR: %v", err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("observe merge: %v", err)
	}

	if got := len(m.queueComments[1]); got != 4 {
		t.Fatalf("PR 1 queue comment updates = %d, want 4", got)
	}
	if body := m.queueComments[1][3]; !strings.Contains(body, "State: Landed via merge queue") || !strings.Contains(body, "Outcome: terminal") {
		t.Fatalf("final queue comment did not mark PR as landed:\n%s", body)
	}
}

func TestQueueStatusCommentsMarkBisectionAsRetrying(t *testing.T) {
	m := newMock(2, 1, 2)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", QueueComments: true}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("split failed batch: %v", err)
	}

	body := m.queueComments[1][len(m.queueComments[1])-1]
	if !strings.Contains(body, "State: retrying after gate failure; isolating the batch") {
		t.Fatalf("retry queue comment missing state:\n%s", body)
	}
}

func TestQueueStatusCommentsMarkTerminalOutcomes(t *testing.T) {
	m := newMock(1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", QueueComments: true}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start failing batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("bounce failing batch: %v", err)
	}

	sticky := m.queueComments[1][len(m.queueComments[1])-1]
	if !strings.Contains(sticky, "Outcome: terminal") {
		t.Fatalf("terminal sticky comment missing outcome:\n%s", sticky)
	}
	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, outcomeCommentMarker) {
		t.Fatalf("durable outcome comment missing marker:\n%s", got)
	}
}

func TestQueueStatusCommentsMarkCancelledAutomergeRequeued(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", QueueComments: true}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	before := len(m.queueComments[1])
	m.runStatus = "success"
	m.automerge[1] = false
	m.scheduleLive[1] = false
	m.automergeAt[1] = m.nextEventTime()

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("requeue cancelled auto-merge: %v", err)
	}

	if got := len(m.queueComments[1]); got != before+1 {
		t.Fatalf("queue comment updates = %d, want %d", got, before+1)
	}
	body := m.queueComments[1][len(m.queueComments[1])-1]
	if !strings.Contains(body, "State: requeued after auto-merge was cancelled") {
		t.Fatalf("requeue comment missing state:\n%s", body)
	}
	if strings.Contains(body, "Outcome: terminal") {
		t.Fatalf("requeue comment incorrectly marked terminal:\n%s", body)
	}
}

func TestBatchLingerWaitsWhileUnderTargetAndWindow(t *testing.T) {
	m := newMock(-1, 1, 2)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", BatchLinger: 10 * time.Second, BatchTarget: 3, QueueComments: true}, m, m)
	now := time.Unix(100, 0)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start linger: %v", err)
	}
	if got := len(m.queueComments[1]); got != 1 {
		t.Fatalf("initial linger comment updates = %d, want 1", got)
	}
	if body := m.queueComments[1][0]; !strings.Contains(body, "State: queued; acknowledged by shunt; waiting for batch linger window") {
		t.Fatalf("initial linger comment missing state:\n%s", body)
	}
	now = now.Add(9 * time.Second)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("continue linger: %v", err)
	}
	if got := len(m.queueComments[1]); got != 1 {
		t.Fatalf("unchanged linger comment updates = %d, want 1", got)
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
		t.Fatalf("release first PR: %v", err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release second PR: %v", err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("finish first batch: %v", err)
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
	drive(restarted, 4)
	if got := fmt.Sprint(m.merged); got != "[1 2]" {
		t.Errorf("merged after restore = %s, want [1 2]", got)
	}
	if !store.deleted {
		t.Error("empty queue should delete checkpoint after restored batch lands")
	}
}

func TestCheckpointRestartRestagesRemainderAfterReleasedPRMerges(t *testing.T) {
	m := newMock(-1, 1, 2)
	store := &memoryCheckpointStore{}
	cfg := Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging", Checkpoint: store}
	e := New(cfg, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release first PR: %v", err)
	}
	m.advanceNative()

	restarted := New(cfg, m, m)
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatalf("restore queue: %v", err)
	}

	if got := fmt.Sprint(m.staged); got != "[[1 2] [2]]" {
		t.Fatalf("staged = %s, want remaining PR restaged on the advanced base", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1:pending head-1:success]" {
		t.Fatalf("statuses = %s, want second PR blocked until its fresh gate passes", got)
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

func TestNativeLandingReleasesOnePRAtATime(t *testing.T) {
	m := newMock(-1, 1, 2)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release first PR: %v", err)
	}

	if got := fmt.Sprint(m.statuses); got != "[head-1:pending head-1:success]" {
		t.Fatalf("statuses before first merge = %s, want only PR 1 released", got)
	}
	if got := fmt.Sprint(m.calls); got != "[status:pending status:success]" {
		t.Fatalf("landing calls = %s, want status-only native release", got)
	}
	if got := fmt.Sprint(m.merged); got != "[]" {
		t.Fatalf("merged before native worker = %s, want []", got)
	}

	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("observe first merge and release second: %v", err)
	}

	if got := fmt.Sprint(m.merged); got != "[1]" {
		t.Fatalf("merged = %s, want [1]", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1:pending head-1:success head-2:pending head-2:success]" {
		t.Fatalf("statuses after first merge = %s, want PR 2 released only after PR 1 merged", got)
	}
}

func TestNativeMergeRequeuesSpeculativeBatchesFromOldBase(t *testing.T) {
	m := newMock(-1, 1, 2, 3)
	e := New(Config{
		Owner:         "o",
		Repo:          "r",
		Base:          "main",
		StatusCtx:     "merge-queue",
		StagingBranch: "mq/main/staging",
		BisectFanout:  2,
	}, m, m)
	first := &activeBatch{
		prs:        []forge.PullRequest{*m.prs[1], *m.prs[2]},
		stagingSHA: "stage-first",
		outcome:    "success",
	}
	later := &activeBatch{
		prs:        []forge.PullRequest{*m.prs[3]},
		stagingSHA: "stage-later",
		outcome:    "success",
	}
	e.active = []*activeBatch{first, later}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release first PR: %v", err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("observe merge: %v", err)
	}

	if got := e.baseGen; got != 1 {
		t.Fatalf("base generation = %d, want 1", got)
	}
	if got := fmt.Sprint(numbersOf(first.prs)); got != "[2]" {
		t.Fatalf("first active batch = %s, want remaining PR 2", got)
	}
	if got := fmt.Sprint(e.pending); got != "[[3]]" {
		t.Fatalf("pending = %s, want later speculative batch requeued", got)
	}
	if got := len(e.active); got != 1 || e.active[0] != first {
		t.Fatalf("active batches = %#v, want only current landing batch", e.active)
	}
}

func TestNativeMergeAdvancesBaseBeforeLaterLandingError(t *testing.T) {
	m := newMock(-1, 1, 2, 3)
	e := New(Config{
		Owner:         "o",
		Repo:          "r",
		Base:          "main",
		StatusCtx:     "merge-queue",
		StagingBranch: "mq/main/staging",
		BisectFanout:  2,
	}, m, m)
	first := &activeBatch{
		prs:        []forge.PullRequest{*m.prs[1], *m.prs[2]},
		stagingSHA: "stage-first",
		outcome:    "success",
	}
	later := &activeBatch{
		prs:        []forge.PullRequest{*m.prs[3]},
		stagingSHA: "stage-later",
		outcome:    "success",
	}
	e.active = []*activeBatch{first, later}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release first PR: %v", err)
	}
	m.advanceNative()
	m.getPRErrors[2] = 1
	if err := e.Reconcile(context.Background()); err == nil {
		t.Fatal("expected later PR lookup failure")
	}

	if got := e.baseGen; got != 1 {
		t.Fatalf("base generation = %d, want merge recorded before returning the later error", got)
	}
	if got := fmt.Sprint(numbersOf(first.prs)); got != "[2]" {
		t.Fatalf("first active batch = %s, want unresolved PR 2 retained", got)
	}
	if first.baseGen != e.baseGen {
		t.Fatalf("first batch base generation = %d, want %d", first.baseGen, e.baseGen)
	}
	if got := fmt.Sprint(e.pending); got != "[[3]]" {
		t.Fatalf("pending = %s, want stale speculative batch requeued", got)
	}
}

func TestForgeCompletedMergeDoesNotOverwriteSuccessStatus(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release PR: %v", err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("observe merge: %v", err)
	}

	if got := fmt.Sprint(m.statuses); got != "[head-1:pending head-1:success]" {
		t.Errorf("statuses = %s, want pending then success", got)
	}
	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, "Landed via merge queue") {
		t.Errorf("comments missing landed outcome:\n%s", got)
	}
}

func TestNativeMergeTimeoutBlocksRestoresAndRequeues(t *testing.T) {
	m := newMock(-1, 1)
	m.failNativeMerge = 1
	now := time.Now()
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", MergeStyle: "rebase", StagingBranch: "mq/main/staging"}, m, m)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release PR: %v", err)
	}
	m.advanceNative()
	now = now.Add(nativeMergeTimeout + time.Second)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("recover timed-out native merge: %v", err)
	}

	if got := fmt.Sprint(m.merged); got != "[]" {
		t.Fatalf("merged = %s, want []", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1:pending head-1:success head-1:error head-1:pending]" {
		t.Fatalf("statuses = %s, want success blocked before restoration", got)
	}
	if got := fmt.Sprint(m.calls); got != "[status:pending status:success status:error schedule:1 status:pending]" {
		t.Fatalf("recovery calls = %s, want error, schedule, pending", got)
	}
	if got := fmt.Sprint(m.scheduled); got != "[1]" {
		t.Fatalf("restored schedules = %s, want [1]", got)
	}
	if got := fmt.Sprint(e.pending); got != "[[1]]" {
		t.Fatalf("pending = %s, want timed-out PR requeued", got)
	}
	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, "Merge did not complete") {
		t.Fatalf("outcome comment = %q, want timeout outcome", got)
	}
}

func TestNativeMergeTimeoutRequeuesBeforeRestoreStatusFailure(t *testing.T) {
	m := newMock(-1, 1)
	m.failNativeMerge = 1
	now := time.Now()
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release PR: %v", err)
	}
	m.advanceNative()
	now = now.Add(nativeMergeTimeout + time.Second)
	m.failStatusState = "pending"
	m.failStatusCount = 1
	if err := e.Reconcile(context.Background()); err == nil {
		t.Fatal("expected restored pending status failure")
	}

	if got := fmt.Sprint(e.pending); got != "[[1]]" {
		t.Fatalf("pending = %s, want timed-out PR queued before restore completes", got)
	}
	if len(e.active) != 0 {
		t.Fatalf("active batches = %d, want old passing batch discarded", len(e.active))
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1:pending head-1:success head-1:error]" {
		t.Fatalf("statuses = %s, want no second success from the old batch", got)
	}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("restage after partial recovery: %v", err)
	}
	if got := fmt.Sprint(m.staged); got != "[[1] [1]]" {
		t.Fatalf("staged = %s, want a fresh run after partial recovery", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1:pending head-1:success head-1:error]" {
		t.Fatalf("statuses after restage = %s, want no release before the fresh gate resolves", got)
	}
}

func TestNativeMergeTimeoutDoesNotResurrectCancellation(t *testing.T) {
	m := newMock(-1, 1)
	now := time.Now()
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release PR: %v", err)
	}
	m.scheduleLive[1] = false
	m.automerge[1] = false
	m.automergeAt[1] = m.nextEventTime()
	now = now.Add(nativeMergeTimeout + time.Second)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("observe cancellation: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("drop cancelled requeue: %v", err)
	}

	if got := fmt.Sprint(m.scheduled); got != "[]" {
		t.Fatalf("restored schedules = %s, want cancellation preserved", got)
	}
	if got := fmt.Sprint(m.staged); got != "[[1]]" {
		t.Fatalf("staged = %s, want no fresh run after cancellation", got)
	}
}

func TestNativeMergeTimeoutRechecksCancellationBeforeRestore(t *testing.T) {
	m := newMock(-1, 1)
	m.failNativeMerge = 1
	now := time.Now()
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release PR: %v", err)
	}
	m.advanceNative()
	now = now.Add(nativeMergeTimeout + time.Second)
	m.beforeAutomergeState = func(n int) {
		status := m.latestStatus[n]
		if status.Status != "error" || status.Description != statusDescription("Merge did not complete") {
			return
		}
		m.beforeAutomergeState = nil
		m.scheduleLive[n] = false
		m.automerge[n] = false
		m.automergeAt[n] = m.nextEventTime()
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("observe cancellation during recovery: %v", err)
	}

	if got := fmt.Sprint(m.scheduled); got != "[]" {
		t.Fatalf("restored schedules = %s, want newer cancellation preserved", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1:pending head-1:success head-1:error]" {
		t.Fatalf("statuses = %s, want timed-out success blocked without a duplicate status", got)
	}
	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, "Skipped by merge queue") {
		t.Fatalf("outcome comment = %q, want cancellation outcome", got)
	}
}

func TestCancellationBetweenClaimAndReleaseWins(t *testing.T) {
	m := newMock(-1, 1)
	m.beforeGetPR = func(n int) {
		status, ok := m.latestStatus[n]
		if !ok || status.Status != "pending" || status.Description != landingClaimDescription {
			return
		}
		m.scheduleLive[n] = false
		m.automerge[n] = false
		m.automergeAt[n] = m.nextEventTime()
	}
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("observe cancellation: %v", err)
	}

	if got := fmt.Sprint(m.statuses); got != "[head-1:pending head-1:error]" {
		t.Fatalf("statuses = %s, want cancellation to prevent success", got)
	}
	if got := fmt.Sprint(m.scheduled); got != "[]" {
		t.Fatalf("restored schedules = %s, want cancellation preserved", got)
	}
}

func TestStaleLandingSuccessFromOlderVersionIsRecovered(t *testing.T) {
	m := newMock(-1, 1)
	m.scheduleLive[1] = false
	releasedAt := time.Now().Add(-nativeMergeTimeout - time.Second)
	m.latestStatus[1] = forge.CommitStatus{
		ID:          200,
		Status:      "success",
		Description: landingSuccessDescription,
		Context:     "merge-queue",
		CreatedAt:   releasedAt,
	}
	now := time.Now()
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", MergeStyle: "squash", StagingBranch: "mq/main/staging"}, m, m)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("restage stale landing: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("recover stale landing: %v", err)
	}

	if got := fmt.Sprint(m.scheduled); got != "[1]" {
		t.Fatalf("restored schedules = %s, want [1]", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[head-1:error head-1:pending]" {
		t.Fatalf("statuses = %s, want stale success blocked before requeue", got)
	}
}

func TestNewerTerminalStatusSuppressesOrphanedScheduleEvent(t *testing.T) {
	m := newMock(-1, 1)
	m.latestStatus[1] = forge.CommitStatus{
		ID:          200,
		Status:      "error",
		Description: "merge queue: PR rejected",
		Context:     "merge-queue",
		CreatedAt:   m.automergeAt[1].Add(time.Second),
	}
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := fmt.Sprint(m.staged); got != "[]" {
		t.Fatalf("staged = %s, want orphaned schedule suppressed", got)
	}
}

func TestNewerScheduleRestoresQueueAfterTerminalStatus(t *testing.T) {
	m := newMock(-1, 1)
	statusTime := m.automergeAt[1].Add(time.Second)
	m.latestStatus[1] = forge.CommitStatus{
		ID:          200,
		Status:      "error",
		Description: "merge queue: PR rejected",
		Context:     "merge-queue",
		CreatedAt:   statusTime,
	}
	m.automergeAt[1] = statusTime.Add(time.Second)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := fmt.Sprint(m.staged); got != "[[1]]" {
		t.Fatalf("staged = %s, want newer schedule honored", got)
	}
}

func TestCancellationAfterLandingClaimIsNotRecovered(t *testing.T) {
	m := newMock(-1, 1)
	m.automerge[1] = false
	m.scheduleLive[1] = false
	m.latestStatus[1] = forge.CommitStatus{
		ID:          200,
		Status:      "pending",
		Description: landingClaimDescription,
		Context:     "merge-queue",
		CreatedAt:   m.automergeAt[1].Add(time.Second),
	}
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := fmt.Sprint(m.staged); got != "[]" {
		t.Fatalf("staged = %s, want cancellation preserved", got)
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
		t.Fatalf("release PR: %v", err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("observe merge: %v", err)
	}

	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, "https://forge.example.com/o/r/actions/runs/7") {
		t.Fatalf("terminal comment did not use run target URL:\n%s", got)
	}
	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, outcomeCommentMarker) {
		t.Fatalf("terminal comment missing outcome marker:\n%s", got)
	}
}

func TestActiveBatchRestacksChangedHeadImmediately(t *testing.T) {
	m := newMock(-1, 1)
	m.runStatus = "running"
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	m.prs[1].Head.Sha = "head-1-new"
	m.runStatus = "failure"
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("restack changed batch: %v", err)
	}

	if got := fmt.Sprint(m.merged); got != "[]" {
		t.Errorf("merged = %s, want []", got)
	}
	if got := fmt.Sprint(m.statuses); got != "[]" {
		t.Errorf("statuses = %s, want none before retest", got)
	}
	if got := strings.Join(m.comments[1], "\n"); got != "" {
		t.Errorf("comments = %s, want none before retest", got)
	}
	if m.bounced[1] {
		t.Fatal("stale failing batch must not bounce a PR whose head changed")
	}
	if got := fmt.Sprint(m.staged); got != "[[1] [1]]" {
		t.Fatalf("staged = %s, want immediate restack", got)
	}
	if len(e.active) != 1 {
		t.Fatalf("active batches = %d, want 1", len(e.active))
	}
	if got := e.active[0].prs[0].Head.Sha; got != "head-1-new" {
		t.Fatalf("restacked head = %s, want head-1-new", got)
	}
}

func TestTerminalFailureRestacksIfHeadChangesWhileReadingStatus(t *testing.T) {
	m := newMock(-1, 1)
	m.runStatus = "failure"
	m.beforeRunStatus = func(string) {
		m.prs[1].Head.Sha = "head-1-new"
	}
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("restack changed batch: %v", err)
	}

	if m.bounced[1] {
		t.Fatal("stale failure must not bounce a PR whose head changed while status was read")
	}
	if got := fmt.Sprint(m.statuses); got != "[]" {
		t.Errorf("statuses = %s, want none before retest", got)
	}
	if got := fmt.Sprint(m.staged); got != "[[1] [1]]" {
		t.Fatalf("staged = %s, want immediate restack", got)
	}
	if got := e.active[0].prs[0].Head.Sha; got != "head-1-new" {
		t.Fatalf("restacked head = %s, want head-1-new", got)
	}
}

func TestTerminalFailureRestacksIfHeadChangesWhileReadingRunURL(t *testing.T) {
	m := newMock(-1, 1)
	m.runStatus = "failure"
	m.beforeRunURL = func(string) {
		m.prs[1].Head.Sha = "head-1-new"
	}
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("restack changed batch: %v", err)
	}

	if m.bounced[1] {
		t.Fatal("stale failure must not bounce a PR whose head changed while run URL was read")
	}
	if got := fmt.Sprint(m.statuses); got != "[]" {
		t.Errorf("statuses = %s, want none before retest", got)
	}
	if got := fmt.Sprint(m.staged); got != "[[1] [1]]" {
		t.Fatalf("staged = %s, want immediate restack", got)
	}
	if got := e.active[0].prs[0].Head.Sha; got != "head-1-new" {
		t.Fatalf("restacked head = %s, want head-1-new", got)
	}
}

func TestTerminalFailureRestacksIfHeadChangesDuringBounce(t *testing.T) {
	m := newMock(-1, 1)
	m.runStatus = "failure"
	gets := 0
	m.beforeGetPR = func(int) {
		gets++
		if gets == 4 {
			m.prs[1].Head.Sha = "head-1-new"
		}
	}
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("restack changed batch: %v", err)
	}

	if m.bounced[1] {
		t.Fatal("stale failure must not bounce a PR whose head changed during bounce")
	}
	if got := fmt.Sprint(m.statuses); got != "[]" {
		t.Errorf("statuses = %s, want none before retest", got)
	}
	if got := fmt.Sprint(m.staged); got != "[[1] [1]]" {
		t.Fatalf("staged = %s, want immediate restack", got)
	}
	if got := e.active[0].prs[0].Head.Sha; got != "head-1-new" {
		t.Fatalf("restacked head = %s, want head-1-new", got)
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

func TestNativeLandingHandlesHeadChangeAfterRelease(t *testing.T) {
	m := newMock(-1, 1)
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release PR: %v", err)
	}
	m.prs[1].Head.Sha = "head-1-new"
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("requeue changed PR: %v", err)
	}

	if got := fmt.Sprint(m.statuses); got != "[head-1:pending head-1:success head-1:error head-1-new:error]" {
		t.Errorf("statuses = %s, want success cleared on tested and current heads", got)
	}
	if got := fmt.Sprint(m.merged); got != "[]" {
		t.Errorf("merged = %s, want []", got)
	}
	if got := strings.Join(m.comments[1], "\n"); !strings.Contains(got, "Skipped by merge queue") || !strings.Contains(got, "head changed from head-1 to head-1-new") {
		t.Errorf("comments = %s, want changed-head skip comment", got)
	}
	if got := fmt.Sprint(m.scheduled); got != "[]" {
		t.Errorf("restored schedules = %s, want existing schedule preserved", got)
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
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.advanceNative()
	if got := fmt.Sprint(m.merged); got != "[1]" {
		t.Fatalf("merged after prefix = %s, want [1]", got)
	}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
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
	m.advanceNative()
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
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.advanceNative()
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
	if got := fmt.Sprint(e.pending); got != "[[2]]" {
		t.Fatalf("pending after changed prefix = %s, want suffix waiting behind active prefix", got)
	}
	if len(e.active) != 1 {
		t.Fatalf("active batches = %d, want changed prefix restaged", len(e.active))
	}
	if got := fmt.Sprint(numbersOf(e.active[0].prs)); got != "[1]" {
		t.Fatalf("active changed prefix = %s, want [1]", got)
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
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.advanceNative()
	sort.Ints(m.merged)
	if got := fmt.Sprint(m.merged); got != "[2 3]" {
		t.Errorf("merged = %s, want [2 3]", got)
	}
	if m.bounced[2] || m.bounced[3] {
		t.Fatalf("rest of suffix should not bounce, bounced = %v", m.bounced)
	}
}

func TestStagingConflictOnFirstPRRestacksIfHeadChangesDuringBounce(t *testing.T) {
	m := newMock(-1, 1, 2)
	m.conflictPR = 1
	m.conflictFirst = true
	gets := 0
	m.beforeGetPR = func(n int) {
		if n != 1 {
			return
		}
		gets++
		if gets == 2 {
			m.prs[1].Head.Sha = "head-1-new"
		}
	}
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging"}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	if m.bounced[1] {
		t.Fatal("stale first-PR staging conflict must not bounce a PR whose head changed")
	}
	if got := fmt.Sprint(m.statuses); got != "[]" {
		t.Errorf("statuses = %s, want none before retest", got)
	}
	if got := fmt.Sprint(e.pending); got != "[[1] [2]]" {
		t.Fatalf("pending = %s, want changed PR before suffix", got)
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
		t.Fatalf("release first PR: %v", err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release second PR: %v", err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("finish landing: %v", err)
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

func TestMetricsTrackQueueAgeAndTimeInQueue(t *testing.T) {
	m := newMock(-1, 1, 2)
	c := metrics.New()
	e := New(Config{Owner: "o", Repo: "r", Base: "main", StatusCtx: "merge-queue", StagingBranch: "mq/main/staging", Metrics: c}, m, m)
	now := time.Unix(100, 0)
	e.now = func() time.Time { return now }

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("start reconcile: %v", err)
	}
	now = now.Add(30 * time.Second)
	e.observeQueue()
	assertMetric(t, c, `shunt_queue_oldest_age_seconds{owner="o",repo="r",base="main"} 30`)

	now = now.Add(15 * time.Second)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release first PR: %v", err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("release second PR: %v", err)
	}
	m.advanceNative()
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("finish landing: %v", err)
	}
	assertMetric(t, c, `shunt_queue_oldest_age_seconds{owner="o",repo="r",base="main"} 0`)
	assertMetric(t, c, `shunt_time_in_queue_seconds_bucket{owner="o",repo="r",base="main",outcome="merged",le="60"} 2`)
	assertMetric(t, c, `shunt_time_in_queue_seconds_bucket{owner="o",repo="r",base="main",outcome="merged",le="+Inf"} 2`)
	assertMetric(t, c, `shunt_time_in_queue_seconds_sum{owner="o",repo="r",base="main",outcome="merged"} 90`)
	assertMetric(t, c, `shunt_time_in_queue_seconds_count{owner="o",repo="r",base="main",outcome="merged"} 2`)
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
	assertMetric(t, c, `shunt_time_in_queue_seconds_count{owner="o",repo="r",base="main",outcome="bounced"} 1`)
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

func TestLeaseContentionSkipsQueueActions(t *testing.T) {
	m := newMock(-1, 1)
	store := &memoryCheckpointStore{}
	lease := &testQueueLease{held: []bool{false}}
	c := metrics.New()
	e := New(Config{
		Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging",
		Checkpoint: store, Lease: lease, LeaseHolderID: "holder", LeaseTTL: time.Minute, Metrics: c,
	}, m, m)

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if lease.calls != 1 {
		t.Fatalf("lease calls = %d, want 1", lease.calls)
	}
	if m.listCalls != 0 || len(m.staged) != 0 {
		t.Fatalf("forge actions = list %d, staged %v; want none", m.listCalls, m.staged)
	}
	if store.loads != 0 || store.saves != 0 || store.deletes != 0 {
		t.Fatalf("checkpoint actions = loads %d saves %d deletes %d; want none", store.loads, store.saves, store.deletes)
	}
	var out strings.Builder
	c.WritePrometheus(&out)
	if strings.Contains(out.String(), `shunt_reconcile_errors_total{owner="o",repo="r",base="main"} 1`) {
		t.Fatalf("contention recorded a reconcile error:\n%s", out.String())
	}
}

func TestLeaseReacquisitionResetsVolatileStateAndReloadsCheckpoint(t *testing.T) {
	m := newMock(-1, 1, 2)
	store := &memoryCheckpointStore{saved: &checkpoint.QueueSnapshot{
		Key:     checkpoint.QueueKey{Owner: "o", Repo: "r", Base: "main"},
		Pending: [][]int{{1}},
	}}
	lease := &testQueueLease{held: []bool{false, true}}
	e := New(Config{
		Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", QueueComments: true,
		Checkpoint: store, Lease: lease, LeaseHolderID: "holder", LeaseTTL: time.Minute,
	}, m, m)
	e.pending = [][]int{{2}}
	e.queueComments[2] = "stale"
	e.terminalQueueComments[2] = "stale"

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("contended Reconcile: %v", err)
	}
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("reacquired Reconcile: %v", err)
	}
	if store.loads != 1 {
		t.Fatalf("checkpoint loads = %d, want 1 after reacquisition", store.loads)
	}
	if got := fmt.Sprint(m.staged); got != "[[1]]" {
		t.Fatalf("staged = %s, want only durable candidate [1]", got)
	}
	if got := len(m.queueComments[2]); got != 0 {
		t.Fatalf("stale queue comment updates = %d, want 0", got)
	}
}

func TestLeasePreventsDuplicateEffectsUntilTakeover(t *testing.T) {
	m := newMock(-1, 1)
	store := &memoryCheckpointStore{}
	lease := &holderQueueLease{holder: "first"}
	first := New(Config{
		Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging",
		Checkpoint: store, Lease: lease, LeaseHolderID: "first", LeaseTTL: time.Minute,
	}, m, m)
	second := New(Config{
		Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging",
		Checkpoint: store, Lease: lease, LeaseHolderID: "second", LeaseTTL: time.Minute,
	}, m, m)

	if err := first.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if err := second.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if got := fmt.Sprint(m.staged); got != "[[1]]" {
		t.Fatalf("staged = %s, want one staging action while first holder owns lease", got)
	}

	lease.holder = ""
	if err := second.Reconcile(context.Background()); err != nil {
		t.Fatalf("takeover Reconcile: %v", err)
	}
	if got := fmt.Sprint(m.staged); got != "[[1] [1]]" {
		t.Fatalf("staged = %s, want durable active batch re-staged after takeover", got)
	}
}

func TestUnavailableForgeSkipsReconcileError(t *testing.T) {
	m := newMock(-1, 1)
	m.runStatusErr = forge.ErrUnavailable
	c := metrics.New()
	e := New(Config{
		Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging", Metrics: c,
	}, m, m)
	e.active = []*activeBatch{{prs: []forge.PullRequest{*m.prs[1]}, stagingBranch: "mq/main/staging-1", stagingSHA: "stage-1"}}

	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile returned unavailable error: %v", err)
	}
	if !errors.Is(m.runStatusErr, forge.ErrUnavailable) {
		t.Fatal("test setup did not use forge unavailability")
	}
	status := c.StatusSnapshot()
	if len(status.Queues) != 1 || status.Queues[0].QueueDepth != 1 || !status.Queues[0].ActiveBatch {
		t.Fatalf("queue status after unavailable forge = %#v, want active queue", status)
	}
	var out strings.Builder
	c.WritePrometheus(&out)
	if strings.Contains(out.String(), `shunt_reconcile_errors_total{owner="o",repo="r",base="main"} 1`) {
		t.Fatalf("unavailable forge recorded a reconcile error:\n%s", out.String())
	}
}

func TestDurableLeaseBoundsReconcileContext(t *testing.T) {
	m := newMock(-1, 1)
	lease := &deadlineQueueLease{}
	stager := &contextBlockingStager{}
	store := &memoryCheckpointStore{}
	const ttl = 40 * time.Millisecond
	e := New(Config{
		Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging",
		Checkpoint: store, Lease: lease, LeaseHolderID: "holder", LeaseTTL: ttl,
	}, m, stager)

	err := e.Reconcile(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reconcile error = %v, want context deadline exceeded", err)
	}
	if !lease.hasDeadline || !stager.hasDeadline {
		t.Fatal("lease and stager must receive a deadline")
	}
	if got := lease.deadline.Sub(lease.acquiredAt); got <= 0 || got > ttl/2 {
		t.Fatalf("lease context budget = %s, want (0, %s]", got, ttl/2)
	}
	if !lease.deadline.Equal(stager.deadline) {
		t.Fatalf("lease deadline = %s, stager deadline = %s; want propagated deadline", lease.deadline, stager.deadline)
	}
	if store.saves != 0 {
		t.Fatalf("checkpoint saves = %d, want none after deadline", store.saves)
	}
}

func TestDeadlinePreservesEarlierReconcileError(t *testing.T) {
	m := newMock(-1)
	lease := &deadlineQueueLease{}
	store := &deadlineSaveCheckpointStore{}
	const ttl = 40 * time.Millisecond
	e := New(Config{
		Owner: "o", Repo: "r", Base: "main", StagingBranch: "mq/main/staging",
		Checkpoint: store, Lease: lease, LeaseHolderID: "holder", LeaseTTL: ttl,
	}, m, m)
	if err := e.Reconcile(context.Background()); err != nil {
		t.Fatalf("initial Reconcile: %v", err)
	}

	earlier := errors.New("run status failed")
	m.addPR(1)
	m.runStatusErr = earlier
	e.active = []*activeBatch{{prs: []forge.PullRequest{*m.prs[1]}, stagingBranch: "mq/main/staging-1", stagingSHA: "stage-1"}}
	err := e.Reconcile(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reconcile error = %v, want context deadline exceeded", err)
	}
	if !strings.Contains(err.Error(), earlier.Error()) {
		t.Fatalf("Reconcile error = %v, want earlier error %q preserved", err, earlier)
	}
}

type testQueueLease struct {
	held  []bool
	calls int
}

func (l *testQueueLease) AcquireLease(_ context.Context, _ checkpoint.QueueKey, _ string, _ time.Duration) (bool, error) {
	held := l.held[l.calls]
	l.calls++
	return held, nil
}

type holderQueueLease struct {
	holder string
}

func (l *holderQueueLease) AcquireLease(_ context.Context, _ checkpoint.QueueKey, holderID string, _ time.Duration) (bool, error) {
	if l.holder == "" {
		l.holder = holderID
	}
	return l.holder == holderID, nil
}

type deadlineQueueLease struct {
	acquiredAt  time.Time
	deadline    time.Time
	hasDeadline bool
}

func (l *deadlineQueueLease) AcquireLease(ctx context.Context, _ checkpoint.QueueKey, _ string, _ time.Duration) (bool, error) {
	l.acquiredAt = time.Now()
	l.deadline, l.hasDeadline = ctx.Deadline()
	return true, nil
}

type contextBlockingStager struct {
	deadline    time.Time
	hasDeadline bool
}

func (s *contextBlockingStager) BuildStaging(ctx context.Context, _ string, _ string, _ []gitops.MergedRef) (string, int, error) {
	s.deadline, s.hasDeadline = ctx.Deadline()
	<-ctx.Done()
	return "", 0, ctx.Err()
}

type deadlineSaveCheckpointStore struct{}

func (*deadlineSaveCheckpointStore) LoadQueue(context.Context, checkpoint.QueueKey) (checkpoint.QueueSnapshot, bool, error) {
	return checkpoint.QueueSnapshot{}, false, nil
}

func (*deadlineSaveCheckpointStore) SaveQueue(ctx context.Context, _ checkpoint.QueueSnapshot) error {
	<-ctx.Done()
	return fmt.Errorf("write checkpoint: %w", ctx.Err())
}

func (*deadlineSaveCheckpointStore) DeleteQueue(context.Context, checkpoint.QueueKey) error {
	return nil
}

type memoryCheckpointStore struct {
	saved   *checkpoint.QueueSnapshot
	deleted bool
	loads   int
	saves   int
	deletes int
}

func (s *memoryCheckpointStore) LoadQueue(_ context.Context, _ checkpoint.QueueKey) (checkpoint.QueueSnapshot, bool, error) {
	s.loads++
	if s.saved == nil {
		return checkpoint.QueueSnapshot{}, false, nil
	}
	return s.saved.Clone(), true, nil
}

func (s *memoryCheckpointStore) SaveQueue(_ context.Context, snapshot checkpoint.QueueSnapshot) error {
	s.saves++
	clone := snapshot.Clone()
	s.saved = &clone
	s.deleted = false
	return nil
}

func (s *memoryCheckpointStore) DeleteQueue(_ context.Context, _ checkpoint.QueueKey) error {
	s.deletes++
	s.saved = nil
	s.deleted = true
	return nil
}
