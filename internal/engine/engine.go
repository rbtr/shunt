// Package engine is the merge-queue reconcile loop: rollup batching with
// bisection. It keeps a work queue of candidate batches (lists of PR numbers).
// Each cycle it tests one candidate on a fresh staging branch; on success the
// whole batch lands, on failure a multi-PR batch is split in half (bisection)
// to isolate the culprit while letting the good PRs through. Staging conflicts
// split at the conflict point so earlier PRs keep their place in the queue.
package engine

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
	"github.com/rbtr/shunt/internal/metrics"
)

type Config struct {
	Owner         string
	Repo          string
	Base          string
	StatusCtx     string // required commit-status context, e.g. "merge-queue"
	MergeStyle    string // merge|rebase|squash
	StagingBranch string // e.g. "mq/main/staging"
	InstanceURL   string // used for API/git (may be an in-cluster URL)
	PublicURL     string // used for user-facing links (defaults to InstanceURL)
	MaxBatch      int    // cap the initial rollup size (0 = unlimited)
	BatchLinger   time.Duration
	BatchTarget   int
	BisectFanout  int // max concurrent bisection staging runs (0 = 1)
	QueueComments bool
	BotUser       string
	Metrics       *metrics.Collector
}

type activeBatch struct {
	prs           []forge.PullRequest
	stagingBranch string
	stagingSHA    string
	debugURL      string
	baseGen       int
	outcome       string
}

// ForgeAPI is the subset of the forge client the engine needs (interface so the
// reconcile/bisection logic is unit-testable with a mock).
type ForgeAPI interface {
	ListOpenPRs(owner, repo, base string) ([]forge.PullRequest, error)
	GetPR(owner, repo string, index int) (forge.PullRequest, error)
	AutomergeScheduled(owner, repo string, index int) (bool, error)
	RunStatus(owner, repo, sha, branch string) (string, error)
	RunTargetURL(owner, repo, sha, branch string) (string, error)
	SetCommitStatus(owner, repo, sha, context, state, desc, targetURL string) error
	MergePR(owner, repo string, index int, style, headSHA string) error
	CancelAutomerge(owner, repo string, index int) error
	Comment(owner, repo string, index int, body string) error
	UpsertComment(owner, repo string, index int, marker, botUser, body string) error
	DeleteBranch(owner, repo, branch string) error
}

// Stager builds an integration ("staging") branch from a base + PR head refs.
type Stager interface {
	BuildStaging(base, stagingBranch string, refs []gitops.MergedRef) (sha string, conflictPR int, err error)
}

type Engine struct {
	cfg     Config
	fc      ForgeAPI
	st      Stager
	pending [][]int // work queue of candidate batches (PR numbers, in order)
	active  []*activeBatch
	now     func() time.Time

	lingerSince time.Time
	baseGen     int
	stagingSeq  int

	queueComments         map[int]string
	terminalQueueComments map[int]string
}

func New(cfg Config, fc ForgeAPI, st Stager) *Engine {
	return &Engine{cfg: cfg, fc: fc, st: st, now: time.Now, queueComments: map[int]string{}, terminalQueueComments: map[int]string{}}
}

// Reconcile advances the queue by one step. Safe to call on a fixed interval.
func (e *Engine) Reconcile() error {
	resolved, err := e.checkActive()
	if err == nil && !resolved {
		e.freeSlotForEarlierPending()
		for len(e.active) < e.activeLimit() {
			var started bool
			started, err = e.startNext()
			if err != nil || !started {
				break
			}
		}
	}
	if err != nil {
		e.cfg.Metrics.IncReconcileError(e.metricLabels())
	}
	if commentErr := e.syncQueueComments(); commentErr != nil {
		log.Printf("queue: status comment sync failed: %v", commentErr)
	}
	e.observeQueue()
	return err
}

// readyNumbers lists open PRs targeting base that currently have auto-merge
// scheduled, ordered FIFO, capped to MaxBatch.
func (e *Engine) readyNumbers() ([]int, error) {
	prs, err := e.fc.ListOpenPRs(e.cfg.Owner, e.cfg.Repo, e.cfg.Base)
	if err != nil {
		return nil, err
	}
	var nums []int
	for _, p := range prs {
		ok, err := e.fc.AutomergeScheduled(e.cfg.Owner, e.cfg.Repo, p.Number)
		if err != nil {
			return nil, err
		}
		if ok {
			nums = append(nums, p.Number)
		}
	}
	sort.Ints(nums)
	if e.cfg.MaxBatch > 0 && len(nums) > e.cfg.MaxBatch {
		nums = nums[:e.cfg.MaxBatch]
	}
	return nums, nil
}

// resolve drops PRs from a candidate that are no longer open or no longer have
// auto-merge scheduled (e.g. merged in a prior sub-batch, or bounced).
func (e *Engine) resolve(nums []int) ([]forge.PullRequest, error) {
	var out []forge.PullRequest
	for _, n := range nums {
		pr, err := e.fc.GetPR(e.cfg.Owner, e.cfg.Repo, n)
		if err != nil {
			return nil, err
		}
		if pr.State != "open" || pr.Merged {
			continue
		}
		ok, err := e.fc.AutomergeScheduled(e.cfg.Owner, e.cfg.Repo, n)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, pr)
		}
	}
	return out, nil
}

func (e *Engine) startNext() (bool, error) {
	if len(e.pending) == 0 {
		if len(e.active) > 0 {
			return false, nil
		}
		ready, err := e.readyNumbers()
		if err != nil || len(ready) == 0 {
			e.lingerSince = time.Time{}
			return false, err
		}
		if e.linger(ready) {
			return false, nil
		}
		e.enqueue(ready)
		e.lingerSince = time.Time{}
	}
	cand := e.pending[0]
	e.pending = e.pending[1:]

	prs, err := e.resolve(cand)
	if err != nil {
		return false, err
	}
	if len(prs) == 0 {
		return false, nil
	}

	refs := make([]gitops.MergedRef, len(prs))
	for i, p := range prs {
		refs[i] = gitops.MergedRef{PR: p.Number, Ref: fmt.Sprintf("refs/pull/%d/head", p.Number)}
	}
	stagingBranch := e.stagingBranch()
	sha, conflictPR, err := e.st.BuildStaging(e.cfg.Base, stagingBranch, refs)
	if err != nil {
		if conflictPR > 0 {
			e.cfg.Metrics.IncStagingConflict(e.metricLabels())
			return false, e.handleStagingConflict(numbersOf(prs), conflictPR)
		}
		return false, err
	}
	a := &activeBatch{prs: prs, stagingBranch: stagingBranch, stagingSHA: sha, baseGen: e.baseGen}
	e.active = append(e.active, a)
	e.cfg.Metrics.IncBatchesStarted(e.metricLabels())
	log.Printf("queue: testing batch %v on %s sha=%s", numbersOf(prs), a.stagingBranch, short(sha))
	return true, nil
}

func (e *Engine) linger(ready []int) bool {
	if e.cfg.BatchLinger <= 0 {
		return false
	}
	if e.cfg.BatchTarget > 0 && len(ready) >= e.cfg.BatchTarget {
		return false
	}
	now := e.now()
	if e.lingerSince.IsZero() {
		e.lingerSince = now
		log.Printf("queue: lingering up to %s for batch target %d; currently ready=%v", e.cfg.BatchLinger, e.cfg.BatchTarget, ready)
		return true
	}
	return now.Sub(e.lingerSince) < e.cfg.BatchLinger
}

func (e *Engine) checkActive() (bool, error) {
	for _, a := range e.active {
		if a.outcome == "" {
			status, err := e.fc.RunStatus(e.cfg.Owner, e.cfg.Repo, a.stagingSHA, a.stagingBranch)
			if err != nil {
				return false, err
			}
			switch status {
			case "success", "failure", "cancelled", "error":
				a.outcome = status
				a.debugURL = e.debugURL(a)
				e.cfg.Metrics.IncGateOutcome(e.metricLabels(), status)
			default: // "", running, waiting, blocked -> keep waiting
				continue
			}
		}
		if !e.readyToResolve(a) {
			continue
		}
		if a.baseGen != e.baseGen {
			e.requeueStaleActive(a)
			return true, nil
		}
		switch a.outcome {
		case "success":
			baseChanged, err := e.land(a)
			if err != nil {
				return false, err
			}
			if baseChanged {
				e.baseGen++
				e.requeueStaleActives()
			}
		case "failure", "cancelled", "error":
			if err := e.bisectOrBounce(a, a.outcome); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	return false, nil
}

// land merges every PR in the passing batch via Forgejo (status-gated), in
// order. Sequential merges reproduce the tested staging tree.
func (e *Engine) land(a *activeBatch) (bool, error) {
	requeueFrom := -1
	merged := 0
	for i, pr := range a.prs {
		ok, reason, current, err := e.readyToLand(pr)
		if err != nil {
			return false, err
		}
		if !ok {
			e.skipLand(pr, current, reason, i < len(a.prs)-1, a.debugURL)
			requeueFrom = i
			break
		}
		if err := e.fc.SetCommitStatus(e.cfg.Owner, e.cfg.Repo, pr.Head.Sha, e.cfg.StatusCtx, "success", "merge queue: batch passed", e.commitURL(a.stagingSHA)); err != nil {
			return false, err
		}
		// Forgejo can return a transient 405 ("Please try again later") while it
		// is still finishing the previous merge; retry briefly before giving up
		// and re-queuing the remainder.
		var mErr error
		drifted := false
		for attempt := 0; attempt < 5; attempt++ {
			if mErr = e.fc.MergePR(e.cfg.Owner, e.cfg.Repo, pr.Number, e.cfg.MergeStyle, pr.Head.Sha); mErr == nil {
				break
			}
			ok, reason, current, err := e.readyToLand(pr)
			if err != nil {
				return false, fmt.Errorf("merge #%d failed: %v; also failed to revalidate after merge error: %w", pr.Number, mErr, err)
			}
			if !ok {
				if err := e.fc.SetCommitStatus(e.cfg.Owner, e.cfg.Repo, pr.Head.Sha, e.cfg.StatusCtx, "error", "merge queue: PR changed before merge; re-queued", a.debugURL); err != nil {
					return false, fmt.Errorf("merge #%d failed after PR changed: %v; also failed to reset status: %w", pr.Number, mErr, err)
				}
				e.skipLand(pr, current, reason, i < len(a.prs)-1, a.debugURL)
				drifted = true
				requeueFrom = i
				break
			}
			time.Sleep(2 * time.Second)
		}
		if drifted {
			break
		}
		if mErr != nil {
			if err := e.fc.SetCommitStatus(e.cfg.Owner, e.cfg.Repo, pr.Head.Sha, e.cfg.StatusCtx, "error", "merge queue: merge did not complete; re-queued", a.debugURL); err != nil {
				return false, fmt.Errorf("merge #%d failed: %v; also failed to reset status: %w", pr.Number, mErr, err)
			}
			e.notifyPR(pr.Number, pr.Head.Sha, "error", "Merge did not complete", "shunt re-queued this PR after the forge merge API did not complete.", a.debugURL, true)
			log.Printf("queue: merge #%d failed: %v (remaining PRs re-queued next cycle)", pr.Number, mErr)
			requeueFrom = i
			break
		}
		e.cfg.Metrics.IncPRMerge(e.metricLabels())
		e.notifyPR(pr.Number, pr.Head.Sha, "", "Landed via merge queue", "shunt tested this PR in a staging batch and merged it after the gate passed.", a.debugURL, true)
		log.Printf("queue: merged #%d", pr.Number)
		merged++
	}
	if requeueFrom >= 0 {
		e.requeueActiveRemainder(a.prs[requeueFrom:])
	}
	_ = e.fc.DeleteBranch(e.cfg.Owner, e.cfg.Repo, a.stagingBranch)
	e.removeActive(a)
	return merged > 0, nil
}

func (e *Engine) requeueActiveRemainder(prs []forge.PullRequest) {
	nums := numbersOf(prs)
	if len(nums) > 0 {
		e.enqueue(nums)
	}
}

func (e *Engine) handleStagingConflict(nums []int, conflictPR int) error {
	idx := indexOfNum(nums, conflictPR)
	if idx < 0 {
		return fmt.Errorf("stager reported conflict on PR #%d outside candidate %v", conflictPR, nums)
	}
	if idx == 0 {
		e.bounce(conflictPR, "merge conflict while staging the PR", "error", "")
		if len(nums) > 1 {
			rest := append([]int(nil), nums[1:]...)
			e.enqueue(rest)
			log.Printf("queue: batch %v conflicts on first PR #%d -> re-queued %v", nums, conflictPR, rest)
		}
		return nil
	}

	prefix := append([]int(nil), nums[:idx]...)
	suffix := append([]int(nil), nums[idx:]...)
	e.enqueue(prefix, suffix)
	log.Printf("queue: batch %v conflicts on #%d -> testing prefix %v before suffix %v", nums, conflictPR, prefix, suffix)
	return nil
}

func (e *Engine) readyToLand(staged forge.PullRequest) (bool, string, forge.PullRequest, error) {
	current, err := e.fc.GetPR(e.cfg.Owner, e.cfg.Repo, staged.Number)
	if err != nil {
		return false, "", current, err
	}
	if current.Merged {
		return false, "already merged", current, nil
	}
	if current.State != "open" {
		return false, fmt.Sprintf("state changed to %q", current.State), current, nil
	}
	ok, err := e.fc.AutomergeScheduled(e.cfg.Owner, e.cfg.Repo, staged.Number)
	if err != nil {
		return false, "", current, err
	}
	if !ok {
		return false, "auto-merge is no longer scheduled", current, nil
	}
	if current.Head.Sha != staged.Head.Sha {
		return false, fmt.Sprintf("head changed from %s to %s", short(staged.Head.Sha), short(current.Head.Sha)), current, nil
	}
	return true, "", current, nil
}

func (e *Engine) skipLand(staged, current forge.PullRequest, reason string, hasRemainder bool, debugURL string) {
	suffix := ""
	if hasRemainder {
		suffix = " (remaining PRs re-queued next cycle)"
	}
	num := staged.Number
	log.Printf("queue: skipped #%d before merge: %s%s", num, reason, suffix)
	if current.State == "open" && !current.Merged {
		sha := current.Head.Sha
		if sha == "" {
			sha = staged.Head.Sha
		}
		e.notifyPR(num, sha, "error", "Skipped by merge queue", "shunt skipped this PR before landing because "+reason+". It will be re-tested if it remains queued.", debugURL, true)
	}
}

// bisectOrBounce: a size-1 failing batch bounces the culprit; a larger batch is
// split in half, with the first half tested next (the good half lands, the
// recursion isolates the bad PR(s)).
func (e *Engine) bisectOrBounce(a *activeBatch, status string) error {
	_ = e.fc.DeleteBranch(e.cfg.Owner, e.cfg.Repo, a.stagingBranch)
	nums := numbersOf(a.prs)
	e.removeActive(a)

	if len(nums) == 1 {
		e.bounce(nums[0], fmt.Sprintf("merge-queue gate **%s**", status), gateOutcomeStatus(status), a.debugURL)
		return nil
	}
	mid := len(nums) / 2
	first := append([]int(nil), nums[:mid]...)
	second := append([]int(nil), nums[mid:]...)
	e.enqueue(first, second)
	log.Printf("queue: batch %v failed (%s) -> bisecting into %v then %v", nums, status, first, second)
	return nil
}

func gateOutcomeStatus(status string) string {
	if status == "failure" {
		return "failure"
	}
	return "error"
}

func (e *Engine) bounce(num int, reason, statusState, debugURL string) {
	e.cfg.Metrics.IncBounce(e.metricLabels())
	if pr, err := e.fc.GetPR(e.cfg.Owner, e.cfg.Repo, num); err == nil && pr.State == "open" && !pr.Merged {
		e.notifyPR(num, pr.Head.Sha, statusState, "Bounced from merge queue", "shunt rejected this PR from the merge queue: "+reason+".", debugURL, true)
	}
	_ = e.fc.CancelAutomerge(e.cfg.Owner, e.cfg.Repo, num)
	log.Printf("queue: bounced #%d: %s", num, reason)
}

func (e *Engine) activeLimit() int {
	if e.cfg.BisectFanout > 0 {
		return e.cfg.BisectFanout
	}
	return 1
}

func (e *Engine) stagingBranch() string {
	if e.activeLimit() <= 1 {
		return e.cfg.StagingBranch
	}
	e.stagingSeq++
	return fmt.Sprintf("%s-%d", e.cfg.StagingBranch, e.stagingSeq)
}

func (e *Engine) enqueue(cands ...[]int) {
	for _, cand := range cands {
		if len(cand) == 0 {
			continue
		}
		e.pending = append(e.pending, append([]int(nil), cand...))
	}
	sort.SliceStable(e.pending, func(i, j int) bool {
		return e.pending[i][0] < e.pending[j][0]
	})
}

func (e *Engine) readyToResolve(a *activeBatch) bool {
	first := firstPR(a.prs)
	for _, cand := range e.pending {
		if len(cand) > 0 && cand[0] < first {
			return false
		}
	}
	for _, other := range e.active {
		if other != a && firstPR(other.prs) < first {
			return false
		}
	}
	return true
}

func (e *Engine) freeSlotForEarlierPending() {
	if len(e.pending) == 0 || len(e.active) < e.activeLimit() {
		return
	}
	earliestPending := e.pending[0][0]
	idx := -1
	latest := -1
	for i, a := range e.active {
		if first := firstPR(a.prs); first > earliestPending && first > latest {
			idx = i
			latest = first
		}
	}
	if idx < 0 {
		return
	}
	a := e.active[idx]
	_ = e.fc.DeleteBranch(e.cfg.Owner, e.cfg.Repo, a.stagingBranch)
	e.active = append(e.active[:idx], e.active[idx+1:]...)
	e.enqueue(numbersOf(a.prs))
	log.Printf("queue: re-queued speculative batch %v to test earlier candidate %v", numbersOf(a.prs), e.pending[0])
}

func (e *Engine) requeueStaleActive(a *activeBatch) {
	_ = e.fc.DeleteBranch(e.cfg.Owner, e.cfg.Repo, a.stagingBranch)
	e.removeActive(a)
	e.enqueue(numbersOf(a.prs))
	log.Printf("queue: re-queued stale speculative batch %v after base advanced", numbersOf(a.prs))
}

func (e *Engine) requeueStaleActives() {
	for _, a := range append([]*activeBatch(nil), e.active...) {
		if a.baseGen != e.baseGen {
			e.requeueStaleActive(a)
		}
	}
}

func (e *Engine) removeActive(a *activeBatch) {
	for i, candidate := range e.active {
		if candidate == a {
			e.active = append(e.active[:i], e.active[i+1:]...)
			return
		}
	}
}

func firstPR(prs []forge.PullRequest) int {
	if len(prs) == 0 {
		return 0
	}
	return prs[0].Number
}

func (e *Engine) observeQueue() {
	depth := 0
	for _, cand := range e.pending {
		depth += len(cand)
	}
	for _, a := range e.active {
		depth += len(a.prs)
	}
	e.cfg.Metrics.ObserveQueue(e.metricLabels(), depth, len(e.active) > 0)
}

const queueCommentMarker = "<!-- shunt:queue-status -->"

type queueCommentStatus struct {
	number        int
	position      int
	total         int
	state         string
	activeSummary string
}

func (e *Engine) syncQueueComments() error {
	if !e.cfg.QueueComments {
		return nil
	}
	statuses, err := e.queueCommentStatuses()
	if err != nil {
		return err
	}
	want := make(map[int]string, len(statuses))
	for _, status := range statuses {
		want[status.number] = e.queueCommentBody(status)
	}

	var firstErr error
	for num, body := range want {
		if e.queueComments[num] == body {
			continue
		}
		if err := e.fc.UpsertComment(e.cfg.Owner, e.cfg.Repo, num, queueCommentMarker, e.cfg.BotUser, body); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("PR #%d: %w", num, err)
			}
			continue
		}
		e.queueComments[num] = body
	}
	for num := range e.queueComments {
		if _, ok := want[num]; ok {
			continue
		}
		body := e.queueCommentNotQueuedBody()
		if terminal, ok := e.terminalQueueComments[num]; ok {
			body = terminal
		}
		if e.queueComments[num] != body {
			if err := e.fc.UpsertComment(e.cfg.Owner, e.cfg.Repo, num, queueCommentMarker, e.cfg.BotUser, body); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("PR #%d: %w", num, err)
				}
				continue
			}
		}
		if _, terminal := e.terminalQueueComments[num]; !terminal {
			delete(e.queueComments, num)
		}
	}
	return firstErr
}

func (e *Engine) queueCommentStatuses() ([]queueCommentStatus, error) {
	states := map[int]string{}
	for _, cand := range e.pending {
		for _, num := range cand {
			states[num] = "queued"
		}
	}
	activeSummary := e.activeSummary()
	for _, a := range e.active {
		state := "testing in active batch"
		if a.outcome != "" {
			state = "gate " + a.outcome + "; resolving"
			if !e.readyToResolve(a) {
				state = "gate " + a.outcome + "; waiting for earlier queue entry"
			}
		}
		for _, pr := range a.prs {
			states[pr.Number] = state
		}
	}
	if len(states) == 0 {
		ready, err := e.readyNumbers()
		if err != nil {
			return nil, err
		}
		for _, num := range ready {
			state := "queued"
			if e.cfg.BatchLinger > 0 && !e.lingerSince.IsZero() {
				state = "queued; waiting for batch linger window"
			}
			states[num] = state
		}
	}
	nums := make([]int, 0, len(states))
	for num := range states {
		nums = append(nums, num)
	}
	sort.Ints(nums)
	out := make([]queueCommentStatus, 0, len(nums))
	for i, num := range nums {
		out = append(out, queueCommentStatus{
			number:        num,
			position:      i + 1,
			total:         len(nums),
			state:         states[num],
			activeSummary: activeSummary,
		})
	}
	return out, nil
}

func (e *Engine) queueCommentBody(status queueCommentStatus) string {
	var b strings.Builder
	fmt.Fprintln(&b, queueCommentMarker)
	fmt.Fprintln(&b, "**Merge queue status**")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "- Repository: `%s/%s`\n", e.cfg.Owner, e.cfg.Repo)
	fmt.Fprintf(&b, "- Base: `%s`\n", e.cfg.Base)
	fmt.Fprintf(&b, "- Position: %d/%d\n", status.position, status.total)
	fmt.Fprintf(&b, "- State: %s\n", status.state)
	if status.activeSummary != "" {
		fmt.Fprintf(&b, "- Active batch: %s\n", status.activeSummary)
	} else {
		fmt.Fprintln(&b, "- Active batch: none")
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "_shunt updates this sticky comment instead of posting new queue-status comments._")
	return strings.TrimRight(b.String(), "\n")
}

func (e *Engine) queueCommentNotQueuedBody() string {
	var b strings.Builder
	fmt.Fprintln(&b, queueCommentMarker)
	fmt.Fprintln(&b, "**Merge queue status**")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "- Repository: `%s/%s`\n", e.cfg.Owner, e.cfg.Repo)
	fmt.Fprintf(&b, "- Base: `%s`\n", e.cfg.Base)
	fmt.Fprintln(&b, "- State: not currently queued")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "_shunt updates this sticky comment instead of posting new queue-status comments._")
	return strings.TrimRight(b.String(), "\n")
}

func (e *Engine) queueCommentTerminalBody(title, detail, debugURL string) string {
	var b strings.Builder
	fmt.Fprintln(&b, queueCommentMarker)
	fmt.Fprintln(&b, "**Merge queue status**")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "- Repository: `%s/%s`\n", e.cfg.Owner, e.cfg.Repo)
	fmt.Fprintf(&b, "- Base: `%s`\n", e.cfg.Base)
	fmt.Fprintf(&b, "- State: %s\n", title)
	if detail != "" {
		fmt.Fprintf(&b, "- Detail: %s\n", detail)
	}
	if debugURL != "" {
		fmt.Fprintf(&b, "- Debug: [staging run/commit](%s)\n", debugURL)
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "_shunt updates this sticky comment instead of posting new queue-status comments._")
	return strings.TrimRight(b.String(), "\n")
}

func (e *Engine) notifyPR(num int, sha, statusState, title, detail, debugURL string, durableComment bool) {
	if statusState != "" && sha != "" {
		if err := e.fc.SetCommitStatus(e.cfg.Owner, e.cfg.Repo, sha, e.cfg.StatusCtx, statusState, statusDescription(title), debugURL); err != nil {
			log.Printf("queue: notify #%d: set status: %v", num, err)
		}
	}
	body := terminalCommentBody(title, detail, debugURL)
	if e.cfg.QueueComments {
		sticky := e.queueCommentTerminalBody(title, detail, debugURL)
		if err := e.fc.UpsertComment(e.cfg.Owner, e.cfg.Repo, num, queueCommentMarker, e.cfg.BotUser, sticky); err != nil {
			log.Printf("queue: notify #%d: update sticky comment: %v", num, err)
		} else {
			e.queueComments[num] = sticky
			e.terminalQueueComments[num] = sticky
		}
	}
	if durableComment {
		if err := e.fc.Comment(e.cfg.Owner, e.cfg.Repo, num, body); err != nil {
			log.Printf("queue: notify #%d: comment: %v", num, err)
		}
	}
}

func terminalCommentBody(title, detail, debugURL string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**%s**\n", title)
	if detail != "" {
		fmt.Fprintf(&b, "\n%s\n", detail)
	}
	if debugURL != "" {
		fmt.Fprintf(&b, "\nDebug: [staging run/commit](%s)\n", debugURL)
	}
	return strings.TrimRight(b.String(), "\n")
}

func statusDescription(title string) string {
	switch title {
	case "Bounced from merge queue":
		return "merge queue: PR rejected"
	case "Skipped by merge queue":
		return "merge queue: PR skipped; re-queued if still eligible"
	case "Merge did not complete":
		return "merge queue: merge did not complete; re-queued"
	default:
		return "merge queue: " + strings.ToLower(title)
	}
}

func (e *Engine) activeSummary() string {
	if len(e.active) == 0 {
		return ""
	}
	parts := make([]string, 0, len(e.active))
	for _, a := range e.active {
		state := "running"
		if a.outcome != "" {
			state = a.outcome
		}
		parts = append(parts, fmt.Sprintf("%s on `%s` (`%s`, %s)", formatPRNums(numbersOf(a.prs)), a.stagingBranch, short(a.stagingSHA), state))
	}
	return strings.Join(parts, "; ")
}

func (e *Engine) metricLabels() metrics.Labels {
	return metrics.Labels{Owner: e.cfg.Owner, Repo: e.cfg.Repo, Base: e.cfg.Base}
}

func (e *Engine) commitURL(sha string) string {
	base := e.cfg.PublicURL
	if base == "" {
		base = e.cfg.InstanceURL
	}
	return fmt.Sprintf("%s/%s/%s/commit/%s", base, e.cfg.Owner, e.cfg.Repo, sha)
}

func (e *Engine) debugURL(a *activeBatch) string {
	targetURL, err := e.fc.RunTargetURL(e.cfg.Owner, e.cfg.Repo, a.stagingSHA, a.stagingBranch)
	if err != nil {
		log.Printf("queue: run target lookup for %s: %v", short(a.stagingSHA), err)
	}
	if targetURL != "" {
		return targetURL
	}
	return e.commitURL(a.stagingSHA)
}

func numbersOf(prs []forge.PullRequest) []int {
	out := make([]int, len(prs))
	for i, p := range prs {
		out[i] = p.Number
	}
	return out
}

func removeNum(nums []int, n int) []int {
	var out []int
	for _, x := range nums {
		if x != n {
			out = append(out, x)
		}
	}
	return out
}

func formatPRNums(nums []int) string {
	parts := make([]string, len(nums))
	for i, n := range nums {
		parts[i] = fmt.Sprintf("#%d", n)
	}
	return strings.Join(parts, ", ")
}

func indexOfNum(nums []int, n int) int {
	for i, x := range nums {
		if x == n {
			return i
		}
	}
	return -1
}

func short(s string) string {
	if len(s) > 10 {
		return s[:10]
	}
	return s
}
