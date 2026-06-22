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
	Metrics       *metrics.Collector
}

type activeBatch struct {
	prs           []forge.PullRequest
	stagingBranch string
	stagingSHA    string
	gateOutcome   string
}

// ForgeAPI is the subset of the forge client the engine needs (interface so the
// reconcile/bisection logic is unit-testable with a mock).
type ForgeAPI interface {
	ListOpenPRs(owner, repo, base string) ([]forge.PullRequest, error)
	GetPR(owner, repo string, index int) (forge.PullRequest, error)
	AutomergeScheduled(owner, repo string, index int) (bool, error)
	RunStatus(owner, repo, sha, branch string) (string, error)
	SetCommitStatus(owner, repo, sha, context, state, desc, targetURL string) error
	MergePR(owner, repo string, index int, style, headSHA string) error
	CancelAutomerge(owner, repo string, index int) error
	Comment(owner, repo string, index int, body string) error
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
	active  *activeBatch
	now     func() time.Time

	lingerSince time.Time
}

func New(cfg Config, fc ForgeAPI, st Stager) *Engine {
	return &Engine{cfg: cfg, fc: fc, st: st, now: time.Now}
}

// Reconcile advances the queue by one step. Safe to call on a fixed interval.
func (e *Engine) Reconcile() error {
	var err error
	if e.active != nil {
		err = e.checkActive()
	} else {
		err = e.startNext()
	}
	if err != nil {
		e.cfg.Metrics.IncReconcileError(e.metricLabels())
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

func (e *Engine) startNext() error {
	if len(e.pending) == 0 {
		ready, err := e.readyNumbers()
		if err != nil || len(ready) == 0 {
			e.lingerSince = time.Time{}
			return err
		}
		if e.linger(ready) {
			return nil
		}
		e.pending = [][]int{ready}
		e.lingerSince = time.Time{}
	}
	cand := e.pending[0]
	e.pending = e.pending[1:]

	prs, err := e.resolve(cand)
	if err != nil {
		return err
	}
	if len(prs) == 0 {
		return nil
	}

	refs := make([]gitops.MergedRef, len(prs))
	for i, p := range prs {
		refs[i] = gitops.MergedRef{PR: p.Number, Ref: fmt.Sprintf("refs/pull/%d/head", p.Number)}
	}
	sha, conflictPR, err := e.st.BuildStaging(e.cfg.Base, e.cfg.StagingBranch, refs)
	if err != nil {
		if conflictPR > 0 {
			e.cfg.Metrics.IncStagingConflict(e.metricLabels())
			return e.handleStagingConflict(numbersOf(prs), conflictPR)
		}
		return err
	}
	e.active = &activeBatch{prs: prs, stagingBranch: e.cfg.StagingBranch, stagingSHA: sha}
	e.cfg.Metrics.IncBatchesStarted(e.metricLabels())
	log.Printf("queue: testing batch %v on staging sha=%s", numbersOf(prs), short(sha))
	return nil
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

func (e *Engine) checkActive() error {
	a := e.active
	status, err := e.fc.RunStatus(e.cfg.Owner, e.cfg.Repo, a.stagingSHA, a.stagingBranch)
	if err != nil {
		return err
	}
	switch status {
	case "success":
		e.recordGateOutcome(status)
		return e.land()
	case "failure", "cancelled", "error":
		e.recordGateOutcome(status)
		return e.bisectOrBounce(status)
	default: // "", running, waiting, blocked -> keep waiting
		return nil
	}
}

// land merges every PR in the passing batch via Forgejo (status-gated), in
// order. Sequential merges reproduce the tested staging tree.
func (e *Engine) land() error {
	a := e.active
	requeueFrom := -1
	for i, pr := range a.prs {
		ok, reason, current, err := e.readyToLand(pr)
		if err != nil {
			return err
		}
		if !ok {
			e.skipLand(pr.Number, current, reason, i < len(a.prs)-1)
			requeueFrom = i
			break
		}
		if err := e.fc.SetCommitStatus(e.cfg.Owner, e.cfg.Repo, pr.Head.Sha, e.cfg.StatusCtx, "success", "merge queue: batch passed", e.commitURL(a.stagingSHA)); err != nil {
			return err
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
				return fmt.Errorf("merge #%d failed: %v; also failed to revalidate after merge error: %w", pr.Number, mErr, err)
			}
			if !ok {
				if err := e.fc.SetCommitStatus(e.cfg.Owner, e.cfg.Repo, pr.Head.Sha, e.cfg.StatusCtx, "error", "merge queue: PR changed before merge; re-queued", e.commitURL(a.stagingSHA)); err != nil {
					return fmt.Errorf("merge #%d failed after PR changed: %v; also failed to reset status: %w", pr.Number, mErr, err)
				}
				e.skipLand(pr.Number, current, reason, i < len(a.prs)-1)
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
			if err := e.fc.SetCommitStatus(e.cfg.Owner, e.cfg.Repo, pr.Head.Sha, e.cfg.StatusCtx, "error", "merge queue: merge did not complete; re-queued", e.commitURL(a.stagingSHA)); err != nil {
				return fmt.Errorf("merge #%d failed: %v; also failed to reset status: %w", pr.Number, mErr, err)
			}
			log.Printf("queue: merge #%d failed: %v (remaining PRs re-queued next cycle)", pr.Number, mErr)
			requeueFrom = i
			break
		}
		e.cfg.Metrics.IncPRMerge(e.metricLabels())
		_ = e.fc.Comment(e.cfg.Owner, e.cfg.Repo, pr.Number, "Landed via merge queue.")
		log.Printf("queue: merged #%d", pr.Number)
	}
	if requeueFrom >= 0 {
		e.requeueActiveRemainder(a.prs[requeueFrom:])
	}
	_ = e.fc.DeleteBranch(e.cfg.Owner, e.cfg.Repo, a.stagingBranch)
	e.active = nil
	return nil
}

func (e *Engine) requeueActiveRemainder(prs []forge.PullRequest) {
	nums := numbersOf(prs)
	if len(nums) > 0 {
		e.pending = append([][]int{nums}, e.pending...)
	}
}

func (e *Engine) handleStagingConflict(nums []int, conflictPR int) error {
	idx := indexOfNum(nums, conflictPR)
	if idx < 0 {
		return fmt.Errorf("stager reported conflict on PR #%d outside candidate %v", conflictPR, nums)
	}
	if idx == 0 {
		e.bounce(conflictPR, "merge conflict while staging the PR")
		if len(nums) > 1 {
			rest := append([]int(nil), nums[1:]...)
			e.pending = append([][]int{rest}, e.pending...)
			log.Printf("queue: batch %v conflicts on first PR #%d -> re-queued %v", nums, conflictPR, rest)
		}
		return nil
	}

	prefix := append([]int(nil), nums[:idx]...)
	suffix := append([]int(nil), nums[idx:]...)
	e.pending = append([][]int{prefix, suffix}, e.pending...)
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

func (e *Engine) skipLand(num int, pr forge.PullRequest, reason string, hasRemainder bool) {
	suffix := ""
	if hasRemainder {
		suffix = " (remaining PRs re-queued next cycle)"
	}
	log.Printf("queue: skipped #%d before merge: %s%s", num, reason, suffix)
	if pr.State == "open" && !pr.Merged {
		_ = e.fc.Comment(e.cfg.Owner, e.cfg.Repo, num, "Skipped by the merge queue: "+reason+".")
	}
}

// bisectOrBounce: a size-1 failing batch bounces the culprit; a larger batch is
// split in half, with the first half tested next (the good half lands, the
// recursion isolates the bad PR(s)).
func (e *Engine) bisectOrBounce(status string) error {
	a := e.active
	_ = e.fc.DeleteBranch(e.cfg.Owner, e.cfg.Repo, a.stagingBranch)
	nums := numbersOf(a.prs)
	e.active = nil

	if len(nums) == 1 {
		e.bounce(nums[0], fmt.Sprintf("merge-queue gate **%s**; see the [staging run](%s)", status, e.commitURL(a.stagingSHA)))
		return nil
	}
	mid := len(nums) / 2
	first := append([]int(nil), nums[:mid]...)
	second := append([]int(nil), nums[mid:]...)
	e.pending = append([][]int{first, second}, e.pending...)
	log.Printf("queue: batch %v failed (%s) -> bisecting into %v then %v", nums, status, first, second)
	return nil
}

func (e *Engine) bounce(num int, reason string) {
	e.cfg.Metrics.IncBounce(e.metricLabels())
	_ = e.fc.CancelAutomerge(e.cfg.Owner, e.cfg.Repo, num)
	_ = e.fc.Comment(e.cfg.Owner, e.cfg.Repo, num, "Bounced from the merge queue: "+reason)
	log.Printf("queue: bounced #%d: %s", num, reason)
}

func (e *Engine) recordGateOutcome(status string) {
	if e.active != nil && e.active.gateOutcome == "" {
		e.active.gateOutcome = status
		e.cfg.Metrics.IncGateOutcome(e.metricLabels(), status)
	}
}

func (e *Engine) observeQueue() {
	depth := 0
	for _, cand := range e.pending {
		depth += len(cand)
	}
	active := e.active != nil
	if active {
		depth += len(e.active.prs)
	}
	e.cfg.Metrics.ObserveQueue(e.metricLabels(), depth, active)
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
