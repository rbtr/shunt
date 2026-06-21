// Package engine is the merge-queue reconcile loop: rollup batching with
// bisection. It keeps a work queue of candidate batches (lists of PR numbers).
// Each cycle it tests one candidate on a fresh staging branch; on success the
// whole batch lands, on failure a multi-PR batch is split in half (bisection)
// to isolate the culprit while letting the good PRs through.
package engine

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
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
}

type activeBatch struct {
	prs           []forge.PullRequest
	stagingBranch string
	stagingSHA    string
}

// ForgeAPI is the subset of the forge client the engine needs (interface so the
// reconcile/bisection logic is unit-testable with a mock).
type ForgeAPI interface {
	ListOpenPRs(owner, repo, base string) ([]forge.PullRequest, error)
	GetPR(owner, repo string, index int) (forge.PullRequest, error)
	AutomergeScheduled(owner, repo string, index int) (bool, error)
	RunStatus(owner, repo, sha, branch string) (string, error)
	SetCommitStatus(owner, repo, sha, context, state, desc, targetURL string) error
	MergePR(owner, repo string, index int, style string) error
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
}

func New(cfg Config, fc ForgeAPI, st Stager) *Engine {
	return &Engine{cfg: cfg, fc: fc, st: st}
}

// Reconcile advances the queue by one step. Safe to call on a fixed interval.
func (e *Engine) Reconcile() error {
	if e.active != nil {
		return e.checkActive()
	}
	return e.startNext()
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
			return err
		}
		e.pending = [][]int{ready}
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
			e.bounce(conflictPR, "merge conflict while staging the batch")
			rest := removeNum(numbersOf(prs), conflictPR)
			if len(rest) > 0 {
				e.pending = append([][]int{rest}, e.pending...)
			}
			return nil
		}
		return err
	}
	e.active = &activeBatch{prs: prs, stagingBranch: e.cfg.StagingBranch, stagingSHA: sha}
	log.Printf("queue: testing batch %v on staging sha=%s", numbersOf(prs), short(sha))
	return nil
}

func (e *Engine) checkActive() error {
	a := e.active
	status, err := e.fc.RunStatus(e.cfg.Owner, e.cfg.Repo, a.stagingSHA, a.stagingBranch)
	if err != nil {
		return err
	}
	switch status {
	case "success":
		return e.land()
	case "failure", "cancelled", "error":
		return e.bisectOrBounce(status)
	default: // "", running, waiting, blocked -> keep waiting
		return nil
	}
}

// land merges every PR in the passing batch via Forgejo (status-gated), in
// order. Sequential merges reproduce the tested staging tree.
func (e *Engine) land() error {
	a := e.active
	for _, pr := range a.prs {
		if err := e.fc.SetCommitStatus(e.cfg.Owner, e.cfg.Repo, pr.Head.Sha, e.cfg.StatusCtx, "success", "merge queue: batch passed", e.commitURL(a.stagingSHA)); err != nil {
			return err
		}
		// Forgejo can return a transient 405 ("Please try again later") while it
		// is still finishing the previous merge; retry briefly before giving up
		// and re-queuing the remainder.
		var mErr error
		for attempt := 0; attempt < 5; attempt++ {
			if mErr = e.fc.MergePR(e.cfg.Owner, e.cfg.Repo, pr.Number, e.cfg.MergeStyle); mErr == nil {
				break
			}
			time.Sleep(2 * time.Second)
		}
		if mErr != nil {
			if err := e.fc.SetCommitStatus(e.cfg.Owner, e.cfg.Repo, pr.Head.Sha, e.cfg.StatusCtx, "error", "merge queue: merge did not complete; re-queued", e.commitURL(a.stagingSHA)); err != nil {
				return fmt.Errorf("merge #%d failed: %v; also failed to reset status: %w", pr.Number, mErr, err)
			}
			log.Printf("queue: merge #%d failed: %v (remaining PRs re-queued next cycle)", pr.Number, mErr)
			break
		}
		_ = e.fc.Comment(e.cfg.Owner, e.cfg.Repo, pr.Number, "Landed via merge queue.")
		log.Printf("queue: merged #%d", pr.Number)
	}
	_ = e.fc.DeleteBranch(e.cfg.Owner, e.cfg.Repo, a.stagingBranch)
	e.active = nil
	return nil
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
	_ = e.fc.CancelAutomerge(e.cfg.Owner, e.cfg.Repo, num)
	_ = e.fc.Comment(e.cfg.Owner, e.cfg.Repo, num, "Bounced from the merge queue: "+reason)
	log.Printf("queue: bounced #%d: %s", num, reason)
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

func short(s string) string {
	if len(s) > 10 {
		return s[:10]
	}
	return s
}
