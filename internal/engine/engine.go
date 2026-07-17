// Package engine is the merge-queue reconcile loop: rollup batching with
// bisection. It keeps a work queue of candidate batches (lists of PR numbers).
// Each cycle it tests one candidate on a fresh staging branch; on success the
// whole batch lands, on failure a multi-PR batch is split in half (bisection)
// to isolate the culprit while letting the good PRs through. Staging conflicts
// split at the conflict point so earlier PRs keep their place in the queue.
package engine

import (
	"context"
	"fmt"
	"log/slog"
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
	MergeStyle    string // fallback merge style when restoring a consumed schedule
	StagingBranch string // staging branch prefix, e.g. "mq/main/staging"
	InstanceURL   string // used for API/git (may be an in-cluster URL)
	PublicURL     string // used for user-facing links (defaults to InstanceURL)
	MaxBatch      int    // cap the initial rollup size (0 = unlimited)
	BatchLinger   time.Duration
	BatchTarget   int
	BisectFanout  int // max concurrent bisection staging runs (0 = 1)
	QueueComments bool
	BotUser       string
	Metrics       *metrics.Collector
	Checkpoint    CheckpointStore
	Logger        *slog.Logger
}

type activeBatch struct {
	prs           []forge.PullRequest
	stagingBranch string
	stagingSHA    string
	debugURL      string
	baseGen       int
	outcome       string
	releasedPR    int
	releasedAt    time.Time
}

// ForgeAPI is the subset of the forge client the engine needs (interface so the
// reconcile/bisection logic is unit-testable with a mock).
type ForgeAPI interface {
	ListOpenPRs(ctx context.Context, owner, repo, base string) ([]forge.PullRequest, error)
	GetPR(ctx context.Context, owner, repo string, index int) (forge.PullRequest, error)
	AutomergeState(ctx context.Context, owner, repo string, index int) (forge.AutomergeState, error)
	LatestCommitStatus(ctx context.Context, owner, repo, sha, statusContext string) (forge.CommitStatus, bool, error)
	RunStatus(ctx context.Context, owner, repo, sha, branch string) (string, error)
	RunTargetURL(ctx context.Context, owner, repo, sha, branch string) (string, error)
	SetCommitStatus(ctx context.Context, owner, repo, sha, context, state, desc, targetURL string) error
	ScheduleAutomerge(ctx context.Context, owner, repo string, index int, style, headSHA string) error
	CancelAutomerge(ctx context.Context, owner, repo string, index int) (bool, error)
	UpsertComment(ctx context.Context, owner, repo string, index int, marker, botUser, body string) error
}

const (
	landingClaimDescription   = "merge queue: preparing passed batch to land"
	landingSuccessDescription = "merge queue: batch passed"
	queueRestoreDescription   = "merge queue: re-queued after incomplete native merge"
	nativeMergeTimeout        = 5 * time.Minute
)

// Stager builds an integration ("staging") branch from a base + PR head refs.
type Stager interface {
	BuildStaging(ctx context.Context, base, stagingBranch string, refs []gitops.MergedRef) (sha string, conflictPR int, err error)
}

type Engine struct {
	cfg     Config
	fc      ForgeAPI
	st      Stager
	logger  *slog.Logger
	pending [][]int // work queue of candidate batches (PR numbers, in order)
	active  []*activeBatch
	now     func() time.Time

	lingerSince    time.Time
	queueFirstSeen map[int]time.Time
	baseGen        int
	stagingSeq     int

	queueComments         map[int]string
	terminalQueueComments map[int]string
	checkpointLoaded      bool
	checkpointExists      bool
}

func New(cfg Config, fc ForgeAPI, st Stager) *Engine {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default().With("component", "engine")
	}
	logger = logger.With("owner", cfg.Owner, "repo", cfg.Repo, "base", cfg.Base)
	return &Engine{
		cfg:                   cfg,
		fc:                    fc,
		st:                    st,
		logger:                logger,
		now:                   time.Now,
		queueFirstSeen:        map[int]time.Time{},
		queueComments:         map[int]string{},
		terminalQueueComments: map[int]string{},
	}
}

// Reconcile advances the queue by one step. Safe to call on a fixed interval.
func (e *Engine) Reconcile(ctx context.Context) error {
	if err := e.loadCheckpoint(ctx); err != nil {
		e.cfg.Metrics.IncReconcileError(e.metricLabels())
		e.observeQueue()
		return err
	}
	resolved, err := e.checkActive(ctx)
	if err == nil && !resolved {
		e.freeSlotForEarlierPending(ctx)
		for len(e.active) < e.activeLimit() {
			var started bool
			started, err = e.startNext(ctx)
			if err != nil || !started {
				break
			}
		}
	}
	if checkpointErr := e.saveCheckpoint(ctx); checkpointErr != nil {
		if err != nil {
			err = fmt.Errorf("%v; checkpoint: %w", err, checkpointErr)
		} else {
			err = checkpointErr
		}
	}
	if err != nil {
		e.cfg.Metrics.IncReconcileError(e.metricLabels())
	}
	if commentErr := e.syncQueueComments(ctx); commentErr != nil {
		e.logger.Error("queue status comment sync failed", "error", commentErr)
	}
	e.observeQueue()
	return err
}

// readyNumbers lists open PRs targeting base that currently have auto-merge
// scheduled, ordered FIFO, capped to MaxBatch.
func (e *Engine) readyNumbers(ctx context.Context) ([]int, error) {
	prs, err := e.fc.ListOpenPRs(ctx, e.cfg.Owner, e.cfg.Repo, e.cfg.Base)
	if err != nil {
		return nil, err
	}
	var nums []int
	for _, p := range prs {
		ok, err := e.queued(ctx, p)
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
func (e *Engine) resolve(ctx context.Context, nums []int) ([]forge.PullRequest, error) {
	var out []forge.PullRequest
	for _, n := range nums {
		pr, err := e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, n)
		if err != nil {
			return nil, err
		}
		if pr.State != "open" || pr.Merged {
			e.observeQueueExit(n, "dropped")
			continue
		}
		ok, err := e.queued(ctx, pr)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, pr)
		} else {
			e.observeQueueExit(n, "dropped")
		}
	}
	return out, nil
}

func (e *Engine) queued(ctx context.Context, pr forge.PullRequest) (bool, error) {
	return e.queueEligibility(ctx, pr)
}

func (e *Engine) queueEligibility(ctx context.Context, pr forge.PullRequest) (bool, error) {
	state, err := e.fc.AutomergeState(ctx, e.cfg.Owner, e.cfg.Repo, pr.Number)
	if err != nil {
		return false, err
	}
	if e.cfg.StatusCtx == "" || pr.Head.Sha == "" {
		return state.Scheduled, nil
	}
	status, ok, err := e.fc.LatestCommitStatus(ctx, e.cfg.Owner, e.cfg.Repo, pr.Head.Sha, e.cfg.StatusCtx)
	if err != nil {
		return false, err
	}
	if state.Scheduled {
		if ok &&
			(status.Status == "error" || status.Status == "failure") &&
			!status.CreatedAt.IsZero() &&
			!state.UpdatedAt.IsZero() &&
			status.CreatedAt.After(state.UpdatedAt) {
			return false, nil
		}
		return true, nil
	}
	return false, nil
}

func (e *Engine) startNext(ctx context.Context) (bool, error) {
	if len(e.pending) == 0 {
		if len(e.active) > 0 {
			return false, nil
		}
		ready, err := e.readyNumbers(ctx)
		if err != nil {
			return false, err
		}
		e.observeReady(ready)
		if len(ready) == 0 {
			e.lingerSince = time.Time{}
			return false, nil
		}
		if e.linger(ready) {
			return false, nil
		}
		e.enqueue(ready)
		e.lingerSince = time.Time{}
	}
	cand := e.pending[0]
	e.pending = e.pending[1:]

	prs, err := e.resolve(ctx, cand)
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
	sha, conflictPR, err := e.st.BuildStaging(ctx, e.cfg.Base, stagingBranch, refs)
	if err != nil {
		if conflictPR > 0 {
			e.cfg.Metrics.IncStagingConflict(e.metricLabels())
			return false, e.handleStagingConflict(ctx, prs, conflictPR)
		}
		return false, err
	}
	a := &activeBatch{prs: prs, stagingBranch: stagingBranch, stagingSHA: sha, baseGen: e.baseGen}
	e.active = append(e.active, a)
	e.cfg.Metrics.IncBatchesStarted(e.metricLabels())
	e.logger.Info("testing batch", "prs", numbersOf(prs), "stagingBranch", a.stagingBranch, "sha", short(sha))
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
		e.logger.Info("batch linger started", "linger", e.cfg.BatchLinger, "target", e.cfg.BatchTarget, "ready", ready)
		return true
	}
	return now.Sub(e.lingerSince) < e.cfg.BatchLinger
}

func (e *Engine) checkActive(ctx context.Context) (bool, error) {
	for _, a := range e.active {
		if a.outcome == "" {
			changed, err := e.requeueActiveIfHeadChanged(ctx, a)
			if err != nil {
				return false, err
			}
			if changed {
				return false, nil
			}
			status, err := e.fc.RunStatus(ctx, e.cfg.Owner, e.cfg.Repo, a.stagingSHA, a.stagingBranch)
			if err != nil {
				return false, err
			}
			switch status {
			case "success", "failure", "cancelled", "error":
				changed, err := e.requeueActiveIfHeadChanged(ctx, a)
				if err != nil {
					return false, err
				}
				if changed {
					return false, nil
				}
				debugURL := e.debugURL(ctx, a)
				changed, err = e.requeueActiveIfHeadChanged(ctx, a)
				if err != nil {
					return false, err
				}
				if changed {
					return false, nil
				}
				a.outcome = status
				a.debugURL = debugURL
				e.cfg.Metrics.IncGateOutcome(e.metricLabels(), status)
			default: // "", running, waiting, blocked -> keep waiting
				continue
			}
		}
		if !e.readyToResolve(a) {
			continue
		}
		if a.baseGen != e.baseGen {
			e.requeueStaleActive(ctx, a)
			return true, nil
		}
		switch a.outcome {
		case "success":
			resolved, merged, err := e.land(ctx, a)
			if merged > 0 {
				e.baseGen++
				if !resolved {
					a.baseGen = e.baseGen
				}
				e.requeueStaleActives(ctx)
			}
			if err != nil {
				return merged > 0, err
			}
			if !resolved {
				return merged > 0, nil
			}
		case "failure", "cancelled", "error":
			resolved, err := e.bisectOrBounce(ctx, a, a.outcome)
			if err != nil {
				return false, err
			}
			if !resolved {
				return false, nil
			}
		}
		return true, nil
	}
	return false, nil
}

func (e *Engine) requeueActiveIfHeadChanged(ctx context.Context, a *activeBatch) (bool, error) {
	changed, err := e.activeHeadChanged(ctx, a)
	if err != nil || !changed {
		return changed, err
	}
	e.requeueChangedActive(ctx, a)
	return true, nil
}

func (e *Engine) activeHeadChanged(ctx context.Context, a *activeBatch) (bool, error) {
	for _, staged := range a.prs {
		current, err := e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, staged.Number)
		if err != nil {
			return false, err
		}
		if current.State != "open" || current.Merged {
			continue
		}
		if current.Head.Sha != staged.Head.Sha {
			return true, nil
		}
	}
	return false, nil
}

// land releases one PR at a time to the forge's scheduled auto-merge worker.
// The next PR is not released until the previous one is observed merged.
func (e *Engine) land(ctx context.Context, a *activeBatch) (resolved bool, merged int, err error) {
	for len(a.prs) > 0 {
		staged := a.prs[0]
		current, err := e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, staged.Number)
		if err != nil {
			return false, merged, err
		}
		if current.Merged {
			released, err := e.releasedByShunt(ctx, a, staged)
			if err != nil {
				return false, merged, err
			}
			if released {
				e.recordLanded(ctx, a, staged)
			} else {
				e.observeQueueExit(staged.Number, "dropped")
			}
			a.prs = a.prs[1:]
			a.releasedPR = 0
			a.releasedAt = time.Time{}
			merged++
			continue
		}
		if current.State != "open" {
			e.skipLand(ctx, staged, current, fmt.Sprintf("state changed to %q", current.State), len(a.prs) > 1, a.debugURL)
			e.requeueActiveRemainder(a.prs[1:])
			e.removeActive(a)
			return true, merged, nil
		}
		if current.Head.Sha != staged.Head.Sha {
			if statusErr := e.fc.SetCommitStatus(ctx, e.cfg.Owner, e.cfg.Repo, staged.Head.Sha, e.cfg.StatusCtx, "error", "merge queue: PR changed before merge; re-queued", a.debugURL); statusErr != nil {
				return false, merged, statusErr
			}
			e.skipLand(
				ctx,
				staged,
				current,
				fmt.Sprintf("head changed from %s to %s", short(staged.Head.Sha), short(current.Head.Sha)),
				len(a.prs) > 1,
				a.debugURL,
			)
			e.requeueActiveRemainder(a.prs)
			e.removeActive(a)
			return true, merged, nil
		}

		queued, err := e.queueEligibility(ctx, current)
		if err != nil {
			return false, merged, err
		}
		if !queued {
			e.skipLand(ctx, staged, current, "auto-merge is no longer scheduled", len(a.prs) > 1, a.debugURL)
			e.requeueActiveRemainder(a.prs)
			e.removeActive(a)
			return true, merged, nil
		}

		status, ok, err := e.fc.LatestCommitStatus(ctx, e.cfg.Owner, e.cfg.Repo, staged.Head.Sha, e.cfg.StatusCtx)
		if err != nil {
			return false, merged, err
		}
		if ok && status.Status == "success" && status.Description == landingSuccessDescription {
			if !e.nativeMergeTimedOut(a, staged.Number, status.CreatedAt) {
				return false, merged, nil
			}
			current, err = e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, staged.Number)
			if err != nil {
				return false, merged, err
			}
			if current.Merged {
				continue
			}
			if current.State != "open" || current.Head.Sha != staged.Head.Sha {
				continue
			}
			state, err := e.fc.AutomergeState(ctx, e.cfg.Owner, e.cfg.Repo, staged.Number)
			if err != nil {
				return false, merged, err
			}
			if !state.Scheduled {
				e.skipLand(ctx, staged, current, "auto-merge is no longer scheduled", len(a.prs) > 1, a.debugURL)
				e.requeueActiveRemainder(a.prs)
				e.removeActive(a)
				return true, merged, nil
			}
			if err := e.fc.SetCommitStatus(
				ctx,
				e.cfg.Owner,
				e.cfg.Repo,
				staged.Head.Sha,
				e.cfg.StatusCtx,
				"error",
				statusDescription("Merge did not complete"),
				a.debugURL,
			); err != nil {
				return false, merged, fmt.Errorf("block timed-out auto-merge for PR #%d: %w", staged.Number, err)
			}
			state, err = e.fc.AutomergeState(ctx, e.cfg.Owner, e.cfg.Repo, staged.Number)
			if err != nil {
				return false, merged, err
			}
			if !state.Scheduled {
				e.logger.Info("PR skipped during native merge recovery", "pr", staged.Number, "reason", "auto-merge is no longer scheduled")
				e.notifyPR(
					ctx,
					staged.Number,
					"",
					"",
					"Skipped by merge queue",
					"shunt skipped this PR before landing because auto-merge is no longer scheduled. It will be re-tested if it remains queued.",
					a.debugURL,
					true,
				)
				e.requeueActiveRemainder(a.prs)
				e.removeActive(a)
				return true, merged, nil
			}
			e.requeueActiveRemainder(a.prs)
			e.removeActive(a)
			if err := e.scheduleAutomerge(ctx, current); err != nil {
				return false, merged, fmt.Errorf("restore auto-merge for PR #%d: %w", staged.Number, err)
			}
			if err := e.fc.SetCommitStatus(
				ctx,
				e.cfg.Owner,
				e.cfg.Repo,
				current.Head.Sha,
				e.cfg.StatusCtx,
				"pending",
				queueRestoreDescription,
				a.debugURL,
			); err != nil {
				return false, merged, fmt.Errorf("block restored auto-merge for PR #%d: %w", staged.Number, err)
			}
			e.notifyPR(
				ctx,
				staged.Number,
				staged.Head.Sha,
				"",
				"Merge did not complete",
				"the forge did not complete its scheduled merge; shunt restored the queue entry for a fresh test.",
				a.debugURL,
				true,
			)
			e.logger.Error("native auto-merge timed out", "pr", staged.Number)
			return true, merged, nil
		}
		if err := e.fc.SetCommitStatus(
			ctx,
			e.cfg.Owner,
			e.cfg.Repo,
			staged.Head.Sha,
			e.cfg.StatusCtx,
			"pending",
			landingClaimDescription,
			e.commitURL(a.stagingSHA),
		); err != nil {
			return false, merged, err
		}
		current, err = e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, staged.Number)
		if err != nil {
			return false, merged, err
		}
		if current.Merged {
			continue
		}
		if current.State != "open" || current.Head.Sha != staged.Head.Sha {
			continue
		}
		state, err := e.fc.AutomergeState(ctx, e.cfg.Owner, e.cfg.Repo, staged.Number)
		if err != nil {
			return false, merged, err
		}
		if !state.Scheduled {
			e.skipLand(ctx, staged, current, "auto-merge is no longer scheduled", len(a.prs) > 1, a.debugURL)
			e.requeueActiveRemainder(a.prs)
			e.removeActive(a)
			return true, merged, nil
		}
		if err := e.fc.SetCommitStatus(
			ctx,
			e.cfg.Owner,
			e.cfg.Repo,
			staged.Head.Sha,
			e.cfg.StatusCtx,
			"success",
			landingSuccessDescription,
			e.commitURL(a.stagingSHA),
		); err != nil {
			return false, merged, err
		}
		a.releasedPR = staged.Number
		a.releasedAt = e.now()
		e.logger.Info("PR released to native auto-merge", "pr", staged.Number)
		return false, merged, nil
	}

	e.removeActive(a)
	return true, merged, nil
}

func (e *Engine) scheduleAutomerge(ctx context.Context, pr forge.PullRequest) error {
	if pr.State != "open" || pr.Merged {
		return nil
	}
	return e.fc.ScheduleAutomerge(ctx, e.cfg.Owner, e.cfg.Repo, pr.Number, e.cfg.MergeStyle, pr.Head.Sha)
}

func (e *Engine) nativeMergeTimedOut(a *activeBatch, pr int, statusCreatedAt time.Time) bool {
	releasedAt := statusCreatedAt
	if a.releasedPR == pr && !a.releasedAt.IsZero() {
		releasedAt = a.releasedAt
	}
	if releasedAt.IsZero() {
		a.releasedPR = pr
		a.releasedAt = e.now()
		return false
	}
	return !e.now().Before(releasedAt.Add(nativeMergeTimeout))
}

func (e *Engine) recordLanded(ctx context.Context, a *activeBatch, staged forge.PullRequest) {
	e.cfg.Metrics.IncPRMerge(e.metricLabels())
	e.observeQueueExit(staged.Number, "merged")
	e.notifyPR(
		ctx,
		staged.Number,
		staged.Head.Sha,
		"",
		"Landed via merge queue",
		"shunt tested this PR in a staging batch, then the forge completed its scheduled merge.",
		a.debugURL,
		true,
	)
	e.logger.Info("PR merged", "pr", staged.Number)
}

func (e *Engine) releasedByShunt(ctx context.Context, a *activeBatch, staged forge.PullRequest) (bool, error) {
	if a.releasedPR == staged.Number {
		return true, nil
	}
	status, ok, err := e.fc.LatestCommitStatus(ctx, e.cfg.Owner, e.cfg.Repo, staged.Head.Sha, e.cfg.StatusCtx)
	if err != nil {
		return false, err
	}
	return ok && status.Status == "success" && status.Description == landingSuccessDescription, nil
}

func (e *Engine) requeueActiveRemainder(prs []forge.PullRequest) {
	nums := numbersOf(prs)
	if len(nums) > 0 {
		e.enqueue(nums)
	}
}

func (e *Engine) handleStagingConflict(ctx context.Context, prs []forge.PullRequest, conflictPR int) error {
	nums := numbersOf(prs)
	idx := indexOfNum(nums, conflictPR)
	if idx < 0 {
		return fmt.Errorf("stager reported conflict on PR #%d outside candidate %v", conflictPR, nums)
	}
	if idx == 0 {
		e.bounce(ctx, conflictPR, prs[idx].Head.Sha, "merge conflict while staging the PR", "error", "")
		if len(nums) > 1 {
			rest := append([]int(nil), nums[1:]...)
			e.enqueue(rest)
			e.logger.Info("batch conflict on first PR", "prs", nums, "conflictPR", conflictPR, "requeued", rest)
		}
		return nil
	}

	prefix := append([]int(nil), nums[:idx]...)
	suffix := append([]int(nil), nums[idx:]...)
	e.enqueue(prefix, suffix)
	e.logger.Info("batch conflict split", "prs", nums, "conflictPR", conflictPR, "prefix", prefix, "suffix", suffix)
	return nil
}

func (e *Engine) skipLand(ctx context.Context, staged, current forge.PullRequest, reason string, hasRemainder bool, debugURL string) {
	num := staged.Number
	e.logger.Info("PR skipped before merge", "pr", num, "reason", reason, "hasRemainder", hasRemainder)
	if current.State == "open" && !current.Merged {
		sha := current.Head.Sha
		if sha == "" {
			sha = staged.Head.Sha
		}
		e.notifyPR(ctx, num, sha, "error", "Skipped by merge queue", "shunt skipped this PR before landing because "+reason+". It will be re-tested if it remains queued.", debugURL, true)
	}
}

// bisectOrBounce: a size-1 failing batch bounces the culprit; a larger batch is
// split in half, with the first half tested next (the good half lands, the
// recursion isolates the bad PR(s)).
func (e *Engine) bisectOrBounce(ctx context.Context, a *activeBatch, status string) (bool, error) {
	nums := numbersOf(a.prs)
	e.removeActive(a)

	if len(nums) == 1 {
		bounced := e.bounce(ctx, nums[0], a.prs[0].Head.Sha, fmt.Sprintf("merge-queue gate **%s**", status), gateOutcomeStatus(status), a.debugURL)
		return bounced, nil
	}
	mid := len(nums) / 2
	first := append([]int(nil), nums[:mid]...)
	second := append([]int(nil), nums[mid:]...)
	e.enqueue(first, second)
	e.logger.Info("batch failed; bisecting", "prs", nums, "status", status, "first", first, "second", second)
	return true, nil
}

func gateOutcomeStatus(status string) string {
	if status == "failure" {
		return "failure"
	}
	return "error"
}

func (e *Engine) bounce(ctx context.Context, num int, expectedHeadSHA, reason, statusState, debugURL string) bool {
	if pr, err := e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, num); err == nil && pr.State == "open" && !pr.Merged {
		if expectedHeadSHA != "" && pr.Head.Sha != expectedHeadSHA {
			e.enqueue([]int{num})
			e.logger.Info("PR requeued instead of bounced after head changed", "pr", num, "oldHead", short(expectedHeadSHA), "newHead", short(pr.Head.Sha))
			return false
		}
		e.notifyPR(ctx, num, pr.Head.Sha, statusState, "Bounced from merge queue", "shunt rejected this PR from the merge queue: "+reason+".", debugURL, true)
	}
	e.cfg.Metrics.IncBounce(e.metricLabels())
	e.observeQueueExit(num, "bounced")
	if _, err := e.fc.CancelAutomerge(ctx, e.cfg.Owner, e.cfg.Repo, num); err != nil {
		e.logger.Warn("cancel auto-merge failed", "pr", num, "error", err)
	}
	e.logger.Info("PR bounced", "pr", num, "reason", reason)
	return true
}

func (e *Engine) activeLimit() int {
	if e.cfg.BisectFanout > 0 {
		return e.cfg.BisectFanout
	}
	return 1
}

func (e *Engine) stagingBranch() string {
	e.stagingSeq++
	return fmt.Sprintf("%s-%d-%d", e.cfg.StagingBranch, e.now().UnixNano(), e.stagingSeq)
}

func (e *Engine) enqueue(cands ...[]int) {
	for _, cand := range cands {
		if len(cand) == 0 {
			continue
		}
		copyCand := append([]int(nil), cand...)
		e.markQueued(copyCand...)
		e.pending = append(e.pending, copyCand)
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

func (e *Engine) freeSlotForEarlierPending(ctx context.Context) {
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
	e.active = append(e.active[:idx], e.active[idx+1:]...)
	e.enqueue(numbersOf(a.prs))
	e.logger.Info("speculative batch requeued for earlier candidate", "prs", numbersOf(a.prs), "earlier", e.pending[0])
}

func (e *Engine) requeueStaleActive(ctx context.Context, a *activeBatch) {
	e.removeActive(a)
	e.enqueue(numbersOf(a.prs))
	e.logger.Info("stale speculative batch requeued after base advanced", "prs", numbersOf(a.prs))
}

func (e *Engine) requeueChangedActive(ctx context.Context, a *activeBatch) {
	e.removeActive(a)
	e.enqueue(numbersOf(a.prs))
	e.logger.Info("active batch requeued after PR head changed", "prs", numbersOf(a.prs))
}

func (e *Engine) requeueStaleActives(ctx context.Context) {
	for _, a := range append([]*activeBatch(nil), e.active...) {
		if a.baseGen != e.baseGen {
			e.requeueStaleActive(ctx, a)
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
	pending := make([][]int, 0, len(e.pending))
	for _, cand := range e.pending {
		pending = append(pending, append([]int(nil), cand...))
	}
	active := make([][]int, 0, len(e.active))
	for _, a := range e.active {
		active = append(active, numbersOf(a.prs))
	}
	e.cfg.Metrics.ObserveQueueStatus(e.metricLabels(), pending, active)
	e.cfg.Metrics.ObserveQueueAge(e.metricLabels(), e.oldestQueueAge())
}

const queueCommentMarker = "<!-- shunt:queue-status -->"
const outcomeCommentMarker = "<!-- shunt:outcome -->"

type queueCommentStatus struct {
	number        int
	position      int
	total         int
	state         string
	activeSummary string
}

func (e *Engine) syncQueueComments(ctx context.Context) error {
	if !e.cfg.QueueComments {
		return nil
	}
	statuses, err := e.queueCommentStatuses(ctx)
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
		if err := e.fc.UpsertComment(ctx, e.cfg.Owner, e.cfg.Repo, num, queueCommentMarker, e.cfg.BotUser, body); err != nil {
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
			if err := e.fc.UpsertComment(ctx, e.cfg.Owner, e.cfg.Repo, num, queueCommentMarker, e.cfg.BotUser, body); err != nil {
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

func (e *Engine) queueCommentStatuses(ctx context.Context) ([]queueCommentStatus, error) {
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
			} else if a.releasedPR != 0 {
				state = "released to forge; waiting for merge"
			}
		}
		for _, pr := range a.prs {
			states[pr.Number] = state
		}
	}
	if len(states) == 0 {
		ready, err := e.readyNumbers(ctx)
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

func (e *Engine) notifyPR(ctx context.Context, num int, sha, statusState, title, detail, debugURL string, durableComment bool) {
	if statusState != "" && sha != "" {
		if err := e.fc.SetCommitStatus(ctx, e.cfg.Owner, e.cfg.Repo, sha, e.cfg.StatusCtx, statusState, statusDescription(title), debugURL); err != nil {
			e.logger.Warn("set source PR status failed", "pr", num, "error", err)
		}
	}
	body := terminalCommentBody(title, detail, debugURL)
	if e.cfg.QueueComments {
		sticky := e.queueCommentTerminalBody(title, detail, debugURL)
		if err := e.fc.UpsertComment(ctx, e.cfg.Owner, e.cfg.Repo, num, queueCommentMarker, e.cfg.BotUser, sticky); err != nil {
			e.logger.Warn("update sticky PR comment failed", "pr", num, "error", err)
		} else {
			e.queueComments[num] = sticky
			e.terminalQueueComments[num] = sticky
		}
	}
	if durableComment {
		if err := e.fc.UpsertComment(ctx, e.cfg.Owner, e.cfg.Repo, num, outcomeCommentMarker, e.cfg.BotUser, body); err != nil {
			e.logger.Warn("update durable PR comment failed", "pr", num, "error", err)
		}
	}
}

func terminalCommentBody(title, detail, debugURL string) string {
	var b strings.Builder
	fmt.Fprintln(&b, outcomeCommentMarker)
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
		return "merge queue: merge did not complete"
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

func (e *Engine) markQueued(nums ...int) {
	if e.queueFirstSeen == nil {
		e.queueFirstSeen = map[int]time.Time{}
	}
	now := e.now()
	for _, n := range nums {
		if _, ok := e.queueFirstSeen[n]; !ok {
			e.queueFirstSeen[n] = now
		}
	}
}

func (e *Engine) observeReady(nums []int) {
	ready := make(map[int]bool, len(nums))
	for _, n := range nums {
		ready[n] = true
	}
	for n := range e.queueFirstSeen {
		if !ready[n] {
			e.observeQueueExit(n, "dropped")
		}
	}
	e.markQueued(nums...)
}

func (e *Engine) observeQueueExit(num int, outcome string) {
	if e.queueFirstSeen == nil {
		return
	}
	seen, ok := e.queueFirstSeen[num]
	if !ok {
		return
	}
	age := e.now().Sub(seen)
	if age < 0 {
		age = 0
	}
	e.cfg.Metrics.ObserveTimeInQueue(e.metricLabels(), outcome, age)
	delete(e.queueFirstSeen, num)
}

func (e *Engine) oldestQueueAge() time.Duration {
	now := e.now()
	var oldest time.Duration
	for _, seen := range e.queueFirstSeen {
		age := now.Sub(seen)
		if age > oldest {
			oldest = age
		}
	}
	if oldest < 0 {
		return 0
	}
	return oldest
}

func (e *Engine) commitURL(sha string) string {
	base := e.cfg.PublicURL
	if base == "" {
		base = e.cfg.InstanceURL
	}
	return fmt.Sprintf("%s/%s/%s/commit/%s", base, e.cfg.Owner, e.cfg.Repo, sha)
}

func (e *Engine) debugURL(ctx context.Context, a *activeBatch) string {
	targetURL, err := e.fc.RunTargetURL(ctx, e.cfg.Owner, e.cfg.Repo, a.stagingSHA, a.stagingBranch)
	if err != nil {
		e.logger.Warn("run target lookup failed", "sha", short(a.stagingSHA), "error", err)
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
