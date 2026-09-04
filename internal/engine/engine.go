// Package engine is the merge-queue reconcile loop: rollup batching with
// bisection. It keeps a work queue of candidate batches (lists of PR numbers).
// Each cycle it tests one candidate on a fresh staging branch; on success the
// whole batch lands, on failure a multi-PR batch is split in half (bisection)
// to isolate the culprit while letting the good PRs through. Staging conflicts
// split at the conflict point so earlier PRs keep their place in the queue.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
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
	// ConfigSource describes whether this queue's settings came from a repo-level
	// .shunt.yml file ("repo") or from process-global defaults ("default").
	// Safe to expose in the /status JSON.
	ConfigSource  string
	Metrics       *metrics.Collector
	Checkpoint    CheckpointStore
	Lease         QueueLease
	LeaseHolderID string
	LeaseTTL      time.Duration
	Logger        *slog.Logger
}

type heldLeaf struct {
	batch   *activeBatch
	outcome string
}

type bisectionTree struct {
	accepted []forge.PullRequest
	results  map[string]string
	held     []heldLeaf
	cursor   int
}

type activeBatch struct {
	prs                []forge.PullRequest
	stagingBranch      string
	stagingSHA         string
	debugURL           string
	baseGen            int
	outcome            string
	releasedPR         int
	releasedAt         time.Time
	phase              string    // "waiting_gate", "waiting_merge", or "bisecting"
	phaseSince         time.Time // when the current phase started
	missingGateRetries int
	exactKey           string
	runID              string // the root batch this was bisected out of
	lineagePath        string // "r", "r0", "r01", … ; parent is this minus one char
	// speculative marks a batch staged ahead of the authoritative frontier.
	speculative bool
}

// ForgeAPI is the subset of the forge client the engine needs (interface so the
// reconcile/bisection logic is unit-testable with a mock).
type ForgeAPI interface {
	ListOpenPRs(ctx context.Context, owner, repo, base string) ([]forge.PullRequest, error)
	GetPR(ctx context.Context, owner, repo string, index int) (forge.PullRequest, error)
	AutomergeState(ctx context.Context, owner, repo string, index int) (forge.AutomergeState, error)
	ListReviews(ctx context.Context, owner, repo string, index int) ([]forge.Review, error)
	ProtectedBranch(ctx context.Context, owner, repo, branch string) (forge.BranchProtection, error)
	BranchHead(ctx context.Context, owner, repo, branch string) (string, error)
	LatestCommitStatus(ctx context.Context, owner, repo, sha, statusContext string) (forge.CommitStatus, bool, error)
	RunStatus(ctx context.Context, owner, repo, sha, branch string) (string, error)
	RunTargetURL(ctx context.Context, owner, repo, sha, branch string) (string, error)
	SetCommitStatus(ctx context.Context, owner, repo, sha, context, state, desc, targetURL string) error
	ScheduleAutomerge(ctx context.Context, owner, repo string, index int, style, headSHA string) (forge.ScheduleAutomergeResult, error)
	CancelAutomerge(ctx context.Context, owner, repo string, index int) (bool, error)
	DeleteBranch(ctx context.Context, owner, repo, branch string) error
	UpsertComment(ctx context.Context, owner, repo string, index int, marker, botUser, body string) error
}

const (
	landingClaimDescription   = "merge queue: preparing passed batch to land"
	landingSuccessDescription = "merge queue: batch passed"
	queueRestoreDescription   = "merge queue: re-queued after incomplete native merge"
	nativeMergeTimeout        = 5 * time.Minute
	missingGateMaxRetries     = 3
	// mergeStrikeCap is how many consecutive native-merge failures on the
	// same PR head bounce the PR out of the queue instead of re-queueing it
	// forever. A never-mergeable PR (approval-blocked, changes requested,
	// merge conflict) is observed via the 5-minute native-merge timeout; two
	// consecutive timeouts on the same head terminate it.
	mergeStrikeCap = 2
)

// nativeMergeGrace bounds how long land() waits within one tick for the
// forge to complete a released PR's scheduled merge before yielding to the
// next reconcile tick; nativeMergePoll is the re-check interval while
// waiting. Vars (not consts) so tests can shrink them.
var (
	nativeMergeGrace = 30 * time.Second
	nativeMergePoll  = time.Second
)

// Stager builds a staging branch from a base and PR head references.
// baseAnchor selects a fixed base commit when it is not empty.
type Stager interface {
	BuildStaging(ctx context.Context, base, baseAnchor, stagingBranch string, refs []gitops.MergedRef) (sha string, conflictPR int, err error)
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

	// A staging branch records its position in one bisection tree.
	//
	//   <prefix>-<runID>-r      root batch
	//   <prefix>-<runID>-r0     first child
	//   <prefix>-<runID>-r01    second child of the first child
	//
	// The path identifies the parent and the child order.
	// runID identifies one root batch and its child batches.
	// lineage records the path for each pending candidate.
	runID        string
	lineage      map[int]string
	lineageRunID map[int]string
	trees        map[string]*bisectionTree
	// rootAnchor pins the base-branch commit SHA read when each root runID was
	// opened. Every staged integration and every exact test key under that root
	// is anchored to it, so main moving between two nodes cannot silently
	// change what a cached outcome means.
	rootAnchor map[string]string

	// bisectOrigins tracks the first PR number of each pending candidate that was
	// produced by bisection. Checked in startNext to set phase = "bisecting".
	bisectOrigins map[int]bool

	queueComments         map[int]string
	terminalQueueComments map[int]string
	requeueStates         map[int]string
	// mergeStrikes counts consecutive native-merge failures per PR head SHA
	// so a never-mergeable PR is bounced instead of re-queued indefinitely.
	mergeStrikes     map[string]int
	checkpointLoaded bool
	checkpointExists bool
	leaseHeld        bool
	durableLease     bool

	// transitions accumulates single-shot lifecycle records for the current
	// Reconcile call; see transitions.go.
	transitions []Transition
	// outbox holds terminal transitions (those with an EventID, driving an
	// irreversible side effect) until they have been re-sent enough times to
	// survive a dropped reconcile response. It is checkpointed, so it also
	// survives a restart. Redelivery is safe because the consumer de-dups by
	// EventID.
	outbox []outboxEntry
	// pendingAcks are EventIDs the consumer confirmed on the reconcile request
	// but that could not be applied yet because the outbox is loaded from the
	// checkpoint inside Reconcile. Applied right after loadCheckpoint.
	pendingAcks []string
}

type outboxEntry struct {
	t        Transition
	attempts int
}

// outboxMaxAttempts bounds how many reconcile responses re-carry a terminal
// transition. Seven gives generous coverage for a dropped response without
// letting the outbox grow without bound.
const outboxMaxAttempts = 7

func New(cfg Config, fc ForgeAPI, st Stager) *Engine {
	durableLease := cfg.Lease != nil
	if cfg.Lease == nil {
		cfg.Lease = alwaysHeldLease{}
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 45 * time.Second
	}
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
		bisectOrigins:         map[int]bool{},
		queueComments:         map[int]string{},
		terminalQueueComments: map[int]string{},
		requeueStates:         map[int]string{},
		mergeStrikes:          map[string]int{},
		lineage:               map[int]string{},
		lineageRunID:          map[int]string{},
		trees:                 map[string]*bisectionTree{},
		rootAnchor:            map[string]string{},
		durableLease:          durableLease,
	}
}

// Reconcile advances the queue by one step. Safe to call on a fixed interval.
func (e *Engine) Reconcile(ctx context.Context) error {
	e.transitions = nil
	if e.durableLease {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.cfg.LeaseTTL/2)
		defer cancel()
	}
	if err := e.reconcileContextErr(ctx); err != nil {
		return err
	}
	held, err := e.acquireLease(ctx)
	if err != nil {
		if e.cfg.Metrics != nil {
			e.cfg.Metrics.IncReconcileError(e.metricLabels())
		}
		e.observeQueue()
		return err
	}
	if !held {
		return nil
	}
	// Record reconcile duration only for the instance that holds the lease,
	// to avoid standby instances skewing the histogram in HA deployments.
	start := e.now()
	defer func() {
		if e.cfg.Metrics != nil {
			e.cfg.Metrics.ObserveReconcileDuration(e.metricLabels(), e.now().Sub(start))
		}
	}()
	if err := e.reconcileContextErr(ctx); err != nil {
		return err
	}
	if err := e.loadCheckpoint(ctx); err != nil {
		if e.cfg.Metrics != nil {
			e.cfg.Metrics.IncReconcileError(e.metricLabels())
		}
		e.observeQueue()
		return err
	}
	// The outbox only exists once the checkpoint is loaded, so consumer acks
	// and the retransmit-attempt count are applied here, not at the top of the
	// call — otherwise a fresh process would drop neither and redeliver every
	// terminal transition forever.
	e.applyPendingAcks()
	e.ageOutbox()
	if err := e.reconcileContextErr(ctx); err != nil {
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
	if err := e.reconcileContextErr(ctx); err != nil {
		return err
	}
	if checkpointErr := e.saveCheckpoint(ctx); checkpointErr != nil {
		if err != nil {
			err = fmt.Errorf("%v; checkpoint: %w", err, checkpointErr)
		} else {
			err = checkpointErr
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if err == nil {
			err = ctxErr
		} else if !errors.Is(err, ctxErr) {
			err = errors.Join(err, ctxErr)
		}
		if e.cfg.Metrics != nil {
			e.cfg.Metrics.IncReconcileError(e.metricLabels())
		}
		e.observeQueue()
		return err
	}
	if errors.Is(err, forge.ErrUnavailable) {
		e.observeQueue()
		return nil
	}
	if err != nil {
		if e.cfg.Metrics != nil {
			e.cfg.Metrics.IncReconcileError(e.metricLabels())
		}
	}
	if commentErr := e.syncQueueComments(ctx); commentErr != nil {
		if !errors.Is(commentErr, forge.ErrUnavailable) {
			e.logger.Error("queue status comment sync failed", "error", commentErr)
		}
	}
	e.observeQueue()
	return err
}

func (e *Engine) reconcileContextErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		if e.cfg.Metrics != nil {
			e.cfg.Metrics.IncReconcileError(e.metricLabels())
		}
		e.observeQueue()
		return err
	}
	return nil
}

func (e *Engine) acquireLease(ctx context.Context) (bool, error) {
	held, err := e.cfg.Lease.AcquireLease(ctx, e.queueKey(), e.cfg.LeaseHolderID, e.cfg.LeaseTTL)
	if err != nil || !held {
		e.leaseHeld = false
		return held, err
	}
	if !e.leaseHeld && e.durableLease {
		e.resetVolatileQueueState()
	}
	e.leaseHeld = true
	return true, nil
}

func (e *Engine) resetVolatileQueueState() {
	e.pending = nil
	e.active = nil
	e.lingerSince = time.Time{}
	e.queueFirstSeen = map[int]time.Time{}
	e.bisectOrigins = map[int]bool{}
	e.baseGen = 0
	e.stagingSeq = 0
	e.runID = ""
	e.lineage = nil
	e.queueComments = map[int]string{}
	e.terminalQueueComments = map[int]string{}
	e.requeueStates = map[int]string{}
	e.mergeStrikes = map[string]int{}
	e.outbox = nil
	e.checkpointLoaded = false
	e.checkpointExists = false
}

// readyNumbers lists open PRs targeting base that currently have auto-merge
// scheduled, ordered FIFO, capped to MaxBatch.
func (e *Engine) readyNumbers(ctx context.Context) ([]int, error) {
	prs, err := e.fc.ListOpenPRs(ctx, e.cfg.Owner, e.cfg.Repo, e.cfg.Base)
	if err != nil {
		return nil, err
	}
	// Fetch the base-branch protection rule once per admission pass. If the
	// rule requires approvals or blocks on reviews, a PR that cannot satisfy
	// it (approval-blocked, changes-requested) must not enter the queue at
	// all — it would otherwise waste a full staging/CI run before the
	// native-merge timeout bounce catches it.
	protection, err := e.fc.ProtectedBranch(ctx, e.cfg.Owner, e.cfg.Repo, e.cfg.Base)
	if err != nil {
		return nil, err
	}
	gate := admissionGate{protection: protection}
	var nums []int
	for _, p := range prs {
		// Quick check: if the PR is not scheduled, it was likely a bounced
		// or cancelled PR — skip without the admission gate.
		state, err := e.fc.AutomergeState(ctx, e.cfg.Owner, e.cfg.Repo, p.Number)
		if err != nil {
			return nil, err
		}
		if !state.Scheduled {
			e.logger.Info("PR not eligible for merge queue", "pr", p.Number, "reason", "not scheduled")
			e.observeQueueExit(p.Number, "ineligible")
			continue
		}
		// Admission gate: ask Forgejo to accept the PR into the merge queue.
		// For PRs already scheduled, Forgejo returns 409 (already scheduled) → eligible.
		// For new PRs, Forgejo validates merge requirements (e.g. approvals) and
		// returns 422 if the PR doesn't meet them.
		result, err := e.fc.ScheduleAutomerge(ctx, e.cfg.Owner, e.cfg.Repo, p.Number, e.cfg.MergeStyle, p.Head.Sha)
		if err != nil {
			e.logger.Info("PR not eligible for merge queue", "pr", p.Number, "error", err)
			e.observeQueueExit(p.Number, "ineligible")
			continue
		}
		if !result.Eligible {
			e.logger.Info("PR rejected by forge for merge queue", "pr", p.Number)
			e.observeQueueExit(p.Number, "ineligible")
			continue
		}
		// For an already-scheduled PR the 409 path skips Forgejo's 422
		// requirement validation, so re-check the review policy ourselves:
		// a PR blocked by approvals or a live changes-requested review would
		// never merge and must not consume a staging/CI run.
		if blocked, reason, err := gate.blocks(ctx, e.fc, e.cfg.Owner, e.cfg.Repo, p.Number); err != nil {
			return nil, err
		} else if blocked {
			e.logger.Info("PR blocked from merge queue by review policy", "pr", p.Number, "reason", reason)
			e.observeQueueExit(p.Number, "ineligible")
			continue
		}
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

// admissionGate replicates the branch-protection review policy that Forgejo's
// merge gate applies, so shunt can refuse to queue a PR that can never merge.
// The rule only blocks when it actually requires something; a repo with no
// protection rule (or one without approval/review requirements) admits freely.
type admissionGate struct {
	protection forge.BranchProtection
}

// blocks reports whether the PR is barred from the queue by the base-branch
// review policy, and why. It mirrors HasEnoughApprovals,
// MergeBlockedByRejectedReview, and MergeBlockedByOfficialReviewRequests:
// approvals must be official, non-dismissed, and (when IgnoreStaleApprovals)
// non-stale; a live REQUEST_CHANGES blocks when BlockOnRejectedReviews; an
// outstanding official review request blocks when
// BlockOnOfficialReviewRequests.
func (g admissionGate) blocks(ctx context.Context, fc ForgeAPI, owner, repo string, num int) (bool, string, error) {
	p := g.protection
	if p.RequiredApprovals == 0 && !p.BlockOnRejectedReviews && !p.BlockOnOfficialReviewRequests {
		return false, "", nil
	}
	reviews, err := fc.ListReviews(ctx, owner, repo, num)
	if err != nil {
		return false, "", err
	}
	var granted int64
	for _, r := range reviews {
		if r.Dismissed {
			continue
		}
		if r.State == "APPROVED" && r.Official && (!p.IgnoreStaleApprovals || !r.Stale) {
			granted++
		}
		if p.BlockOnRejectedReviews && r.State == "REQUEST_CHANGES" && r.Official {
			return true, "a reviewer requested changes", nil
		}
		if p.BlockOnOfficialReviewRequests && r.State == "REQUEST_REVIEW" && r.Official {
			return true, "an official review was requested", nil
		}
	}
	if p.RequiredApprovals > 0 && granted < p.RequiredApprovals {
		return true, fmt.Sprintf("needs %d more approval(s)", p.RequiredApprovals-granted), nil
	}
	return false, "", nil
}

// resolve drops PRs from a candidate that are no longer open or no longer have
// auto-merge scheduled (e.g. merged in a prior sub-batch, or bounced).
func (e *Engine) resolve(ctx context.Context, nums []int) ([]forge.PullRequest, error) {
	protection, err := e.fc.ProtectedBranch(ctx, e.cfg.Owner, e.cfg.Repo, e.cfg.Base)
	if err != nil {
		return nil, err
	}
	gate := admissionGate{protection: protection}
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
		// Fast-fail for bounced/cancelled PRs — don't call ScheduleAutomerge
		// (which is an admission gate for new PRs) just to re-check already-bounced ones.
		state, err := e.fc.AutomergeState(ctx, e.cfg.Owner, e.cfg.Repo, n)
		if err != nil {
			return nil, err
		}
		if !state.Scheduled {
			e.logger.Info("dropping PR in resolve: not scheduled", "pr", n)
			e.observeQueueExit(n, "dropped")
			continue
		}
		if blocked, reason, err := gate.blocks(ctx, e.fc, e.cfg.Owner, e.cfg.Repo, n); err != nil {
			return nil, err
		} else if blocked {
			e.logger.Info("dropping PR in resolve: blocked by review policy", "pr", n, "reason", reason)
			e.observeQueueExit(n, "ineligible")
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
	return state.Scheduled, nil
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
		lingering := e.linger(ready)
		if lingering {
			e.acknowledgeQueued(ctx, ready)
			return false, nil
		}
		e.enqueue(ready)
		if !e.lingerSince.IsZero() {
			if e.cfg.Metrics != nil {
				e.cfg.Metrics.ObserveLingerDuration(e.metricLabels(), e.now().Sub(e.lingerSince))
			}
		}
		e.lingerSince = time.Time{}
	}
	cand := e.pending[0]
	e.pending = e.pending[1:]

	prs, err := e.resolve(ctx, cand)
	if err != nil {
		return false, err
	}
	if len(prs) == 0 {
		// Remove lineage when all PRs leave a candidate before staging.
		// This lets its root continue finalization.
		if len(cand) > 0 {
			if runID := e.lineageRunID[cand[0]]; runID != "" {
				path := e.lineage[cand[0]]
				delete(e.lineage, cand[0])
				delete(e.lineageRunID, cand[0])
				delete(e.bisectOrigins, cand[0])
				e.recordTransition(Transition{Kind: "bisected", RunID: runID, LineagePath: path})
				e.logger.Info("bisection node dropped: all its candidates withdrew before staging", "prs", cand, "runID", runID)
			}
		}
		return false, nil
	}

	runID, lineagePath := e.lineageFor(cand)
	anchor, err := e.ensureRootAnchor(ctx, runID)
	if err != nil {
		return false, err
	}
	stagedPRs := prs
	speculative := false
	if tree, ok := e.trees[runID]; ok {
		baseline := append([]forge.PullRequest(nil), tree.accepted...)
		// Include unresolved earlier nodes in a speculative staging attempt.
		// checkActive replaces this attempt when its baseline is not valid.
		if earlier := e.unresolvedEarlier(runID, cand[0]); len(earlier) > 0 {
			baseline = append(baseline, earlier...)
			speculative = true
		}
		stagedPRs = append(baseline, prs...)
	}
	refs := make([]gitops.MergedRef, len(stagedPRs))
	for i, p := range stagedPRs {
		refs[i] = gitops.MergedRef{PR: p.Number, Ref: fmt.Sprintf("refs/pull/%d/head", p.Number)}
	}
	exactKey := e.exactKey(anchor, stagedPRs)
	if tree, ok := e.trees[runID]; ok {
		if outcome, cached := tree.results[exactKey]; cached {
			e.active = append(e.active, &activeBatch{prs: prs, stagingBranch: e.stagingBranchFor(runID, lineagePath), outcome: outcome, phase: phaseForOutcome(outcome), phaseSince: e.now(), runID: runID, lineagePath: lineagePath, exactKey: exactKey, speculative: speculative})
			return true, nil
		}
	}
	stagingBranch := e.stagingBranchFor(runID, lineagePath)
	// Build each root child on the root's fixed base anchor.
	sha, conflictPR, err := e.st.BuildStaging(ctx, e.cfg.Base, anchor, stagingBranch, refs)
	if err != nil {
		if conflictPR > 0 {
			if e.cfg.Metrics != nil {
				e.cfg.Metrics.IncStagingConflict(e.metricLabels())
			}
			return false, e.handleStagingConflict(ctx, prs, conflictPR)
		}
		return false, err
	}
	phase := "waiting_gate"
	if e.bisectOrigins[cand[0]] {
		phase = "bisecting"
		delete(e.bisectOrigins, cand[0])
	}
	a := &activeBatch{
		prs:           prs,
		stagingBranch: stagingBranch,
		stagingSHA:    sha,
		baseGen:       e.baseGen,
		phase:         phase,
		phaseSince:    e.now(),
		runID:         runID,
		lineagePath:   lineagePath,
		exactKey:      exactKey,
		speculative:   speculative,
	}
	e.active = append(e.active, a)
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.IncBatchesStarted(e.metricLabels())
		if speculative {
			e.cfg.Metrics.IncSpeculativeStarted(e.metricLabels())
		}
	}
	e.logger.Info("testing batch", "prs", numbersOf(prs), "stagingBranch", a.stagingBranch, "sha", short(sha))
	e.recordTransition(Transition{
		Kind: "staged", PRs: numbersOf(prs), StagingBranch: a.stagingBranch,
		RunID: a.runID, LineagePath: a.lineagePath,
	})
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
	}
	if now.Sub(e.lingerSince) < e.cfg.BatchLinger {
		// Log every tick so the host's backoff scheduler treats a lingering
		// repo as active (it must keep polling until the batch builds).
		e.logger.Info("batch linger active", "linger", e.cfg.BatchLinger, "elapsed", now.Sub(e.lingerSince), "ready", ready)
		return true
	}
	return false
}

func missingGateGrace(retries int) time.Duration {
	// retries is the number of fresh staging refs already attempted.
	switch retries {
	case 0:
		return 10 * time.Minute
	case 1:
		return 20 * time.Minute
	case 2:
		return 40 * time.Minute
	default:
		return 60 * time.Minute
	}
}

func (e *Engine) checkActive(ctx context.Context) (bool, error) {
	if resolved, err := e.invalidateAdvancedRoots(ctx); resolved || err != nil {
		return resolved, err
	}
	if resolved, err := e.invalidateRootsOnCandidateChange(ctx); resolved || err != nil {
		return resolved, err
	}
	if resolved, err := e.finalizeReadyTree(ctx); resolved || err != nil {
		return resolved, err
	}
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
			case "":
				changed, err := e.invalidateStaleActive(ctx, a)
				if err != nil {
					return false, err
				}
				if changed {
					return false, nil
				}
				// A staging ref without any gate result cannot make progress. Back off
				// re-staging attempts, then bounce rather than creating refs forever.
				if e.now().Sub(a.phaseSince) < missingGateGrace(a.missingGateRetries) {
					continue
				}
				if a.missingGateRetries >= missingGateMaxRetries {
					// A missing gate result cannot decide a source PR.
					// Requeue the root when the retry limit is reached.
					if e.requeuedTreeNode(ctx, a, "staging gate produced no result after retries",
						"re-queued: the staging gate produced no result") {
						e.logger.Error("bisection root aborted: a node's gate never ran", "prs", numbersOf(a.prs), "runID", a.runID)
						return true, nil
					}
					e.cleanupBatch(ctx, a)
					for _, staged := range a.prs {
						e.bounceFrom(ctx, a.runID, a.stagingBranch, staged.Number, staged.Head.Sha, "no CI gate run after 3 retries", "error", "")
					}
					e.logger.Error("staging batch abandoned after missing gate retries", "prs", numbersOf(a.prs))
					return true, nil
				}

				prs := a.prs
				e.cleanupBatch(ctx, a)
				refs := make([]gitops.MergedRef, len(prs))
				for i, pr := range prs {
					refs[i] = gitops.MergedRef{PR: pr.Number, Ref: fmt.Sprintf("refs/pull/%d/head", pr.Number)}
				}
				// A missing-gate retry restages the SAME batch at the same
				// point in the tree, so it keeps its run and path rather than
				// becoming a new node. cleanupBatch above deleted the previous
				// branch, so the name is free to reuse.
				branch := e.stagingBranchFor(a.runID, a.lineagePath)
				sha, conflictPR, err := e.st.BuildStaging(ctx, e.cfg.Base, e.rootAnchor[a.runID], branch, refs)
				if err != nil {
					if conflictPR > 0 {
						return false, e.handleStagingConflict(ctx, prs, conflictPR)
					}
					return false, err
				}
				retry := a.missingGateRetries + 1
				e.active = append(e.active, &activeBatch{
					prs:                prs,
					runID:              a.runID,
					lineagePath:        a.lineagePath,
					stagingBranch:      branch,
					stagingSHA:         sha,
					baseGen:            e.baseGen,
					phase:              "waiting_gate",
					phaseSince:         e.now(),
					missingGateRetries: retry,
					exactKey:           a.exactKey,
				})
				e.logger.Warn("staging batch re-staged after no gate result",
					"prs", numbersOf(prs), "stagingBranch", branch, "retry", retry)
				return true, nil
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
				if tree, ok := e.trees[a.runID]; ok {
					tree.results[a.exactKey] = status
				}
				if e.cfg.Metrics != nil {
					e.cfg.Metrics.IncGateOutcome(e.metricLabels(), status)
					e.cfg.Metrics.ObserveGateDuration(e.metricLabels(), status, e.now().Sub(a.phaseSince))
				}
				if status == "success" {
					a.phase = "waiting_merge"
					a.phaseSince = e.now()
					e.recordTransition(Transition{
						Kind: "gate_success", PRs: numbersOf(a.prs), StagingBranch: a.stagingBranch,
						RunID: a.runID, LineagePath: a.lineagePath,
					})
				}
			default: // running, waiting, blocked -> keep waiting
				changed, err := e.invalidateStaleActive(ctx, a)
				if err != nil {
					return false, err
				}
				if changed {
					return false, nil
				}
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
		// A speculative result is valid only when its exact key still matches
		// the resolved frontier. Re-stage the node when the keys differ.
		if a.exactKey != "" {
			if tree, ok := e.trees[a.runID]; ok {
				staged := append(append([]forge.PullRequest(nil), tree.accepted...), a.prs...)
				if want := e.exactKey(e.rootAnchor[a.runID], staged); want != a.exactKey {
					e.supersedeSpeculative(ctx, a)
					return true, nil
				}
				if a.speculative && a.outcome != "" && e.cfg.Metrics != nil {
					e.cfg.Metrics.IncSpeculativePromoted(e.metricLabels())
				}
			}
		}
		switch a.outcome {
		case "success":
			if e.holdTreeLeaf(ctx, a, "success") {
				return true, nil
			}
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

// invalidateStaleActive discards a staged batch whose PR changed or is no
// longer eligible. A head change is re-queued; cancellation or a review-policy
// change is not, so it cannot recreate the batch until the PR becomes ready.
func (e *Engine) invalidateStaleActive(ctx context.Context, a *activeBatch) (bool, error) {
	protection, err := e.fc.ProtectedBranch(ctx, e.cfg.Owner, e.cfg.Repo, e.cfg.Base)
	if err != nil {
		return false, err
	}
	gate := admissionGate{protection: protection}
	for _, staged := range a.prs {
		current, err := e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, staged.Number)
		if err != nil {
			return false, err
		}
		if current.State != "open" || current.Merged {
			e.discardIneligibleActive(ctx, a, staged.Number, "closed")
			return true, nil
		}
		if current.Head.Sha != staged.Head.Sha {
			e.requeueChangedActive(ctx, a)
			return true, nil
		}
		eligible, err := e.queueEligibility(ctx, current)
		if err != nil {
			return false, err
		}
		if !eligible {
			e.discardIneligibleActive(ctx, a, staged.Number, "auto-merge cancelled")
			return true, nil
		}
		blocked, reason, err := gate.blocks(ctx, e.fc, e.cfg.Owner, e.cfg.Repo, staged.Number)
		if err != nil {
			return false, err
		}
		if blocked {
			e.discardIneligibleActive(ctx, a, staged.Number, reason)
			return true, nil
		}
	}
	return false, nil
}

func (e *Engine) discardIneligibleActive(ctx context.Context, a *activeBatch, pr int, reason string) {
	if e.requeuedTreeNode(ctx, a, "a pinned candidate became ineligible", "re-queued: a pinned candidate became ineligible mid-test") {
		e.observeQueueExit(pr, "ineligible")
		e.logger.Info("bisection root torn down after a candidate became ineligible", "pr", pr, "reason", reason)
		return
	}
	remaining := removeNum(numbersOf(a.prs), pr)
	e.markBatchSuperseded(a, "a candidate became ineligible")
	e.cleanupBatch(ctx, a)
	e.enqueueWithState("requeued after another PR became ineligible", remaining)
	e.observeQueueExit(pr, "ineligible")
	e.logger.Info("active batch discarded after PR became ineligible",
		"pr", pr, "reason", reason, "prs", numbersOf(a.prs), "requeued", remaining)
}

func (e *Engine) requeueActiveIfHeadChanged(ctx context.Context, a *activeBatch) (bool, error) {
	for _, staged := range a.prs {
		current, err := e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, staged.Number)
		if err != nil {
			return false, err
		}
		if current.State == "open" && !current.Merged && current.Head.Sha != staged.Head.Sha {
			e.requeueChangedActive(ctx, a)
			return true, nil
		}
	}
	return false, nil
}

// land releases one PR at a time to the forge's scheduled auto-merge worker.
// The next PR is not released until the previous one is observed merged.
// After each release it waits within a bounded grace window for the forge to
// complete the merge, so the whole queue can land within one tick; if the
// merge has not completed by the deadline it returns unresolved and the next
// tick re-checks (nativeMergeTimeout recovery still applies across ticks).
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
			e.skipLand(ctx, staged, current, fmt.Sprintf("state changed to %q", current.State), len(a.prs) > 1, a.debugURL, false)
			e.requeueActiveRemainder("requeued after an earlier PR left the batch", a.prs[1:])
			e.markBatchSuperseded(a, "a PR left the batch before landing")
			e.cleanupBatch(ctx, a)
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
				true,
			)
			e.requeueActiveRemainder("requeued after PR head changed", a.prs)
			e.markBatchSuperseded(a, "a PR head changed before landing")
			e.cleanupBatch(ctx, a)
			return true, merged, nil
		}

		queued, err := e.queueEligibility(ctx, current)
		if err != nil {
			return false, merged, err
		}
		if !queued {
			if e.strikeMergeFailure(staged) {
				e.logger.Warn("PR bounced: auto-merge repeatedly cancelled", "pr", staged.Number, "head", short(staged.Head.Sha))
				e.requeueActiveRemainder("requeued after an earlier PR failed to merge", a.prs[1:])
				e.cleanupBatch(ctx, a)
				e.bounceFrom(ctx, a.runID, a.stagingBranch, staged.Number, staged.Head.Sha, "auto-merge was repeatedly cancelled before the merge completed", "error", a.debugURL)
				return true, merged, nil
			}
			e.skipLand(ctx, staged, current, "auto-merge is no longer scheduled", len(a.prs) > 1, a.debugURL, true)
			e.requeueActiveRemainder("requeued after auto-merge was cancelled", a.prs)
			e.markBatchSuperseded(a, "auto-merge was cancelled before landing")
			e.cleanupBatch(ctx, a)
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
				if e.strikeMergeFailure(staged) {
					e.logger.Warn("PR bounced: auto-merge repeatedly cancelled", "pr", staged.Number, "head", short(staged.Head.Sha))
					e.requeueActiveRemainder("requeued after an earlier PR failed to merge", a.prs[1:])
					e.cleanupBatch(ctx, a)
					e.bounceFrom(ctx, a.runID, a.stagingBranch, staged.Number, staged.Head.Sha, "auto-merge was repeatedly cancelled before the merge completed", "error", a.debugURL)
					return true, merged, nil
				}
				e.skipLand(ctx, staged, current, "auto-merge is no longer scheduled", len(a.prs) > 1, a.debugURL, true)
				e.requeueActiveRemainder("requeued after auto-merge was cancelled", a.prs)
				e.markBatchSuperseded(a, "auto-merge was cancelled during recovery")
				e.cleanupBatch(ctx, a)
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
				if e.strikeMergeFailure(staged) {
					e.logger.Warn("PR bounced: auto-merge repeatedly cancelled during recovery", "pr", staged.Number, "head", short(staged.Head.Sha))
					e.requeueActiveRemainder("requeued after an earlier PR failed to merge", a.prs[1:])
					e.cleanupBatch(ctx, a)
					e.bounceFrom(ctx, a.runID, a.stagingBranch, staged.Number, staged.Head.Sha, "auto-merge was repeatedly cancelled before the merge completed", "error", a.debugURL)
					return true, merged, nil
				}
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
					false,
				)
				e.requeueActiveRemainder("requeued after auto-merge was cancelled", a.prs)
				e.cleanupBatch(ctx, a)
				return true, merged, nil
			}
			// The forge has had a full nativeMergeTimeout to complete the
			// scheduled merge and has not. A PR whose merge repeatedly times
			// out on the same head SHA will never complete on its own
			// (approval-blocked, changes requested, or conflict) — after
			// mergeStrikeCap consecutive timeouts, bounce it instead of
			// re-queueing it forever.
			if e.strikeMergeFailure(staged) {
				e.logger.Warn("PR bounced: native merge repeatedly did not complete", "pr", staged.Number, "head", short(staged.Head.Sha))
				e.requeueActiveRemainder("requeued after an earlier PR failed to merge", a.prs[1:])
				e.cleanupBatch(ctx, a)
				e.bounceFrom(ctx, a.runID, a.stagingBranch, staged.Number, staged.Head.Sha,
					"the forge did not complete its scheduled merge after repeated attempts", "error", a.debugURL)
				return true, merged, nil
			}
			e.requeueActiveRemainder("retrying after forge merge recovery", a.prs)
			e.markBatchSuperseded(a, "native auto-merge timed out")
			e.cleanupBatch(ctx, a)
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
				false,
			)
			e.logger.Error("native auto-merge timed out", "pr", staged.Number)
			return true, merged, nil
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
			if e.strikeMergeFailure(staged) {
				e.logger.Warn("PR bounced: auto-merge repeatedly cancelled", "pr", staged.Number, "head", short(staged.Head.Sha))
				e.requeueActiveRemainder("requeued after an earlier PR failed to merge", a.prs[1:])
				e.cleanupBatch(ctx, a)
				e.bounceFrom(ctx, a.runID, a.stagingBranch, staged.Number, staged.Head.Sha, "auto-merge was repeatedly cancelled before the merge completed", "error", a.debugURL)
				return true, merged, nil
			}
			e.skipLand(ctx, staged, current, "auto-merge is no longer scheduled", len(a.prs) > 1, a.debugURL, true)
			e.requeueActiveRemainder("requeued after auto-merge was cancelled", a.prs)
			e.markBatchSuperseded(a, "auto-merge was cancelled before release")
			e.cleanupBatch(ctx, a)
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

		// Wait for the forge to complete the scheduled merge so the
		// whole queue can land within this tick. The merge queue must
		// land in order — we cannot set success on PR N+1 until PR N
		// has merged. Poll within a bounded grace window; if the merge
		// has not completed by the deadline, return unresolved and let
		// the next tick re-check (nativeMergeTimeout recovery still
		// applies across ticks). Bail early if the PR left the
		// mergeable state — it is not likely to succeed.
		deadline := e.now().Add(nativeMergeGrace)
		for {
			current, err = e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, staged.Number)
			if err != nil {
				e.logger.Warn("post-merge check failed", "pr", staged.Number, "error", err)
				return false, merged, nil
			}
			if current.Merged {
				released, _ := e.releasedByShunt(ctx, a, staged)
				if released {
					e.recordLanded(ctx, a, staged)
				}
				a.prs = a.prs[1:]
				a.releasedPR = 0
				a.releasedAt = time.Time{}
				merged++
				// Merged — continue to the next PR in the batch.
				break
			}
			if current.State != "open" || current.Head.Sha != staged.Head.Sha {
				// PR left the mergeable state while we waited; the
				// next tick's preflight will requeue it.
				return false, merged, nil
			}
			if !e.now().Before(deadline) {
				// Reasonable effort exhausted; next tick picks it up.
				return false, merged, nil
			}
			select {
			case <-ctx.Done():
				return false, merged, nil
			case <-time.After(nativeMergePoll):
			}
		}
	}

	e.cleanupBatch(ctx, a)

	return true, merged, nil
}

func (e *Engine) scheduleAutomerge(ctx context.Context, pr forge.PullRequest) error {
	if pr.State != "open" || pr.Merged {
		return nil
	}
	result, err := e.fc.ScheduleAutomerge(ctx, e.cfg.Owner, e.cfg.Repo, pr.Number, e.cfg.MergeStyle, pr.Head.Sha)
	if err != nil {
		return err
	}
	if !result.Eligible {
		return fmt.Errorf("forge rejected auto-merge for PR #%d", pr.Number)
	}
	return nil
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

// strikeMergeFailure records one more consecutive native-merge timeout for
// the PR's current head SHA and reports whether mergeStrikeCap was reached.
// A successful merge clears the strike via clearMergeFailure, so the counter
// only accumulates across genuinely repeated failures on the same head.
func (e *Engine) strikeMergeFailure(staged forge.PullRequest) bool {
	e.mergeStrikes[staged.Head.Sha]++
	return e.mergeStrikes[staged.Head.Sha] >= mergeStrikeCap
}

// clearMergeFailure forgets native-merge failure tracking for a head SHA
// (called when the PR merges, so a later PR reusing the head isn't penalized).
func (e *Engine) clearMergeFailure(headSHA string) {
	delete(e.mergeStrikes, headSHA)
}

func (e *Engine) recordLanded(ctx context.Context, a *activeBatch, staged forge.PullRequest) {
	if !a.releasedAt.IsZero() {
		if e.cfg.Metrics != nil {
			e.cfg.Metrics.ObserveNativeMergeDuration(e.metricLabels(), e.now().Sub(a.releasedAt))
		}
	}
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.IncPRMerge(e.metricLabels())
	}
	e.clearMergeFailure(staged.Head.Sha)
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
		true,
	)
	e.logger.Info("PR merged", "pr", staged.Number)
	e.recordTransition(Transition{
		Kind: "landed", PRs: []int{staged.Number}, StagingBranch: a.stagingBranch,
		RunID: a.runID, LineagePath: a.lineagePath,
		EventID: terminalEventID(a.runID, "landed", []int{staged.Number}),
	})
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

func (e *Engine) requeueActiveRemainder(state string, prs []forge.PullRequest) {
	nums := numbersOf(prs)
	if len(nums) > 0 {
		e.enqueueWithState(state, nums)
	}
}

func (e *Engine) handleStagingConflict(ctx context.Context, prs []forge.PullRequest, conflictPR int) error {
	nums := numbersOf(prs)
	idx := indexOfNum(nums, conflictPR)
	if idx < 0 {
		return fmt.Errorf("stager reported conflict on PR #%d outside candidate %v", conflictPR, nums)
	}
	if idx == 0 {
		e.bounce(ctx, "", conflictPR, prs[idx].Head.Sha, "merge conflict while staging the PR", "error", "")
		if len(nums) > 1 {
			rest := append([]int(nil), nums[1:]...)
			e.enqueueWithState("retrying after staging conflict; testing a smaller batch", rest)
			e.logger.Info("batch conflict on first PR", "prs", nums, "conflictPR", conflictPR, "requeued", rest)
		}
		return nil
	}

	prefix := append([]int(nil), nums[:idx]...)
	suffix := append([]int(nil), nums[idx:]...)
	e.enqueueWithState("retrying after staging conflict; testing a smaller batch", prefix, suffix)
	e.logger.Info("batch conflict split", "prs", nums, "conflictPR", conflictPR, "prefix", prefix, "suffix", suffix)
	return nil
}

func (e *Engine) skipLand(ctx context.Context, staged, current forge.PullRequest, reason string, hasRemainder bool, debugURL string, willRetry bool) {
	num := staged.Number
	e.logger.Info("PR skipped before merge", "pr", num, "reason", reason, "hasRemainder", hasRemainder)
	if current.State == "open" && !current.Merged {
		sha := current.Head.Sha
		if sha == "" {
			sha = staged.Head.Sha
		}
		e.notifyPR(ctx, num, sha, "error", "Skipped by merge queue", "shunt skipped this PR before landing because "+reason+". It will be re-tested if it remains queued.", debugURL, true, !willRetry)
	}
}

// bisectOrBounce: a size-1 failing batch bounces the culprit; a larger batch is
// split in half, with the first half tested next (the good half lands, the
// recursion isolates the bad PR(s)).
// holdTreeLeaf moves a terminal-gate node of a bisection tree out of the
// active list without publishing a source-PR decision; the decision is made
// in queue order by finalizeReadyTree once every descendant is terminal.
func (e *Engine) holdTreeLeaf(ctx context.Context, a *activeBatch, outcome string) bool {
	tree, ok := e.trees[a.runID]
	if !ok {
		return false
	}
	e.cleanupBatch(ctx, a)
	if outcome == "success" {
		tree.accepted = append(tree.accepted, a.prs...)
	}
	tree.held = append(tree.held, heldLeaf{batch: a, outcome: outcome})
	sort.Slice(tree.held, func(i, j int) bool {
		return tree.held[i].batch.prs[0].Number < tree.held[j].batch.prs[0].Number
	})
	e.recordTransition(Transition{
		Kind: "held", PRs: numbersOf(a.prs), StagingBranch: a.stagingBranch,
		RunID: a.runID, LineagePath: a.lineagePath, Reason: outcome,
	})
	return true
}

// treeHasUnresolvedNode reports whether any candidate under runID is still
// awaiting a gate result — staged in e.active, or waiting in e.pending. A tree
// is ready to finalize exactly when this is false. It is derived on every read
// rather than tracked as a counter so that no batch-removal path (a missing
// gate, a requeue, a supersede) can strand a root by forgetting to decrement.
func (e *Engine) treeHasUnresolvedNode(runID string) bool {
	return e.unresolvedNodeCount(runID) > 0
}

func (e *Engine) unresolvedNodeCount(runID string) int {
	n := 0
	for _, a := range e.active {
		if a.runID == runID {
			n++
		}
	}
	for _, node := range e.pending {
		if len(node) > 0 && e.lineageRunID[node[0]] == runID {
			n++
		}
	}
	return n
}

// finalizeReadyTree performs one ordered source-PR decision only after every
// descendant has produced terminal gate evidence.
func (e *Engine) finalizeReadyTree(ctx context.Context) (bool, error) {
	for runID, tree := range e.trees {
		if e.treeHasUnresolvedNode(runID) {
			continue
		}
		if tree.cursor == len(tree.held) {
			delete(e.trees, runID)
			delete(e.rootAnchor, runID)
			return true, nil
		}
		// An external base advance while a ready root has not yet landed
		// anything means every not-yet-performed decision used stale evidence.
		// A bounce does not move main, so if we have only bounced so far and
		// the anchor no longer matches, someone else landed a change; re-root
		// the unperformed suffix on current main. (An external advance
		// interleaved with our own landings is a narrower case left for later:
		// once a success has landed, main legitimately moved and the
		// acknowledged actions are irreversible facts.)
		if anchor := e.rootAnchor[runID]; anchor != "" && !treeFinalizedASuccess(tree) {
			head, err := e.fc.BranchHead(ctx, e.cfg.Owner, e.cfg.Repo, e.cfg.Base)
			if err != nil {
				return false, err
			}
			if head != anchor {
				e.reRootFinalizationSuffix(ctx, runID, tree)
				return true, nil
			}
		}
		leaf := tree.held[tree.cursor]
		if leaf.outcome == "success" {
			resolved, _, err := e.land(ctx, leaf.batch)
			if err != nil {
				return false, err
			}
			if !resolved {
				return true, nil
			}
		} else {
			staged := leaf.batch.prs[0]
			e.bounceFrom(ctx, leaf.batch.runID, leaf.batch.stagingBranch, staged.Number, staged.Head.Sha,
				fmt.Sprintf("merge-queue gate **%s**", leaf.outcome), gateOutcomeStatus(leaf.outcome), leaf.batch.debugURL)
		}
		tree.cursor++
		return true, nil
	}
	return false, nil
}

// treeFinalizedASuccess reports whether any leaf up to the finalization cursor
// was a success (and so legitimately moved the base branch).
func treeFinalizedASuccess(tree *bisectionTree) bool {
	for _, leaf := range tree.held[:tree.cursor] {
		if leaf.outcome == "success" {
			return true
		}
	}
	return false
}

// reRootFinalizationSuffix handles an external base advance during a ready
// root's finalization before any success has landed: the already-bounced
// leaves stay bounced (irreversible), and every held leaf from the cursor
// onward is re-queued as a fresh root on current main because its evidence
// used the now-stale anchor.
func (e *Engine) reRootFinalizationSuffix(ctx context.Context, runID string, tree *bisectionTree) {
	suffix := map[int]bool{}
	for _, leaf := range tree.held[tree.cursor:] {
		for _, pr := range leaf.batch.prs {
			suffix[pr.Number] = true
		}
		e.deleteStagingBranch(ctx, leaf.batch.stagingBranch)
		e.recordTransition(Transition{Kind: "bisected", StagingBranch: leaf.batch.stagingBranch, RunID: runID, LineagePath: leaf.batch.lineagePath})
	}
	nums := make([]int, 0, len(suffix))
	for n := range suffix {
		nums = append(nums, n)
	}
	sort.Ints(nums)

	delete(e.trees, runID)
	delete(e.rootAnchor, runID)
	e.recordTransition(Transition{
		Kind: "root_invalidated", PRs: nums, RunID: runID,
		Reason:  "base branch advanced during finalization",
		EventID: terminalEventID(runID, "root_invalidated", nums),
	})
	e.logger.Warn("root finalization aborted: base advanced before any decision landed; re-queuing the suffix",
		"runID", runID, "suffix", nums)
	if len(nums) > 0 {
		e.enqueueWithState("re-queued: base advanced during finalization", nums)
	}
}

func (e *Engine) bisectOrBounce(ctx context.Context, a *activeBatch, status string) (bool, error) {
	nums := numbersOf(a.prs)
	e.cleanupBatch(ctx, a)

	if len(nums) == 1 {
		if e.holdTreeLeaf(ctx, a, status) {
			return true, nil
		}
		bounced := e.bounceFrom(ctx, a.runID, a.stagingBranch, nums[0], a.prs[0].Head.Sha, fmt.Sprintf("merge-queue gate **%s**", status), gateOutcomeStatus(status), a.debugURL)
		return bounced, nil
	}
	mid := len(nums) / 2
	first := append([]int(nil), nums[:mid]...)
	second := append([]int(nil), nums[mid:]...)
	if _, ok := e.trees[a.runID]; !ok {
		e.trees[a.runID] = &bisectionTree{results: map[string]string{a.exactKey: status}}
	}
	e.markBisectOrigins(first[0], second[0])
	e.markLineage(a.runID, a.lineagePath, first, second)
	e.enqueueWithState("retrying after gate "+status+"; isolating the batch", first, second)
	e.logger.Info("batch failed; bisecting", "prs", nums, "status", status, "first", first, "second", second,
		"stagingBranch", a.stagingBranch, "firstPath", e.lineage[first[0]], "secondPath", e.lineage[second[0]])
	e.recordTransition(Transition{
		Kind: "bisected", PRs: nums, StagingBranch: a.stagingBranch,
		RunID: a.runID, LineagePath: a.lineagePath,
	})
	return true, nil
}

// markBisectOrigins records the first PR of each bisection sub-batch so that
// startNext can set phase = "bisecting" when it stages them.
func (e *Engine) markBisectOrigins(firstPRs ...int) {
	if e.bisectOrigins == nil {
		e.bisectOrigins = map[int]bool{}
	}
	for _, n := range firstPRs {
		e.bisectOrigins[n] = true
	}
}

func gateOutcomeStatus(status string) string {
	if status == "failure" {
		return "failure"
	}
	return "error"
}

func (e *Engine) bounce(ctx context.Context, runID string, num int, expectedHeadSHA, reason, statusState, debugURL string) bool {
	return e.bounceFrom(ctx, runID, "", num, expectedHeadSHA, reason, statusState, debugURL)
}

// bounceFrom is bounce with the batch's staging branch attached to the
// resulting Transition, when the caller has one (every caller does, except
// the missing-gate-retries path which has already torn its batch down).
func (e *Engine) bounceFrom(ctx context.Context, runID, stagingBranch string, num int, expectedHeadSHA, reason, statusState, debugURL string) bool {
	if pr, err := e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, num); err == nil && pr.State == "open" && !pr.Merged {
		if expectedHeadSHA != "" && pr.Head.Sha != expectedHeadSHA {
			e.enqueueWithState("requeued after PR head changed", []int{num})
			e.logger.Info("PR requeued instead of bounced after head changed", "pr", num, "oldHead", short(expectedHeadSHA), "newHead", short(pr.Head.Sha))
			return false
		}
		e.notifyPR(ctx, num, pr.Head.Sha, statusState, "Bounced from merge queue", "shunt rejected this PR from the merge queue: "+reason+".", debugURL, true, true)
	}
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.IncBounce(e.metricLabels())
	}
	e.observeQueueExit(num, "bounced")
	if _, err := e.fc.CancelAutomerge(ctx, e.cfg.Owner, e.cfg.Repo, num); err != nil {
		if !errors.Is(err, forge.ErrUnavailable) {
			e.logger.Warn("cancel auto-merge failed", "pr", num, "error", err)
		}
	}
	e.logger.Info("PR bounced", "pr", num, "reason", reason)
	t := Transition{Kind: "bounced", PRs: []int{num}, StagingBranch: stagingBranch, Reason: reason, RunID: runID}
	if runID != "" {
		// Only a root-scoped bounce gets a stable de-dup id. The rootless
		// staging-conflict bounce has no unique key (the same PR can conflict
		// in successive attempts), so it stays non-idempotent — acceptable
		// because it drives no merge counter.
		t.EventID = terminalEventID(runID, "bounced", []int{num})
	}
	e.recordTransition(t)
	return true
}

func (e *Engine) activeLimit() int {
	if e.cfg.BisectFanout > 0 {
		return e.cfg.BisectFanout
	}
	return 1
}

// frontierKeyVersion is bumped whenever the meaning of an exact key changes,
// so a key written by an older engine can never be mistaken for a current one.
// v1 was PR head SHAs only; v2 adds the base anchor and merge style.
const frontierKeyVersion = 2

// exactKey identifies one gate result.
// It includes the base anchor, merge style, and ordered PR heads.
// Results with different keys are independent.
func (e *Engine) exactKey(anchor string, prs []forge.PullRequest) string {
	parts := make([]string, 0, len(prs)+3)
	parts = append(parts, "v"+strconv.Itoa(frontierKeyVersion), anchor, e.cfg.MergeStyle)
	for _, pr := range prs {
		parts = append(parts, pr.Head.Sha)
	}
	return strings.Join(parts, "\x1f")
}

// ensureRootAnchor pins, once per root, the base branch's commit SHA. Every
// staged integration and exact key under that root is scoped to it.
func (e *Engine) ensureRootAnchor(ctx context.Context, runID string) (string, error) {
	if a := e.rootAnchor[runID]; a != "" {
		return a, nil
	}
	head, err := e.fc.BranchHead(ctx, e.cfg.Owner, e.cfg.Repo, e.cfg.Base)
	if err != nil {
		return "", fmt.Errorf("read base anchor for %q: %w", e.cfg.Base, err)
	}
	if e.rootAnchor == nil {
		e.rootAnchor = map[string]string{}
	}
	e.rootAnchor[runID] = head
	return head, nil
}

// unresolvedEarlier returns, in PR order, the candidate PRs of every active
// batch of runID whose first PR is left of beforePR. These are the earlier
// nodes a speculatively-staged node is racing ahead of; assuming they all pass
// gives the single most likely accumulator for the speculative integration.
func (e *Engine) unresolvedEarlier(runID string, beforePR int) []forge.PullRequest {
	var out []forge.PullRequest
	for _, a := range e.active {
		if a.runID == runID && len(a.prs) > 0 && a.prs[0].Number < beforePR {
			out = append(out, a.prs...)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out
}

// stagingBranchFor names a staging branch for a batch at a known point in the
// bisection tree. Callers pass the run this batch belongs to and its path.

func (e *Engine) stagingBranchFor(runID, path string) string {
	e.stagingSeq++
	return fmt.Sprintf("%s-%s-%s", e.cfg.StagingBranch, runID, path)
}

// lineageFor returns the run and path for a candidate about to be staged,
// consuming the entry recorded when it was split out of its parent. A
// candidate with no recorded lineage is a new root: it starts a new run.
func (e *Engine) lineageFor(cand []int) (string, string) {
	if len(cand) > 0 {
		if path, ok := e.lineage[cand[0]]; ok {
			runID := e.lineageRunID[cand[0]]
			delete(e.lineage, cand[0])
			delete(e.lineageRunID, cand[0])
			return runID, path
		}
	}
	// The run id must be unique per root attempt, not merely per instant: a
	// batch restacked because a PR head changed is a fresh root, and if it
	// reused the name the gate status for the previous branch would still be
	// attached to it. stagingSeq is monotonic over the engine's lifetime, so
	// pairing it with the clock keeps roots distinct even when two are staged
	// within the same nanosecond — which is exactly what happens under
	// BisectFanout > 1, and in tests with a frozen clock.
	e.runID = fmt.Sprintf("%d-%d", e.now().UnixNano(), e.stagingSeq)
	return e.runID, "r"
}

// markLineage records the path each half of a split will stage under, keyed by
// its first PR number so startNext can find it. Called before the halves are
// enqueued.
func (e *Engine) markLineage(runID, parentPath string, halves ...[]int) {
	if e.lineage == nil {
		e.lineage = map[int]string{}
	}
	if e.lineageRunID == nil {
		e.lineageRunID = map[int]string{}
	}
	if parentPath == "" {
		parentPath = "r"
	}
	for i, half := range halves {
		if len(half) == 0 {
			continue
		}
		e.lineage[half[0]] = fmt.Sprintf("%s%d", parentPath, i)
		e.lineageRunID[half[0]] = runID
	}
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

func (e *Engine) enqueueWithState(state string, cands ...[]int) {
	if e.requeueStates == nil {
		e.requeueStates = map[int]string{}
	}
	for _, cand := range cands {
		for _, num := range cand {
			e.requeueStates[num] = state
		}
	}
	e.enqueue(cands...)
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

// requeuedTreeNode intercepts a requeue/eviction that targets a live bisection
// node. Detaching a node from its tree without accounting for it would leave
// the root waiting forever on a slot nothing will fill. A candidate whose
// evidence is now in doubt invalidates the whole immutable root, so tear it
// down and re-queue every candidate as a fresh root. Returns true when it
// handled the batch.
func (e *Engine) requeuedTreeNode(ctx context.Context, a *activeBatch, reason, state string) bool {
	if a.runID == "" {
		return false
	}
	if _, ok := e.trees[a.runID]; !ok {
		return false
	}
	e.tearDownAndRequeueRoot(ctx, a.runID, reason, state)
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
		// A live bisection node must finish; do not evict it for a
		// newly-arrived earlier candidate.
		if _, inTree := e.trees[a.runID]; inTree && a.runID != "" {
			continue
		}
		if first := firstPR(a.prs); first > earliestPending && first > latest {
			idx = i
			latest = first
		}
	}
	if idx < 0 {
		return
	}
	a := e.active[idx]
	e.markBatchSuperseded(a, "an earlier queue entry took the staging slot")
	e.cleanupBatch(ctx, a)
	e.enqueueWithState("requeued; waiting for an earlier queue entry", numbersOf(a.prs))
	e.logger.Info("speculative batch requeued for earlier candidate", "prs", numbersOf(a.prs), "earlier", e.pending[0])
}

func (e *Engine) requeueStaleActive(ctx context.Context, a *activeBatch) {
	if e.requeuedTreeNode(ctx, a, "base branch advanced", "re-queued: base branch advanced mid-test") {
		return
	}
	e.markBatchSuperseded(a, "base branch advanced")
	e.cleanupBatch(ctx, a)
	e.enqueueWithState("requeued after base branch advanced", numbersOf(a.prs))
	e.logger.Info("stale speculative batch requeued after base advanced", "prs", numbersOf(a.prs))
}

func (e *Engine) requeueChangedActive(ctx context.Context, a *activeBatch) {
	if e.requeuedTreeNode(ctx, a, "a pinned candidate changed", "re-queued: a pinned candidate changed mid-test") {
		return
	}
	e.markBatchSuperseded(a, "a PR head changed")
	e.cleanupBatch(ctx, a)
	e.enqueueWithState("requeued after PR head changed", numbersOf(a.prs))
	e.logger.Info("active batch requeued after PR head changed", "prs", numbersOf(a.prs))
}

// markBatchSuperseded records that a staging attempt ended without a source
// decision because its candidates will be tested again. The transition lets
// callers retire the old staging row instead of leaving it as running.
// `superseded` means "this attempt was replaced", not "a PR failed".
func (e *Engine) markBatchSuperseded(a *activeBatch, reason string) {
	if a.stagingBranch == "" {
		return
	}
	e.recordTransition(Transition{
		Kind: "node_superseded", PRs: numbersOf(a.prs), StagingBranch: a.stagingBranch,
		RunID: a.runID, LineagePath: a.lineagePath, Reason: reason,
	})
}

// supersedeSpeculative discards a fanout-staged bisection node whose gate ran
// against an accumulator the resolved frontier no longer matches, and re-queues
// the same node — keeping its run id and lineage path — so it re-stages on the
// correct baseline. tree.open is untouched: the node still exists, unresolved.
// A matching exact-key result already in tree.results is reused by startNext.
func (e *Engine) supersedeSpeculative(ctx context.Context, a *activeBatch) {
	first := a.prs[0].Number
	e.recordTransition(Transition{
		Kind: "node_superseded", StagingBranch: a.stagingBranch, PRs: numbersOf(a.prs),
		RunID: a.runID, LineagePath: a.lineagePath,
	})
	e.cleanupBatch(ctx, a)
	if e.lineage == nil {
		e.lineage = map[int]string{}
	}
	if e.lineageRunID == nil {
		e.lineageRunID = map[int]string{}
	}
	e.lineage[first] = a.lineagePath
	e.lineageRunID[first] = a.runID
	e.enqueueWithState("re-staged on the resolved frontier baseline", numbersOf(a.prs))
	e.logger.Info("speculative batch superseded; re-staging on resolved baseline",
		"prs", numbersOf(a.prs), "path", a.lineagePath)
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.IncSpeculativeSuperseded(e.metricLabels())
	}
}

func (e *Engine) requeueStaleActives(ctx context.Context) {
	for _, a := range append([]*activeBatch(nil), e.active...) {
		if a.baseGen != e.baseGen {
			e.requeueStaleActive(ctx, a)
		}
	}
}

// invalidateAdvancedRoots checks each testing root against its base anchor.
// Shunt discards a root when the base changed. It requeues the root candidates.
// Shunt does not publish a source decision during this operation.
// A root in finalization has no unresolved nodes. Its base can move after a
// Shunt merge.
func (e *Engine) invalidateAdvancedRoots(ctx context.Context) (bool, error) {
	testing := map[string]bool{}
	for runID := range e.trees {
		if e.treeHasUnresolvedNode(runID) {
			testing[runID] = true
		}
	}
	for _, a := range e.active {
		if a.runID != "" && e.trees[a.runID] == nil {
			testing[a.runID] = true
		}
	}
	if len(testing) == 0 {
		return false, nil
	}
	head, err := e.fc.BranchHead(ctx, e.cfg.Owner, e.cfg.Repo, e.cfg.Base)
	if err != nil {
		return false, err
	}
	for runID := range testing {
		anchor := e.rootAnchor[runID]
		if anchor == "" || anchor == head {
			continue
		}
		e.tearDownAndRequeueRoot(ctx, runID, "base branch advanced",
			"re-queued: base branch advanced mid-test")
		return true, nil // one root per tick; re-observe on the next reconcile
	}
	return false, nil
}

// invalidateRootsOnCandidateChange checks every pinned candidate of a
// still-testing root — accepted, held, AND the ones currently staged in an
// active bisection node — for a head change, close, or merge. Any such change
// invalidates the immutable root's evidence; reRootPreservingPrefix carries the
// longest independently evidenced prefix into a successor, or the whole root is
// torn down. Covering the active nodes here is what keeps a mid-test push from
// falling through to requeueChangedActive, which would strand the tree.
//
// The GetPR sweep costs one call per distinct pinned candidate per reconcile.
// That is the design's "revalidate every pinned root candidate on each
// reconcile"; a root with no held leaves and one active node sweeps one PR.
func (e *Engine) invalidateRootsOnCandidateChange(ctx context.Context) (bool, error) {
	for runID := range e.trees {
		if !e.treeHasUnresolvedNode(runID) {
			continue // finalizing: acknowledged actions are irreversible facts
		}
		tree := e.trees[runID]
		pinned := map[int]string{}
		for _, pr := range tree.accepted {
			pinned[pr.Number] = pr.Head.Sha
		}
		for _, leaf := range tree.held {
			for _, pr := range leaf.batch.prs {
				pinned[pr.Number] = pr.Head.Sha
			}
		}
		for _, a := range e.active {
			if a.runID == runID {
				for _, pr := range a.prs {
					pinned[pr.Number] = pr.Head.Sha
				}
			}
		}
		// Lowest changed PR number wins: the preserved prefix is everything
		// strictly to its left.
		changed := 0
		for num, sha := range pinned {
			cur, err := e.fc.GetPR(ctx, e.cfg.Owner, e.cfg.Repo, num)
			if err != nil {
				return false, err
			}
			if cur.State != "open" || cur.Merged || cur.Head.Sha != sha {
				if changed == 0 || num < changed {
					changed = num
				}
			}
		}
		if changed == 0 {
			continue
		}
		e.logger.Warn("root invalidated: a pinned candidate changed",
			"runID", runID, "pr", changed)
		if !e.reRootPreservingPrefix(ctx, runID, changed) {
			e.tearDownAndRequeueRoot(ctx, runID, "a pinned candidate changed",
				"re-queued: a pinned candidate changed mid-test")
		}
		return true, nil
	}
	return false, nil
}

// reRootPreservingPrefix handles a pinned-candidate change by carrying the
// longest queue-order prefix of held decisions that is provably independent of
// the changed candidate into a successor root, and re-resolving only the suffix
// from the changed candidate onward. A held leaf is independent iff every PR in
// it is strictly left of `changed` — a successful group is indivisible, so a
// change inside one discards the whole group. Returns false when nothing can be
// preserved, leaving the caller to tear the whole root down.
//
// The successor keeps the predecessor's base anchor (a candidate change is not
// a base change) and its own held/accepted evidence, but starts a fresh
// outcome cache and owns finalization for the whole resulting queue. Inherited
// leaves keep their original run id for audit.
func (e *Engine) reRootPreservingPrefix(ctx context.Context, oldRunID string, changed int) bool {
	tree := e.trees[oldRunID]
	if tree == nil {
		return false
	}
	maxPR := func(prs []forge.PullRequest) int {
		m := 0
		for _, pr := range prs {
			if pr.Number > m {
				m = pr.Number
			}
		}
		return m
	}

	var preserved []heldLeaf
	for _, leaf := range tree.held { // tree.held is kept sorted by first PR
		if maxPR(leaf.batch.prs) < changed {
			preserved = append(preserved, leaf)
			continue
		}
		break
	}
	if len(preserved) == 0 {
		return false
	}

	// Suffix: every still-pinned PR from the changed candidate onward, plus
	// active batches and pending nodes of this root. resolve() drops the ones
	// that are no longer eligible (including a withdrawn `changed`).
	suffix := map[int]bool{}
	var preservedAccepted []forge.PullRequest
	for _, leaf := range preserved {
		if leaf.outcome == "success" {
			preservedAccepted = append(preservedAccepted, leaf.batch.prs...)
		}
	}
	for _, leaf := range tree.held[len(preserved):] {
		for _, pr := range leaf.batch.prs {
			suffix[pr.Number] = true
		}
		e.deleteStagingBranch(ctx, leaf.batch.stagingBranch)
		e.recordTransition(Transition{Kind: "bisected", StagingBranch: leaf.batch.stagingBranch, RunID: oldRunID, LineagePath: leaf.batch.lineagePath})
	}
	for _, a := range append([]*activeBatch(nil), e.active...) {
		if a.runID == oldRunID {
			for _, pr := range a.prs {
				suffix[pr.Number] = true
			}
			e.recordTransition(Transition{Kind: "bisected", StagingBranch: a.stagingBranch, RunID: oldRunID, LineagePath: a.lineagePath})
			e.cleanupBatch(ctx, a)
		}
	}
	var keptPending [][]int
	for _, node := range e.pending {
		if len(node) > 0 && e.lineageRunID[node[0]] == oldRunID {
			for _, n := range node {
				suffix[n] = true
			}
			delete(e.lineage, node[0])
			delete(e.lineageRunID, node[0])
			delete(e.bisectOrigins, node[0])
			continue
		}
		keptPending = append(keptPending, node)
	}
	e.pending = keptPending

	suffixNums := make([]int, 0, len(suffix))
	for n := range suffix {
		suffixNums = append(suffixNums, n)
	}
	sort.Ints(suffixNums)

	anchor := e.rootAnchor[oldRunID] // a candidate change does not move the base
	newRunID := fmt.Sprintf("%d-%d-s", e.now().UnixNano(), e.stagingSeq)
	delete(e.trees, oldRunID)
	delete(e.rootAnchor, oldRunID)
	e.trees[newRunID] = &bisectionTree{
		accepted: preservedAccepted,
		held:     preserved,
		results:  map[string]string{},
	}
	e.rootAnchor[newRunID] = anchor

	if e.lineage == nil {
		e.lineage = map[int]string{}
	}
	if e.lineageRunID == nil {
		e.lineageRunID = map[int]string{}
	}
	if len(suffixNums) > 0 {
		e.lineage[suffixNums[0]] = "r"
		e.lineageRunID[suffixNums[0]] = newRunID
		e.enqueueWithState("re-resolved suffix after a pinned candidate changed", suffixNums)
	}
	// With no suffix the successor has no unresolved node, so
	// treeHasUnresolvedNode reports it ready and finalizeReadyTree runs it out
	// on the next pass.

	e.recordTransition(Transition{
		Kind: "root_invalidated", PRs: suffixNums, RunID: oldRunID,
		Reason:  "a pinned candidate changed; prefix preserved",
		EventID: terminalEventID(oldRunID, "root_invalidated", suffixNums),
	})
	e.logger.Info("root re-rooted with preserved prefix",
		"oldRunID", oldRunID, "newRunID", newRunID,
		"preservedLeaves", len(preserved), "suffix", suffixNums)
	return true
}

func (e *Engine) tearDownAndRequeueRoot(ctx context.Context, runID, reason, requeueState string) {
	var nums []int
	seen := map[int]bool{}
	add := func(ns ...int) {
		for _, n := range ns {
			if !seen[n] {
				seen[n] = true
				nums = append(nums, n)
			}
		}
	}

	var toClean []*activeBatch
	for _, a := range e.active {
		if a.runID == runID {
			toClean = append(toClean, a)
			add(numbersOf(a.prs)...)
		}
	}
	if tree, ok := e.trees[runID]; ok {
		for _, pr := range tree.accepted {
			add(pr.Number)
		}
		for _, leaf := range tree.held {
			add(numbersOf(leaf.batch.prs)...)
			e.recordTransition(Transition{Kind: "bisected", StagingBranch: leaf.batch.stagingBranch, RunID: runID, LineagePath: leaf.batch.lineagePath})
			e.deleteStagingBranch(ctx, leaf.batch.stagingBranch)
		}
		delete(e.trees, runID)
	}
	var keptPending [][]int
	for _, node := range e.pending {
		if len(node) > 0 && e.lineageRunID[node[0]] == runID {
			add(node...)
			delete(e.lineage, node[0])
			delete(e.lineageRunID, node[0])
			delete(e.bisectOrigins, node[0])
			continue
		}
		keptPending = append(keptPending, node)
	}
	e.pending = keptPending

	for _, a := range toClean {
		e.recordTransition(Transition{Kind: "bisected", StagingBranch: a.stagingBranch, RunID: runID, LineagePath: a.lineagePath})
		e.cleanupBatch(ctx, a)
	}

	delete(e.rootAnchor, runID)

	sort.Ints(nums)
	e.logger.Warn("root invalidated; re-queuing its candidates as a fresh root",
		"runID", runID, "reason", reason, "candidates", nums)
	e.recordTransition(Transition{
		Kind: "root_invalidated", PRs: nums, RunID: runID, Reason: reason,
		EventID: terminalEventID(runID, "root_invalidated", nums),
	})
	if len(nums) > 0 {
		e.enqueueWithState(requeueState, nums)
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

// cleanupBatch removes a batch and its Shunt-owned staging branch.
func (e *Engine) cleanupBatch(ctx context.Context, a *activeBatch) {
	e.removeActive(a)
	e.deleteStagingBranch(ctx, a.stagingBranch)
}

func (e *Engine) deleteStagingBranch(ctx context.Context, branch string) {
	if !e.ownsStagingBranch(branch) {
		e.logger.Warn("not deleting a branch outside the staging namespace", "branch", branch)
		return
	}
	if err := e.fc.DeleteBranch(ctx, e.cfg.Owner, e.cfg.Repo, branch); err != nil {
		e.logger.Warn("failed to delete staging branch", "branch", branch, "error", err)
	}
}

func (e *Engine) ownsStagingBranch(branch string) bool {
	return e.cfg.StagingBranch != "" && strings.HasPrefix(branch, e.cfg.StagingBranch+"-")
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
	active := make([]metrics.ActiveBatchState, 0, len(e.active))
	for _, a := range e.active {
		active = append(active, metrics.ActiveBatchState{
			PRs:        numbersOf(a.prs),
			Phase:      a.phase,
			PhaseSince: a.phaseSince,
		})
	}
	cfg := &metrics.EffectiveConfig{
		ConfigSource: e.cfg.ConfigSource,
		Base:         e.cfg.Base,
		MergeStyle:   e.cfg.MergeStyle,
		MaxBatch:     e.cfg.MaxBatch,
		BatchLinger:  e.cfg.BatchLinger,
		BatchTarget:  e.cfg.BatchTarget,
		BisectFanout: e.cfg.BisectFanout,
	}
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.ObserveQueueStatus(e.metricLabels(), pending, active, e.lingerSince, cfg)
		e.cfg.Metrics.ObserveQueueAge(e.metricLabels(), e.oldestQueueAge())
	}
}

const queueCommentMarker = "<!-- shunt:queue-status -->"
const outcomeCommentMarker = "<!-- shunt:outcome -->"
const queueCommentFooter = "_shunt updates this sticky status comment. It may also post one separate durable outcome comment for a final, skipped, or recovery outcome._"

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
			states[num] = e.queuedState(num)
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
			prState := state
			if a.outcome == "" {
				if requeueState, ok := e.requeueStates[pr.Number]; ok {
					prState = requeueState + "; testing in active batch"
				}
			}
			states[pr.Number] = prState
		}
	}
	if len(states) == 0 {
		ready, err := e.readyNumbers(ctx)
		if err != nil {
			return nil, err
		}
		for _, num := range ready {
			state := e.queuedState(num)
			if e.cfg.BatchLinger > 0 && !e.lingerSince.IsZero() {
				state += "; waiting for batch linger window"
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
	fmt.Fprintln(&b, queueCommentFooter)
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
	fmt.Fprintln(&b, queueCommentFooter)
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
	fmt.Fprintln(&b, "- Outcome: terminal")
	if detail != "" {
		fmt.Fprintf(&b, "- Detail: %s\n", detail)
	}
	if debugURL != "" {
		fmt.Fprintf(&b, "- Debug: [staging run/commit](%s)\n", debugURL)
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, queueCommentFooter)
	return strings.TrimRight(b.String(), "\n")
}

func (e *Engine) notifyPR(ctx context.Context, num int, sha, statusState, title, detail, debugURL string, durableComment, terminal bool) {
	if statusState != "" && sha != "" {
		if err := e.fc.SetCommitStatus(ctx, e.cfg.Owner, e.cfg.Repo, sha, e.cfg.StatusCtx, statusState, statusDescription(title), debugURL); err != nil {
			if !errors.Is(err, forge.ErrUnavailable) {
				e.logger.Warn("set source PR status failed", "pr", num, "error", err)
			}
		}
	}
	body := terminalCommentBody(title, detail, debugURL)
	if e.cfg.QueueComments && terminal {
		sticky := e.queueCommentTerminalBody(title, detail, debugURL)
		if err := e.fc.UpsertComment(ctx, e.cfg.Owner, e.cfg.Repo, num, queueCommentMarker, e.cfg.BotUser, sticky); err != nil {
			if !errors.Is(err, forge.ErrUnavailable) {
				e.logger.Warn("update sticky PR comment failed", "pr", num, "error", err)
			}
		} else {
			e.queueComments[num] = sticky
			e.terminalQueueComments[num] = sticky
		}
	} else if !terminal {
		delete(e.terminalQueueComments, num)
	}
	if durableComment {
		if err := e.fc.UpsertComment(ctx, e.cfg.Owner, e.cfg.Repo, num, outcomeCommentMarker, e.cfg.BotUser, body); err != nil {
			if !errors.Is(err, forge.ErrUnavailable) {
				e.logger.Warn("update durable PR comment failed", "pr", num, "error", err)
			}
		}
	}
}

func (e *Engine) acknowledgeQueued(ctx context.Context, nums []int) {
	if !e.cfg.QueueComments {
		return
	}
	for i, num := range nums {
		body := e.queueCommentBody(queueCommentStatus{
			number:   num,
			position: i + 1,
			total:    len(nums),
			state:    "queued; acknowledged by shunt; waiting for batch linger window",
		})
		if e.queueComments[num] == body {
			continue
		}
		if err := e.fc.UpsertComment(ctx, e.cfg.Owner, e.cfg.Repo, num, queueCommentMarker, e.cfg.BotUser, body); err != nil {
			if !errors.Is(err, forge.ErrUnavailable) {
				e.logger.Warn("queue acknowledgement failed", "pr", num, "error", err)
			}
			continue
		}
		e.queueComments[num] = body
	}
}

func (e *Engine) queuedState(num int) string {
	if state, ok := e.requeueStates[num]; ok {
		return state
	}
	return "queued; acknowledged by shunt"
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
	delete(e.requeueStates, num)
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
	if e.cfg.Metrics != nil {
		e.cfg.Metrics.ObserveTimeInQueue(e.metricLabels(), outcome, age)
	}
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
		if !errors.Is(err, forge.ErrUnavailable) {
			e.logger.Warn("run target lookup failed", "sha", short(a.stagingSHA), "error", err)
		}
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
