// Package mq exposes shunt's merge queue engine as a usable API for external
// consumers.
//
// This is the public surface. All internal types (forge, gitops, checkpoint,
// lease, metrics) remain in internal/ — only what's needed here is exposed.
//
// Usage:
//
//	import "github.com/rbtr/shunt/mq"
//
//	engine := mq.New(&mq.Config{...}, forgeClient, stager)
//	err := engine.Reconcile(ctx)
package mq

import (
	"context"
	"github.com/rbtr/shunt/mq/checkpoint"
	"log/slog"
	"time"

	"github.com/rbtr/shunt/internal/engine"
	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
)

// Config holds the configuration for a single merge queue instance.
//
// Zero-valued fields get sensible defaults:
//
//	StatusCtx:     "merge-queue"
//	MergeStyle:    "merge"
//	StagingBranch: "mq/{base}/staging"
//	LeaseTTL:      45s
//	BisectFanout:  1
//	Logger:        slog.Default()
type Config struct {
	Owner         string        // repository owner
	Repo          string        // repository name
	Base          string        // target branch for merge queue
	StatusCtx     string        // commit-status context for gate signals
	MergeStyle    string        // merge style: "merge", "squash", or "rebase"
	StagingBranch string        // staging branch prefix (default: "mq/{base}/staging")
	InstanceURL   string        // forge instance URL
	PublicURL     string        // public URL for user-facing links (defaults to InstanceURL)
	MaxBatch      int           // initial rollup cap (0 = unlimited)
	BatchLinger   time.Duration // wait before starting batch (0 = no linger)
	BatchTarget   int           // batch size at which to stop lingering
	BisectFanout  int           // max concurrent bisection staging runs (0 = 1)
	QueueComments bool          // post sticky queue-status comments on PRs
	BotUser       string        // bot username for comments and status updates
	LeaseTTL      time.Duration // lease TTL for distributed locking (0 = 45s)
	Checkpoint    checkpoint.Store
	Logger        *slog.Logger // engine logger (defaults to slog.Default)
}

// Engine wraps shunt's merge queue engine with a simplified public API.
type Engine struct {
	e *engine.Engine
}

// PullRequest mirrors the Forgejo API shape for PRs.
type PullRequest struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	State  string `json:"state"`
	Merged bool   `json:"merged"`
	Head   struct {
		Sha string `json:"sha"`
		Ref string `json:"ref"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
}

// AutomergeState captures the automerge transition state from the PR timeline.
type AutomergeState struct {
	Scheduled bool
	UpdatedAt time.Time
}

// Review is the subset of a pull-request review the admission gate needs.
type Review struct {
	State     string `json:"state"` // APPROVED | REQUEST_CHANGES | REQUEST_REVIEW | COMMENT | PENDING
	Official  bool   `json:"official"`
	Dismissed bool   `json:"dismissed"`
	Stale     bool   `json:"stale"`
}

// BranchProtection is the subset of a base-branch protection rule the
// admission gate needs. Absent fields mean the rule does not gate on them.
type BranchProtection struct {
	RequiredApprovals             int64 `json:"required_approvals"`
	EnableApprovalsWhitelist      bool  `json:"enable_approvals_whitelist"`
	BlockOnRejectedReviews        bool  `json:"block_on_rejected_reviews"`
	BlockOnOfficialReviewRequests bool  `json:"block_on_official_review_requests"`
	IgnoreStaleApprovals          bool  `json:"ignore_stale_approvals"`
	DismissStaleApprovals         bool  `json:"dismiss_stale_approvals"`
}

// CommitStatus mirrors the Forgejo API shape for commit statuses.
type CommitStatus struct {
	ID          int64     `json:"id"`
	Status      string    `json:"status"`
	Description string    `json:"description"`
	Context     string    `json:"context"`
	CreatedAt   time.Time `json:"created_at"`
}

// ScheduleAutomergeResult captures the outcome of a ScheduleAutomerge attempt.
type ScheduleAutomergeResult struct {
	Eligible bool
}

// MergedRef represents a commit from a PR that needs to be included in the batch.
type MergedRef struct {
	PR  int
	Ref string // e.g. refs/pull/12/head
}

// New creates a merge queue engine.
//
// fc must implement ForgeClient (the forge client interface).
// st must implement Stager (the staging interface).
//
// Config fields may be zero-valued; defaults are applied internally.
func New(cfg *Config, fc ForgeClient, st Stager) *Engine {
	if cfg == nil {
		cfg = &Config{}
	}

	// Convert external interfaces to shunt's internal interfaces
	eCfg := engine.Config{
		Owner:         cfg.Owner,
		Repo:          cfg.Repo,
		Base:          cfg.Base,
		StatusCtx:     cfg.StatusCtx,
		MergeStyle:    cfg.MergeStyle,
		StagingBranch: cfg.StagingBranch,
		InstanceURL:   cfg.InstanceURL,
		PublicURL:     cfg.PublicURL,
		MaxBatch:      cfg.MaxBatch,
		BatchLinger:   cfg.BatchLinger,
		BatchTarget:   cfg.BatchTarget,
		BisectFanout:  cfg.BisectFanout,
		QueueComments: cfg.QueueComments,
		BotUser:       cfg.BotUser,
		Logger:        mqLogger(cfg),
		Checkpoint:    cfg.Checkpoint,
	}

	// Apply defaults
	if eCfg.StatusCtx == "" {
		eCfg.StatusCtx = "merge-queue"
	}
	if eCfg.MergeStyle == "" {
		eCfg.MergeStyle = "merge"
	}
	if eCfg.StagingBranch == "" {
		eCfg.StagingBranch = "mq/" + eCfg.Base + "/staging"
	}
	if eCfg.LeaseTTL == 0 {
		eCfg.LeaseTTL = 45 * time.Second
	}
	if eCfg.BisectFanout == 0 {
		eCfg.BisectFanout = 1
	}

	return &Engine{e: engine.New(eCfg, &adapter{fc}, &stagerAdapter{st})}
}

// mqLogger returns the configured engine logger, defaulting to slog.Default
// when the caller did not provide one.
func mqLogger(cfg *Config) *slog.Logger {
	if cfg != nil && cfg.Logger != nil {
		return cfg.Logger
	}
	return slog.Default()
}

// Reconcile advances the merge queue by one step.
//
// Safe to call on a fixed interval (e.g., every 10-30 seconds). The engine
// handles:
//   - Lease acquisition (distributed locking)
//   - Checkpoint loading/saving
//   - PR discovery and batch construction
//   - Staging branch creation and CI monitoring
//   - Auto-merge when gate passes
//   - Bisect on failure
//
// Returns nil on success (queue may be idle or resolved), or an error if a
// fatal operation (lease, checkpoint, forge) failed. Non-fatal errors (e.g.,
// staging conflict) are handled internally and do not surface here.
func (eng *Engine) Reconcile(ctx context.Context) error {
	return eng.e.Reconcile(ctx)
}

// Transition is a structured record of one merge-queue lifecycle event
// (a batch staged, its gate passed, it bisected, a PR bounced, or a PR
// landed) recorded during a Reconcile call. See LastTransitions.
type Transition = engine.Transition

// LastTransitions returns the transitions recorded during the most
// recently completed Reconcile call, so a caller can persist what actually
// happened without parsing log text. Safe to call after Reconcile returns;
// the slice is replaced, not appended to, on the next call.
func (eng *Engine) LastTransitions() []Transition {
	return eng.e.Transitions()
}

// AckTransitions tells the engine which terminal lifecycle records the caller
// has durably persisted, so it stops re-carrying them on the reconcile
// response. Call before Reconcile with the event ids from the last response
// the caller fully applied.
func (eng *Engine) AckTransitions(eventIDs []string) {
	eng.e.AckTransitions(eventIDs)
}

// ForgeClient is the interface that a forge client must satisfy to be used
// with mq.New.
type ForgeClient interface {
	ListOpenPRs(ctx context.Context, owner, repo, base string) ([]PullRequest, error)
	GetPR(ctx context.Context, owner, repo string, index int) (PullRequest, error)
	AutomergeState(ctx context.Context, owner, repo string, index int) (AutomergeState, error)
	ListReviews(ctx context.Context, owner, repo string, index int) ([]Review, error)
	ProtectedBranch(ctx context.Context, owner, repo, branch string) (BranchProtection, error)
	BranchHead(ctx context.Context, owner, repo, branch string) (string, error)
	LatestCommitStatus(ctx context.Context, owner, repo, sha, statusContext string) (CommitStatus, bool, error)
	RunStatus(ctx context.Context, owner, repo, sha, branch string) (string, error)
	RunTargetURL(ctx context.Context, owner, repo, sha, branch string) (string, error)
	SetCommitStatus(ctx context.Context, owner, repo, sha, context, state, desc, targetURL string) error
	ScheduleAutomerge(ctx context.Context, owner, repo string, index int, style, headSHA string) (ScheduleAutomergeResult, error)
	CancelAutomerge(ctx context.Context, owner, repo string, index int) (bool, error)
	DeleteBranch(ctx context.Context, owner, repo, branch string) error
	UpsertComment(ctx context.Context, owner, repo string, index int, marker, botUser, body string) error
}

// Stager is the interface that a staging implementation must satisfy to be used
// with mq.New.
//
// baseAnchor, when non-empty, is an immutable commit SHA the integration must
// be built on instead of the live tip of base. The merge queue pins it once
// per bisection root so main moving mid-tree cannot change what a staged
// result means.
type Stager interface {
	BuildStaging(ctx context.Context, base, baseAnchor, stagingBranch string, refs []MergedRef) (sha string, conflictPR int, err error)
}

// adapter wraps a mq.ForgeClient to satisfy engine.ForgeAPI.
type adapter struct {
	fc ForgeClient
}

func (a *adapter) ListOpenPRs(ctx context.Context, owner, repo, base string) ([]forge.PullRequest, error) {
	prs, err := a.fc.ListOpenPRs(ctx, owner, repo, base)
	if err != nil {
		return nil, err
	}
	out := make([]forge.PullRequest, len(prs))
	for i, p := range prs {
		out[i] = forge.PullRequest{
			Number: p.Number,
			Title:  p.Title,
			State:  p.State,
			Merged: p.Merged,
			Head: struct {
				Sha string `json:"sha"`
				Ref string `json:"ref"`
			}{Sha: p.Head.Sha, Ref: p.Head.Ref},
			Base: struct {
				Ref string `json:"ref"`
			}{Ref: p.Base.Ref},
		}
	}
	return out, nil
}

func (a *adapter) ListReviews(ctx context.Context, owner, repo string, index int) ([]forge.Review, error) {
	reviews, err := a.fc.ListReviews(ctx, owner, repo, index)
	if err != nil {
		return nil, err
	}
	out := make([]forge.Review, len(reviews))
	for i, r := range reviews {
		out[i] = forge.Review{State: r.State, Official: r.Official, Dismissed: r.Dismissed, Stale: r.Stale}
	}
	return out, nil
}

func (a *adapter) ProtectedBranch(ctx context.Context, owner, repo, branch string) (forge.BranchProtection, error) {
	p, err := a.fc.ProtectedBranch(ctx, owner, repo, branch)
	if err != nil {
		return forge.BranchProtection{}, err
	}
	return forge.BranchProtection{
		RequiredApprovals:             p.RequiredApprovals,
		EnableApprovalsWhitelist:      p.EnableApprovalsWhitelist,
		BlockOnRejectedReviews:        p.BlockOnRejectedReviews,
		BlockOnOfficialReviewRequests: p.BlockOnOfficialReviewRequests,
		IgnoreStaleApprovals:          p.IgnoreStaleApprovals,
		DismissStaleApprovals:         p.DismissStaleApprovals,
	}, nil
}

func (a *adapter) BranchHead(ctx context.Context, owner, repo, branch string) (string, error) {
	return a.fc.BranchHead(ctx, owner, repo, branch)
}

func (a *adapter) GetPR(ctx context.Context, owner, repo string, index int) (forge.PullRequest, error) {
	pr, err := a.fc.GetPR(ctx, owner, repo, index)
	if err != nil {
		return forge.PullRequest{}, err
	}
	return forge.PullRequest{
		Number: pr.Number,
		Title:  pr.Title,
		State:  pr.State,
		Merged: pr.Merged,
		Head: struct {
			Sha string `json:"sha"`
			Ref string `json:"ref"`
		}{Sha: pr.Head.Sha, Ref: pr.Head.Ref},
		Base: struct {
			Ref string `json:"ref"`
		}{Ref: pr.Base.Ref},
	}, nil
}

func (a *adapter) AutomergeState(ctx context.Context, owner, repo string, index int) (forge.AutomergeState, error) {
	state, err := a.fc.AutomergeState(ctx, owner, repo, index)
	if err != nil {
		return forge.AutomergeState{}, err
	}
	return forge.AutomergeState{
		Scheduled: state.Scheduled,
		UpdatedAt: state.UpdatedAt,
	}, nil
}

func (a *adapter) LatestCommitStatus(ctx context.Context, owner, repo, sha, statusContext string) (forge.CommitStatus, bool, error) {
	cs, found, err := a.fc.LatestCommitStatus(ctx, owner, repo, sha, statusContext)
	if err != nil {
		return forge.CommitStatus{}, false, err
	}
	return forge.CommitStatus{
		ID:          cs.ID,
		Status:      cs.Status,
		Description: cs.Description,
		Context:     cs.Context,
		CreatedAt:   cs.CreatedAt,
	}, found, nil
}

func (a *adapter) RunStatus(ctx context.Context, owner, repo, sha, branch string) (string, error) {
	return a.fc.RunStatus(ctx, owner, repo, sha, branch)
}

func (a *adapter) RunTargetURL(ctx context.Context, owner, repo, sha, branch string) (string, error) {
	return a.fc.RunTargetURL(ctx, owner, repo, sha, branch)
}

func (a *adapter) SetCommitStatus(ctx context.Context, owner, repo, sha, context, state, desc, targetURL string) error {
	return a.fc.SetCommitStatus(ctx, owner, repo, sha, context, state, desc, targetURL)
}

func (a *adapter) ScheduleAutomerge(ctx context.Context, owner, repo string, index int, style, headSHA string) (forge.ScheduleAutomergeResult, error) {
	result, err := a.fc.ScheduleAutomerge(ctx, owner, repo, index, style, headSHA)
	if err != nil {
		return forge.ScheduleAutomergeResult{}, err
	}
	return forge.ScheduleAutomergeResult{Eligible: result.Eligible}, nil
}

func (a *adapter) CancelAutomerge(ctx context.Context, owner, repo string, index int) (bool, error) {
	return a.fc.CancelAutomerge(ctx, owner, repo, index)
}

func (a *adapter) DeleteBranch(ctx context.Context, owner, repo, branch string) error {
	return a.fc.DeleteBranch(ctx, owner, repo, branch)
}

func (a *adapter) UpsertComment(ctx context.Context, owner, repo string, index int, marker, botUser, body string) error {
	return a.fc.UpsertComment(ctx, owner, repo, index, marker, botUser, body)
}

// stagerAdapter wraps a mq.Stager to satisfy engine.Stager.
type stagerAdapter struct {
	st Stager
}

func (s *stagerAdapter) BuildStaging(ctx context.Context, base, baseAnchor, stagingBranch string, refs []gitops.MergedRef) (string, int, error) {
	refs2 := make([]MergedRef, len(refs))
	for i, r := range refs {
		refs2[i] = MergedRef{PR: r.PR, Ref: r.Ref}
	}
	return s.st.BuildStaging(ctx, base, baseAnchor, stagingBranch, refs2)
}
