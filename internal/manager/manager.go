// Package manager runs one merge-queue engine per discovered (repo, base),
// discovering opt-in repos by topic and ensuring their branch protection.
package manager

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rbtr/shunt/internal/engine"
	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
	"github.com/rbtr/shunt/internal/metrics"
	"github.com/rbtr/shunt/internal/repoconfig"
)

// reconciler is the subset of *engine.Engine used by the manager.
// Separating it as an interface keeps the manager unit-testable with
// lightweight fakes.
type reconciler interface {
	Reconcile(ctx context.Context) error
}

type Config struct {
	Topic         string
	StatusCtx     string
	MergeStyle    string
	MaxBatch      int
	BatchLinger   time.Duration
	BatchTarget   int
	BisectFanout  int
	QueueComments bool
	WebhookURL    string
	WebhookSecret string
	InstanceURL   string
	PublicURL     string
	Token         string
	BotUser       string
	BotEmail      string
	Metrics       *metrics.Collector
	Checkpoint    engine.CheckpointStore
	Lease         engine.QueueLease
	LeaseHolderID string
	LeaseTTL      time.Duration
	Logger        *slog.Logger
	EngineLogger  *slog.Logger
	// MaxConcurrentReconciles caps how many engines may be reconciled
	// concurrently in a single Tick pass. Values ≤ 0 or 1 preserve today's
	// serial behaviour exactly. Values > 1 enable cross-repo parallelism.
	MaxConcurrentReconciles int
}

type Manager struct {
	fc            *forge.Client
	cfg           Config
	logger        *slog.Logger
	engineLogger  *slog.Logger
	engines       map[string]*managedEngine
	maxConcurrent int
}

// managedEngine couples a reconcile engine to its per-instance lifecycle so
// that Tick and Refresh can safely coordinate:
//
//   - lifetimeCtx / stop: Refresh cancels this context before replacing or
//     removing the engine, giving any in-flight Reconcile a prompt exit signal.
//   - mu: held for the duration of every Reconcile call.  Tick uses TryLock to
//     enforce the at-most-one-concurrent-call-per-engine invariant; Refresh uses
//     Lock to drain any in-flight call before touching the engines map.
type managedEngine struct {
	engine      reconciler
	cfg         engine.Config
	lifetimeCtx context.Context
	stop        context.CancelFunc // cancels lifetimeCtx
	mu          sync.Mutex
}

func newManagedEngine(eng reconciler, cfg engine.Config) *managedEngine {
	ctx, stop := context.WithCancel(context.Background())
	return &managedEngine{engine: eng, cfg: cfg, lifetimeCtx: ctx, stop: stop}
}

// drain cancels the engine's lifetime context (giving any in-flight Reconcile
// a prompt exit signal) and then waits for the call to return.
// Must be called before replacing or removing this engine from the map.
func (me *managedEngine) drain() {
	me.stop()      // signal Reconcile to exit at its next ctx.Err() check
	me.mu.Lock()   // block until any in-flight Reconcile has returned
	me.mu.Unlock() //nolint:staticcheck
}

func New(fc *forge.Client, cfg Config) *Manager {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default().With("component", "manager")
	}
	engineLogger := cfg.EngineLogger
	if engineLogger == nil {
		engineLogger = slog.Default().With("component", "engine")
	}
	maxConcurrent := cfg.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	return &Manager{
		fc: fc, cfg: cfg, logger: logger, engineLogger: engineLogger,
		engines: map[string]*managedEngine{}, maxConcurrent: maxConcurrent,
	}
}

func keyOf(owner, repo, base string) string { return owner + "/" + repo + "@" + base }

// Refresh discovers topic repos and registers an engine per configured
// (repo, base), ensuring branch protection on first sight.
func (m *Manager) Refresh(ctx context.Context) error {
	repos, err := m.fc.SearchReposByTopic(ctx, m.cfg.Topic)
	if err != nil {
		if errors.Is(err, forge.ErrUnavailable) {
			return nil
		}
		return err
	}
	seen := make(map[string]bool, len(repos))
	for _, r := range repos {
		settings, configSource, err := m.repoSettings(ctx, r)
		if err != nil {
			if errors.Is(err, forge.ErrUnavailable) {
				m.keepExistingRepo(seen, r.Owner, r.Name)
				continue
			}
			m.logger.Warn("repo config skipped", "owner", r.Owner, "repo", r.Name, "path", repoconfig.FileName, "error", err)
			m.keepExistingRepo(seen, r.Owner, r.Name)
			continue
		}
		k := keyOf(r.Owner, r.Name, settings.Base)
		seen[k] = true
		cfg := m.engineConfig(r, settings, configSource)
		if existing, ok := m.engines[k]; ok && sameEngineConfig(existing.cfg, cfg) {
			continue
		}
		queueLogger := m.logger.With("owner", r.Owner, "repo", r.Name, "base", settings.Base)
		if changed, err := m.fc.EnsureBranchProtection(ctx, r.Owner, r.Name, settings.Base, settings.StatusCtx, m.cfg.BotUser); err != nil {
			if !errors.Is(err, forge.ErrUnavailable) {
				queueLogger.Warn("ensure branch protection failed", "error", err)
			}
			continue
		} else if changed {
			queueLogger.Info("branch protection configured")
		}
		if changed, err := m.fc.EnsureStagingBranchProtection(ctx, r.Owner, r.Name, settings.Base, m.cfg.BotUser); err != nil {
			if !errors.Is(err, forge.ErrUnavailable) {
				queueLogger.Warn("ensure staging branch protection failed", "error", err)
			}
			continue
		} else if changed {
			queueLogger.Info("staging branch protection configured")
		}
		if changed, err := m.fc.EnsureWebhook(ctx, r.Owner, r.Name, m.cfg.WebhookURL, m.cfg.WebhookSecret); err != nil {
			if !errors.Is(err, forge.ErrUnavailable) {
				queueLogger.Warn("ensure webhook failed", "error", err)
			}
			continue
		} else if changed {
			queueLogger.Info("webhook configured")
		}
		st := gitops.NewStager(cloneURL(m.cfg.InstanceURL, r.Owner, r.Name), m.cfg.BotUser, m.cfg.Token, m.cfg.BotUser, m.cfg.BotEmail)
		// Drain before replacement: cancel any in-flight Reconcile on the old engine
		// and wait for it to finish so stale checkpoint writes cannot race with the
		// new engine loading state under the same (owner, repo, base) key.
		if old, ok := m.engines[k]; ok {
			old.drain()
		}
		m.engines[k] = newManagedEngine(engine.New(cfg, m.fc, st), cfg)
		queueLogger.Info("repo managed")
	}
	for k := range m.engines {
		if !seen[k] {
			m.engines[k].drain() // wait for any in-flight Reconcile before removing
			delete(m.engines, k)
			m.cfg.Metrics.ForgetQueue(labelsOfKey(k))
			m.logger.Info("repo no longer managed", "queue", k)
		}
	}
	return nil
}

// Tick reconciles every managed queue, bounded to m.maxConcurrent concurrent
// engines at a time. The default (maxConcurrent == 1) is serial and preserves
// exactly the same behaviour as the original single-goroutine loop.
//
// Invariants:
//   - At most one Reconcile call is ever in flight for a given engine at once.
//     If a previous tick's Reconcile is still running (TryLock fails), this tick
//     is skipped for that engine with a warning; this means SHUNT_POLL_INTERVAL
//     is too short relative to the actual reconcile duration.
//   - Tick blocks until every goroutine it started has returned, so the caller
//     (the reconcile loop) can safely call Refresh immediately afterwards.
func (m *Manager) Tick(ctx context.Context) {
	var eg errgroup.Group
	eg.SetLimit(m.maxConcurrent)
	for k, e := range m.engines {
		k, e := k, e
		eg.Go(func() error {
			// At-most-one-concurrent-Reconcile-per-engine invariant. A failure
			// here indicates SHUNT_POLL_INTERVAL is shorter than this engine's
			// actual reconcile duration, or Tick is being called concurrently
			// (unsupported).
			if !e.mu.TryLock() {
				m.logger.Warn("engine tick skipped: previous reconcile still in flight", "queue", k)
				return nil
			}
			defer e.mu.Unlock()
			// Derive a context that is cancelled by either process shutdown (ctx)
			// or this engine's replacement/removal (e.lifetimeCtx), whichever
			// comes first. context.AfterFunc is non-blocking and cleans up after
			// itself once the stop function returned by AfterFunc is called.
			rctx, rcancel := context.WithCancel(ctx)
			defer rcancel()
			stop := context.AfterFunc(e.lifetimeCtx, rcancel)
			defer stop()
			if err := e.engine.Reconcile(rctx); err != nil {
				if !errors.Is(err, forge.ErrUnavailable) {
					m.logger.Error("reconcile failed", "queue", k, "error", err)
				}
			}
			return nil
		})
	}
	_ = eg.Wait()
}

func (m *Manager) repoSettings(ctx context.Context, r forge.RepoRef) (repoconfig.Settings, string, error) {
	base := r.DefaultBranch
	if base == "" {
		base = "main"
	}
	defaults := repoconfig.Settings{
		Base:         base,
		StatusCtx:    m.cfg.StatusCtx,
		MergeStyle:   m.cfg.MergeStyle,
		MaxBatch:     m.cfg.MaxBatch,
		BatchLinger:  m.cfg.BatchLinger,
		BatchTarget:  m.cfg.BatchTarget,
		BisectFanout: m.cfg.BisectFanout,
	}
	data, err := m.fc.ReadFile(ctx, r.Owner, r.Name, base, repoconfig.FileName)
	if errors.Is(err, forge.ErrNotFound) {
		return defaults, "default", nil
	}
	if err != nil {
		return repoconfig.Settings{}, "", err
	}
	settings, err := repoconfig.Apply(data, defaults)
	if err != nil {
		return repoconfig.Settings{}, "", err
	}
	return settings, "repo", nil
}

func (m *Manager) engineConfig(r forge.RepoRef, settings repoconfig.Settings, configSource string) engine.Config {
	return engine.Config{
		Owner:         r.Owner,
		Repo:          r.Name,
		Base:          settings.Base,
		StatusCtx:     settings.StatusCtx,
		MergeStyle:    settings.MergeStyle,
		StagingBranch: "mq/" + settings.Base + "/staging",
		InstanceURL:   m.cfg.InstanceURL,
		PublicURL:     m.cfg.PublicURL,
		MaxBatch:      settings.MaxBatch,
		BatchLinger:   settings.BatchLinger,
		BatchTarget:   settings.BatchTarget,
		BisectFanout:  settings.BisectFanout,
		QueueComments: m.cfg.QueueComments,
		BotUser:       m.cfg.BotUser,
		ConfigSource:  configSource,
		Metrics:       m.cfg.Metrics,
		Checkpoint:    m.cfg.Checkpoint,
		Lease:         m.cfg.Lease,
		LeaseHolderID: m.cfg.LeaseHolderID,
		LeaseTTL:      m.cfg.LeaseTTL,
		Logger:        m.engineLogger,
	}
}

func sameEngineConfig(a, b engine.Config) bool {
	a.Logger = nil
	b.Logger = nil
	return a == b
}

func (m *Manager) keepExistingRepo(seen map[string]bool, owner, repo string) {
	prefix := owner + "/" + repo + "@"
	for k := range m.engines {
		if strings.HasPrefix(k, prefix) {
			seen[k] = true
		}
	}
}

func cloneURL(instance, owner, repo string) string {
	return strings.TrimRight(instance, "/") + "/" + owner + "/" + repo + ".git"
}

func labelsOfKey(k string) metrics.Labels {
	ownerRepo, base, ok := strings.Cut(k, "@")
	if !ok {
		return metrics.Labels{}
	}
	owner, repo, ok := strings.Cut(ownerRepo, "/")
	if !ok {
		return metrics.Labels{Repo: ownerRepo, Base: base}
	}
	return metrics.Labels{Owner: owner, Repo: repo, Base: base}
}
