// Package manager runs one merge-queue engine per discovered (repo, base),
// discovering opt-in repos by topic and ensuring their branch protection.
package manager

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/rbtr/shunt/internal/engine"
	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
	"github.com/rbtr/shunt/internal/metrics"
	"github.com/rbtr/shunt/internal/repoconfig"
)

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
}

type Manager struct {
	fc           *forge.Client
	cfg          Config
	logger       *slog.Logger
	engineLogger *slog.Logger
	engines      map[string]*managedEngine
}

type managedEngine struct {
	engine *engine.Engine
	cfg    engine.Config
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
	return &Manager{fc: fc, cfg: cfg, logger: logger, engineLogger: engineLogger, engines: map[string]*managedEngine{}}
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
		settings, err := m.repoSettings(ctx, r)
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
		cfg := m.engineConfig(r, settings)
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
		m.engines[k] = &managedEngine{engine: engine.New(cfg, m.fc, st), cfg: cfg}
		queueLogger.Info("repo managed")
	}
	for k := range m.engines {
		if !seen[k] {
			delete(m.engines, k)
			m.cfg.Metrics.ForgetQueue(labelsOfKey(k))
			m.logger.Info("repo no longer managed", "queue", k)
		}
	}
	return nil
}

// Tick reconciles every managed queue once.
func (m *Manager) Tick(ctx context.Context) {
	for k, e := range m.engines {
		if err := e.engine.Reconcile(ctx); err != nil {
			if !errors.Is(err, forge.ErrUnavailable) {
				m.logger.Error("reconcile failed", "queue", k, "error", err)
			}
		}
	}
}

func (m *Manager) repoSettings(ctx context.Context, r forge.RepoRef) (repoconfig.Settings, error) {
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
		return defaults, nil
	}
	if err != nil {
		return repoconfig.Settings{}, err
	}
	return repoconfig.Apply(data, defaults)
}

func (m *Manager) engineConfig(r forge.RepoRef, settings repoconfig.Settings) engine.Config {
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
