// Package manager runs one merge-queue engine per discovered (repo, base),
// discovering opt-in repos by topic and ensuring their branch protection.
package manager

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/rbtr/shunt/internal/engine"
	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
	"github.com/rbtr/shunt/internal/metrics"
	"github.com/rbtr/shunt/internal/repoconfig"
)

type Config struct {
	Topic              string
	StatusCtx          string
	MergeStyle         string
	MaxBatch           int
	BatchLinger        time.Duration
	BatchTarget        int
	InitialBatchFanout int
	BisectFanout       int
	QueueComments      bool
	WebhookURL         string
	WebhookSecret      string
	InstanceURL        string
	PublicURL          string
	Token              string
	BotUser            string
	BotEmail           string
	Metrics            *metrics.Collector
}

type Manager struct {
	fc      *forge.Client
	cfg     Config
	engines map[string]*managedEngine
}

type managedEngine struct {
	engine *engine.Engine
	cfg    engine.Config
}

func New(fc *forge.Client, cfg Config) *Manager {
	return &Manager{fc: fc, cfg: cfg, engines: map[string]*managedEngine{}}
}

func keyOf(owner, repo, base string) string { return owner + "/" + repo + "@" + base }

// Refresh discovers topic repos and registers an engine per configured
// (repo, base), ensuring branch protection on first sight.
func (m *Manager) Refresh() error {
	repos, err := m.fc.SearchReposByTopic(m.cfg.Topic)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(repos))
	for _, r := range repos {
		settings, err := m.repoSettings(r)
		if err != nil {
			log.Printf("manager: %s/%s: %s: %v", r.Owner, r.Name, repoconfig.FileName, err)
			m.keepExistingRepo(seen, r.Owner, r.Name)
			continue
		}
		k := keyOf(r.Owner, r.Name, settings.Base)
		seen[k] = true
		cfg := m.engineConfig(r, settings)
		if existing, ok := m.engines[k]; ok && existing.cfg == cfg {
			continue
		}
		if changed, err := m.fc.EnsureBranchProtection(r.Owner, r.Name, settings.Base, settings.StatusCtx, m.cfg.BotUser); err != nil {
			log.Printf("manager: %s: ensure protection: %v", k, err)
			continue
		} else if changed {
			log.Printf("manager: %s: configured branch protection", k)
		}
		if changed, err := m.fc.EnsureWebhook(r.Owner, r.Name, m.cfg.WebhookURL, m.cfg.WebhookSecret); err != nil {
			log.Printf("manager: %s: ensure webhook: %v", k, err)
			continue
		} else if changed {
			log.Printf("manager: %s: configured webhook", k)
		}
		if deleted, err := m.fc.PruneStagingBranches(r.Owner, r.Name, settings.Base); err != nil {
			log.Printf("manager: %s: staging branch gc: %v", k, err)
		} else if len(deleted) > 0 {
			log.Printf("manager: %s: deleted stale staging branches: %s", k, strings.Join(deleted, ", "))
		}
		st := gitops.NewStager(cloneURL(m.cfg.InstanceURL, r.Owner, r.Name), m.cfg.BotUser, m.cfg.Token, m.cfg.BotUser, m.cfg.BotEmail)
		m.engines[k] = &managedEngine{engine: engine.New(cfg, m.fc, st), cfg: cfg}
		log.Printf("manager: managing %s", k)
	}
	for k := range m.engines {
		if !seen[k] {
			delete(m.engines, k)
			m.cfg.Metrics.ForgetQueue(labelsOfKey(k))
			log.Printf("manager: stopped managing %s (topic removed or repo archived)", k)
		}
	}
	return nil
}

// Tick reconciles every managed queue once.
func (m *Manager) Tick() {
	for k, e := range m.engines {
		if err := e.engine.Reconcile(); err != nil {
			log.Printf("manager: %s: reconcile: %v", k, err)
		}
	}
}

func (m *Manager) repoSettings(r forge.RepoRef) (repoconfig.Settings, error) {
	base := r.DefaultBranch
	if base == "" {
		base = "main"
	}
	defaults := repoconfig.Settings{
		Base:               base,
		StatusCtx:          m.cfg.StatusCtx,
		MergeStyle:         m.cfg.MergeStyle,
		MaxBatch:           m.cfg.MaxBatch,
		BatchLinger:        m.cfg.BatchLinger,
		BatchTarget:        m.cfg.BatchTarget,
		InitialBatchFanout: m.cfg.InitialBatchFanout,
		BisectFanout:       m.cfg.BisectFanout,
	}
	data, err := m.fc.ReadFile(r.Owner, r.Name, base, repoconfig.FileName)
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
		Owner:              r.Owner,
		Repo:               r.Name,
		Base:               settings.Base,
		StatusCtx:          settings.StatusCtx,
		MergeStyle:         settings.MergeStyle,
		StagingBranch:      "mq/" + settings.Base + "/staging",
		InstanceURL:        m.cfg.InstanceURL,
		PublicURL:          m.cfg.PublicURL,
		MaxBatch:           settings.MaxBatch,
		BatchLinger:        settings.BatchLinger,
		BatchTarget:        settings.BatchTarget,
		InitialBatchFanout: settings.InitialBatchFanout,
		BisectFanout:       settings.BisectFanout,
		QueueComments:      m.cfg.QueueComments,
		BotUser:            m.cfg.BotUser,
		Metrics:            m.cfg.Metrics,
	}
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
