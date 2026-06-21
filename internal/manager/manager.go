// Package manager runs one merge-queue engine per discovered (repo, base),
// discovering opt-in repos by topic and ensuring their branch protection.
package manager

import (
	"log"
	"strings"

	"github.com/rbtr/shunt/internal/engine"
	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
)

type Config struct {
	Topic       string
	StatusCtx   string
	MergeStyle  string
	MaxBatch    int
	InstanceURL string
	PublicURL   string
	Token       string
	BotUser     string
	BotEmail    string
}

type Manager struct {
	fc      *forge.Client
	cfg     Config
	engines map[string]*engine.Engine
}

func New(fc *forge.Client, cfg Config) *Manager {
	return &Manager{fc: fc, cfg: cfg, engines: map[string]*engine.Engine{}}
}

func keyOf(owner, repo, base string) string { return owner + "/" + repo + "@" + base }

// Refresh discovers topic repos and registers an engine per (repo, default
// branch), ensuring branch protection on first sight.
func (m *Manager) Refresh() error {
	repos, err := m.fc.SearchReposByTopic(m.cfg.Topic)
	if err != nil {
		return err
	}
	for _, r := range repos {
		base := r.DefaultBranch
		if base == "" {
			base = "main"
		}
		k := keyOf(r.Owner, r.Name, base)
		if _, ok := m.engines[k]; ok {
			continue
		}
		if changed, err := m.fc.EnsureBranchProtection(r.Owner, r.Name, base, m.cfg.StatusCtx, m.cfg.BotUser); err != nil {
			log.Printf("manager: %s: ensure protection: %v", k, err)
			continue
		} else if changed {
			log.Printf("manager: %s: configured branch protection", k)
		}
		st := gitops.NewStager(cloneURL(m.cfg.InstanceURL, r.Owner, r.Name), m.cfg.BotUser, m.cfg.Token, m.cfg.BotUser, m.cfg.BotEmail)
		m.engines[k] = engine.New(engine.Config{
			Owner:         r.Owner,
			Repo:          r.Name,
			Base:          base,
			StatusCtx:     m.cfg.StatusCtx,
			MergeStyle:    m.cfg.MergeStyle,
			StagingBranch: "mq/" + base + "/staging",
			InstanceURL:   m.cfg.InstanceURL,
			PublicURL:     m.cfg.PublicURL,
			MaxBatch:      m.cfg.MaxBatch,
		}, m.fc, st)
		log.Printf("manager: managing %s", k)
	}
	return nil
}

// Tick reconciles every managed queue once.
func (m *Manager) Tick() {
	for k, e := range m.engines {
		if err := e.Reconcile(); err != nil {
			log.Printf("manager: %s: reconcile: %v", k, err)
		}
	}
}

func cloneURL(instance, owner, repo string) string {
	return strings.TrimRight(instance, "/") + "/" + owner + "/" + repo + ".git"
}
