package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/metrics"
)

func TestRefreshAppliesRepoConfigOverrides(t *testing.T) {
	var rawRef, protectedBranch, protectedContext string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() == "/api/v1/repos/o/r/branch_protections/mq%2Ftrunk%2Fstaging%2A" {
			writeSatisfiedStagingProtection(t, w, r)
			return
		}
		switch r.URL.Path {
		case "/api/v1/repos/search":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{
				"name":           "r",
				"default_branch": "main",
				"owner":          map[string]string{"login": "o"},
			}}})
		case "/api/v1/repos/o/r/raw/.shunt.yml":
			rawRef = r.URL.Query().Get("ref")
			_, _ = w.Write([]byte(`
base: trunk
status_context: shunt
merge_style: squash
max_batch: 4
batch_linger: 20s
batch_target: 3
bisect_fanout: 2
`))
		case "/api/v1/repos/o/r/branch_protections/trunk":
			if r.Method != http.MethodGet {
				t.Fatalf("branch protection method = %s, want GET", r.Method)
			}
			protectedBranch = "trunk"
			_ = json.NewEncoder(w).Encode(forge.BranchProtection{
				EnableStatusCheck:      true,
				StatusCheckContexts:    []string{"shunt"},
				EnablePush:             true,
				EnablePushWhitelist:    true,
				PushWhitelistUsernames: []string{},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		if r.URL.Path == "/api/v1/repos/o/r/branch_protections/trunk" {
			protectedContext = "shunt"
		}
	}))
	defer srv.Close()

	m := New(forge.New(srv.URL, "token"), Config{
		Topic:        "merge-queue",
		StatusCtx:    "merge-queue",
		MergeStyle:   "merge",
		MaxBatch:     0,
		BatchLinger:  time.Second,
		BatchTarget:  0,
		BisectFanout: 1,
		InstanceURL:  srv.URL,
		PublicURL:    srv.URL,
		Token:        "token",
		BotUser:      "bot",
		BotEmail:     "bot@example.invalid",
		Metrics:      metrics.New(),
	})
	if err := m.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if rawRef != "main" {
		t.Fatalf("raw ref = %q, want main", rawRef)
	}
	if protectedBranch != "trunk" || protectedContext != "shunt" {
		t.Fatalf("protected branch/context = %q/%q, want trunk/shunt", protectedBranch, protectedContext)
	}
	got, ok := m.engines["o/r@trunk"]
	if !ok {
		t.Fatalf("missing engine for overridden base; got keys %#v", m.engines)
	}
	if got.cfg.StatusCtx != "shunt" || got.cfg.MergeStyle != "squash" || got.cfg.MaxBatch != 4 ||
		got.cfg.BatchLinger != 20*time.Second || got.cfg.BatchTarget != 3 || got.cfg.BisectFanout != 2 {
		t.Fatalf("engine config = %+v", got.cfg)
	}
	firstEngine := got.engine
	if err := m.Refresh(context.Background()); err != nil {
		t.Fatalf("second Refresh: %v", err)
	}
	if m.engines["o/r@trunk"].engine != firstEngine {
		t.Fatal("refresh recreated unchanged engine")
	}
}

func TestRefreshUsesDefaultsWhenRepoConfigMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() == "/api/v1/repos/o/r/branch_protections/mq%2Fmain%2Fstaging%2A" {
			writeSatisfiedStagingProtection(t, w, r)
			return
		}
		switch r.URL.Path {
		case "/api/v1/repos/search":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{
				"name":           "r",
				"default_branch": "main",
				"owner":          map[string]string{"login": "o"},
			}}})
		case "/api/v1/repos/o/r/raw/.shunt.yml":
			http.NotFound(w, r)
		case "/api/v1/repos/o/r/branch_protections/main":
			if r.Method != http.MethodGet {
				t.Fatalf("branch protection method = %s, want GET", r.Method)
			}
			_ = json.NewEncoder(w).Encode(forge.BranchProtection{
				EnableStatusCheck:      true,
				StatusCheckContexts:    []string{"merge-queue"},
				EnablePush:             true,
				EnablePushWhitelist:    true,
				PushWhitelistUsernames: []string{},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	m := New(forge.New(srv.URL, "token"), Config{
		Topic:        "merge-queue",
		StatusCtx:    "merge-queue",
		MergeStyle:   "merge",
		BisectFanout: 1,
		InstanceURL:  srv.URL,
		PublicURL:    srv.URL,
		Token:        "token",
		BotUser:      "bot",
		BotEmail:     "bot@example.invalid",
		Metrics:      metrics.New(),
	})
	if err := m.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	got, ok := m.engines["o/r@main"]
	if !ok {
		t.Fatalf("missing engine for default base; got keys %#v", m.engines)
	}
	if got.cfg.StatusCtx != "merge-queue" || got.cfg.MergeStyle != "merge" || got.cfg.BisectFanout != 1 {
		t.Fatalf("engine config = %+v", got.cfg)
	}
}

func TestRefreshEnsuresWebhookWhenConfigured(t *testing.T) {
	var createdHook map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() == "/api/v1/repos/o/r/branch_protections/mq%2Fmain%2Fstaging%2A" {
			writeSatisfiedStagingProtection(t, w, r)
			return
		}
		switch r.URL.Path {
		case "/api/v1/repos/search":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{
				"name":           "r",
				"default_branch": "main",
				"owner":          map[string]string{"login": "o"},
			}}})
		case "/api/v1/repos/o/r/raw/.shunt.yml":
			http.NotFound(w, r)
		case "/api/v1/repos/o/r/branch_protections/main":
			_ = json.NewEncoder(w).Encode(forge.BranchProtection{
				EnableStatusCheck:      true,
				StatusCheckContexts:    []string{"merge-queue"},
				EnablePush:             true,
				EnablePushWhitelist:    true,
				PushWhitelistUsernames: []string{},
			})
		case "/api/v1/repos/o/r/hooks":
			switch r.Method {
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode([]forge.Hook{})
			case http.MethodPost:
				if err := json.NewDecoder(r.Body).Decode(&createdHook); err != nil {
					t.Fatalf("decode hook body: %v", err)
				}
				w.WriteHeader(http.StatusCreated)
			default:
				t.Fatalf("hook method = %s", r.Method)
			}
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	m := New(forge.New(srv.URL, "token"), Config{
		Topic:         "merge-queue",
		StatusCtx:     "merge-queue",
		MergeStyle:    "merge",
		BisectFanout:  1,
		WebhookURL:    "https://shunt.example.com/webhook",
		WebhookSecret: "secret",
		InstanceURL:   srv.URL,
		PublicURL:     srv.URL,
		Token:         "token",
		BotUser:       "bot",
		BotEmail:      "bot@example.invalid",
		Metrics:       metrics.New(),
	})
	if err := m.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if createdHook == nil {
		t.Fatal("webhook was not created")
	}
	config := createdHook["config"].(map[string]any)
	if config["url"] != "https://shunt.example.com/webhook" || config["secret"] != "secret" {
		t.Fatalf("hook config = %#v", config)
	}
}

func TestRefreshRejectsInvalidRepoConfig(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/repos/search":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{
				"name":           "r",
				"default_branch": "main",
				"owner":          map[string]string{"login": "o"},
			}}})
		case "/api/v1/repos/o/r/raw/.shunt.yml":
			_, _ = w.Write([]byte("max_batch: -1\n"))
		default:
			t.Fatalf("unexpected request after invalid config: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	m := New(forge.New(srv.URL, "token"), Config{
		Topic:        "merge-queue",
		StatusCtx:    "merge-queue",
		MergeStyle:   "merge",
		BisectFanout: 1,
		InstanceURL:  srv.URL,
		PublicURL:    srv.URL,
		Token:        "token",
		BotUser:      "bot",
		BotEmail:     "bot@example.invalid",
		Metrics:      metrics.New(),
	})
	if err := m.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if len(m.engines) != 0 {
		t.Fatalf("engines = %#v, want none", m.engines)
	}
}

func writeSatisfiedStagingProtection(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	if r.Method != http.MethodGet {
		t.Fatalf("staging branch protection method = %s, want GET", r.Method)
	}
	_ = json.NewEncoder(w).Encode(forge.BranchProtection{
		EnablePush:              true,
		EnablePushWhitelist:     true,
		PushWhitelistUsernames:  []string{"bot"},
		PushWhitelistTeams:      []string{},
		PushWhitelistDeployKeys: false,
	})
}
