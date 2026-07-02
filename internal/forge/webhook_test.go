package forge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEnsureStagingBranchProtectionCreatesMissingRule(t *testing.T) {
	var created map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.EscapedPath() == "/api/v1/repos/o/r/branch_protections/mq%2Fmain%2Fstaging%2A":
			http.NotFound(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/repos/o/r/branch_protections":
			if err := json.NewDecoder(r.Body).Decode(&created); err != nil {
				t.Fatalf("decode create body: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	changed, err := New(srv.URL, "token").EnsureStagingBranchProtection(context.Background(), "o", "r", "main", "bot")
	if err != nil {
		t.Fatalf("EnsureStagingBranchProtection: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if created["rule_name"] != "mq/main/staging*" {
		t.Fatalf("created rule_name = %v", created["rule_name"])
	}
	if created["enable_push"] != true || created["enable_push_whitelist"] != true {
		t.Fatalf("created push protection = %#v", created)
	}
	assertOnlyBotCanPush(t, created)
	if _, ok := created["enable_status_check"]; ok {
		t.Fatalf("staging protection should not require status checks: %#v", created)
	}
}

func TestEnsureStagingBranchProtectionUpdatesExistingRule(t *testing.T) {
	var patched map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.EscapedPath() == "/api/v1/repos/o/r/branch_protections/mq%2Frelease%2Fv1%2Fstaging%2A":
			_ = json.NewEncoder(w).Encode(BranchProtection{
				EnablePush:              false,
				EnablePushWhitelist:     false,
				PushWhitelistUsernames:  []string{"existing"},
				PushWhitelistTeams:      []string{"team"},
				PushWhitelistDeployKeys: true,
			})
		case r.Method == http.MethodPatch && r.URL.EscapedPath() == "/api/v1/repos/o/r/branch_protections/mq%2Frelease%2Fv1%2Fstaging%2A":
			if err := json.NewDecoder(r.Body).Decode(&patched); err != nil {
				t.Fatalf("decode patch body: %v", err)
			}
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	changed, err := New(srv.URL, "token").EnsureStagingBranchProtection(context.Background(), "o", "r", "release/v1", "bot")
	if err != nil {
		t.Fatalf("EnsureStagingBranchProtection: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if patched["enable_push"] != true || patched["enable_push_whitelist"] != true {
		t.Fatalf("patched push protection = %#v", patched)
	}
	assertOnlyBotCanPush(t, patched)
}

func assertOnlyBotCanPush(t *testing.T, body map[string]any) {
	t.Helper()
	users := body["push_whitelist_usernames"].([]any)
	if len(users) != 1 || users[0] != "bot" {
		t.Fatalf("push whitelist users = %#v, want bot only", users)
	}
	teams := body["push_whitelist_teams"].([]any)
	if len(teams) != 0 {
		t.Fatalf("push whitelist teams = %#v, want none", teams)
	}
	if body["push_whitelist_deploy_keys"] != false {
		t.Fatalf("push whitelist deploy keys = %#v, want false", body["push_whitelist_deploy_keys"])
	}
}

func TestEnsureWebhookCreatesMissingHook(t *testing.T) {
	var created map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/repos/o/r/hooks":
			_ = json.NewEncoder(w).Encode([]Hook{})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/repos/o/r/hooks":
			if err := json.NewDecoder(r.Body).Decode(&created); err != nil {
				t.Fatalf("decode create body: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	changed, err := New(srv.URL, "token").EnsureWebhook(context.Background(), "o", "r", "https://shunt.example.com/webhook", "secret")
	if err != nil {
		t.Fatalf("EnsureWebhook: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if created["type"] != "gitea" || created["active"] != true {
		t.Fatalf("created type/active = %v/%v", created["type"], created["active"])
	}
	config := created["config"].(map[string]any)
	if config["url"] != "https://shunt.example.com/webhook" || config["secret"] != "secret" || config["content_type"] != "json" {
		t.Fatalf("created config = %#v", config)
	}
	events := created["events"].([]any)
	if !eventListContains(events, "pull_request_sync") {
		t.Fatalf("created events = %#v, want pull_request_sync", events)
	}
}

func TestEnsureWebhookUpdatesExistingManagedHook(t *testing.T) {
	var patched map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/repos/o/r/hooks":
			_ = json.NewEncoder(w).Encode([]Hook{{
				ID:     42,
				Type:   "gitea",
				Active: false,
				Config: map[string]string{"url": "https://shunt.example.com/webhook", "content_type": "form"},
				Events: []string{"push"},
			}})
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/repos/o/r/hooks/42":
			if err := json.NewDecoder(r.Body).Decode(&patched); err != nil {
				t.Fatalf("decode patch body: %v", err)
			}
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	changed, err := New(srv.URL, "token").EnsureWebhook(context.Background(), "o", "r", "https://shunt.example.com/webhook", "")
	if err != nil {
		t.Fatalf("EnsureWebhook: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if patched["active"] != true {
		t.Fatalf("patched active = %v, want true", patched["active"])
	}
}

func TestEnsureWebhookAdoptsForgejoTypedHook(t *testing.T) {
	var patched map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/repos/o/r/hooks":
			_ = json.NewEncoder(w).Encode([]Hook{{
				ID:     42,
				Type:   "forgejo",
				Active: false,
				Config: map[string]string{"url": "https://shunt.example.com/webhook", "content_type": "form"},
				Events: []string{"push"},
			}})
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/repos/o/r/hooks/42":
			if err := json.NewDecoder(r.Body).Decode(&patched); err != nil {
				t.Fatalf("decode patch body: %v", err)
			}
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	changed, err := New(srv.URL, "token").EnsureWebhook(context.Background(), "o", "r", "https://shunt.example.com/webhook", "secret")
	if err != nil {
		t.Fatalf("EnsureWebhook: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if patched["active"] != true {
		t.Fatalf("patched active = %v, want true", patched["active"])
	}
}

func TestEnsureWebhookLeavesMatchingHookAlone(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/repos/o/r/hooks" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		_ = json.NewEncoder(w).Encode([]Hook{{
			ID:     42,
			Type:   "gitea",
			Active: true,
			Config: map[string]string{"url": "https://shunt.example.com/webhook", "content_type": "json", "secret": "secret"},
			Events: append([]string(nil), shuntWebhookEvents...),
		}})
	}))
	defer srv.Close()

	changed, err := New(srv.URL, "token").EnsureWebhook(context.Background(), "o", "r", "https://shunt.example.com/webhook", "secret")
	if err != nil {
		t.Fatalf("EnsureWebhook: %v", err)
	}
	if changed {
		t.Fatal("changed = true, want false")
	}
}

func TestEnsureWebhookTreatsRedactedSecretAsMatching(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/repos/o/r/hooks" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		_ = json.NewEncoder(w).Encode([]Hook{{
			ID:     42,
			Type:   "gitea",
			Active: true,
			Config: map[string]string{"url": "https://shunt.example.com/webhook", "content_type": "json"},
			Events: append([]string(nil), shuntWebhookEvents...),
		}})
	}))
	defer srv.Close()

	changed, err := New(srv.URL, "token").EnsureWebhook(context.Background(), "o", "r", "https://shunt.example.com/webhook", "secret")
	if err != nil {
		t.Fatalf("EnsureWebhook: %v", err)
	}
	if changed {
		t.Fatal("changed = true, want false")
	}
}

func eventListContains(events []any, want string) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}
