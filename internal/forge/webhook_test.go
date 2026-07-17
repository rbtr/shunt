package forge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEnsureBranchProtectionCreatesMissingRuleWithoutBotPushAccess(t *testing.T) {
	var created map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/repos/o/r/branch_protections/main":
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

	changed, err := New(srv.URL, "token").EnsureBranchProtection(context.Background(), "o", "r", "main", "merge-queue", "bot")
	if err != nil {
		t.Fatalf("EnsureBranchProtection: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if created["enable_status_check"] != true || created["enable_push"] != true || created["enable_push_whitelist"] != true {
		t.Fatalf("created protection = %#v", created)
	}
	assertJSONStrings(t, created, "status_check_contexts", "merge-queue")
	assertJSONStrings(t, created, "push_whitelist_usernames")
	assertJSONStrings(t, created, "push_whitelist_teams")
	if created["push_whitelist_deploy_keys"] != false {
		t.Fatalf("push whitelist deploy keys = %#v, want false", created["push_whitelist_deploy_keys"])
	}
}

func TestEnsureBranchProtectionRemovesOnlyBotFromBasePushWhitelist(t *testing.T) {
	var patched map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/repos/o/r/branch_protections/main":
			_ = json.NewEncoder(w).Encode(BranchProtection{
				EnableStatusCheck:       true,
				StatusCheckContexts:     []string{"ci"},
				EnablePush:              true,
				EnablePushWhitelist:     true,
				PushWhitelistUsernames:  []string{"maintainer", "bot"},
				PushWhitelistTeams:      []string{"release"},
				PushWhitelistDeployKeys: true,
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/repos/o/r/branch_protections/main":
			if err := json.NewDecoder(r.Body).Decode(&patched); err != nil {
				t.Fatalf("decode patch body: %v", err)
			}
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	changed, err := New(srv.URL, "token").EnsureBranchProtection(context.Background(), "o", "r", "main", "merge-queue", "bot")
	if err != nil {
		t.Fatalf("EnsureBranchProtection: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	assertJSONStrings(t, patched, "status_check_contexts", "ci", "merge-queue")
	assertJSONStrings(t, patched, "push_whitelist_usernames", "maintainer")
	if _, ok := patched["push_whitelist_teams"]; ok {
		t.Fatalf("patch must preserve push whitelist teams by omission: %#v", patched)
	}
	if _, ok := patched["push_whitelist_deploy_keys"]; ok {
		t.Fatalf("patch must preserve deploy-key access by omission: %#v", patched)
	}
}

func TestEnsureBranchProtectionNoopWithoutBotBasePushAccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/repos/o/r/branch_protections/main" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		_ = json.NewEncoder(w).Encode(BranchProtection{
			EnableStatusCheck:      true,
			StatusCheckContexts:    []string{"merge-queue"},
			EnablePush:             true,
			EnablePushWhitelist:    true,
			PushWhitelistUsernames: []string{"maintainer"},
		})
	}))
	defer srv.Close()

	changed, err := New(srv.URL, "token").EnsureBranchProtection(context.Background(), "o", "r", "main", "merge-queue", "bot")
	if err != nil {
		t.Fatalf("EnsureBranchProtection: %v", err)
	}
	if changed {
		t.Fatal("changed = true, want false")
	}
}

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
	assertJSONStrings(t, body, "push_whitelist_usernames", "bot")
	assertJSONStrings(t, body, "push_whitelist_teams")
	if body["push_whitelist_deploy_keys"] != false {
		t.Fatalf("push whitelist deploy keys = %#v, want false", body["push_whitelist_deploy_keys"])
	}
}

func assertJSONStrings(t *testing.T, body map[string]any, key string, want ...string) {
	t.Helper()
	got, ok := body[key].([]any)
	if !ok {
		t.Fatalf("%s = %#v, want string array", key, body[key])
	}
	if len(got) != len(want) {
		t.Fatalf("%s = %#v, want %v", key, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %#v, want %v", key, got, want)
		}
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
