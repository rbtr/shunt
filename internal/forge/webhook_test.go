package forge

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

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

	changed, err := New(srv.URL, "token").EnsureWebhook("o", "r", "https://shunt.example.com/webhook", "secret")
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

	changed, err := New(srv.URL, "token").EnsureWebhook("o", "r", "https://shunt.example.com/webhook", "")
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

	changed, err := New(srv.URL, "token").EnsureWebhook("o", "r", "https://shunt.example.com/webhook", "secret")
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

	changed, err := New(srv.URL, "token").EnsureWebhook("o", "r", "https://shunt.example.com/webhook", "secret")
	if err != nil {
		t.Fatalf("EnsureWebhook: %v", err)
	}
	if changed {
		t.Fatal("changed = true, want false")
	}
}
