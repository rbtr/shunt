package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/metrics"
)

func TestForgeConfigFromEnv(t *testing.T) {
	for _, name := range []string{
		"SHUNT_FORGE_RATE_PER_SECOND",
		"SHUNT_FORGE_RATE_BURST",
		"SHUNT_FORGE_RETRY_INITIAL",
		"SHUNT_FORGE_RETRY_MAX",
		"SHUNT_FORGE_RETRY_ATTEMPTS",
		"SHUNT_FORGE_OUTAGE_INITIAL",
		"SHUNT_FORGE_OUTAGE_MAX",
	} {
		t.Setenv(name, "")
	}

	cfg, err := forgeConfigFromEnv()
	if err != nil {
		t.Fatalf("forgeConfigFromEnv defaults: %v", err)
	}
	if cfg != forge.DefaultConfig() {
		t.Fatalf("default config = %#v, want %#v", cfg, forge.DefaultConfig())
	}

	t.Setenv("SHUNT_FORGE_OUTAGE_MAX", "10s")
	if _, err := forgeConfigFromEnv(); err == nil {
		t.Fatal("forgeConfigFromEnv accepted an outage maximum below its initial duration")
	}
}

func TestQueueLeaseTTLFromEnv(t *testing.T) {
	t.Setenv("SHUNT_QUEUE_LEASE_TTL", "")
	ttl, err := queueLeaseTTLFromEnv()
	if err != nil {
		t.Fatalf("queueLeaseTTLFromEnv default: %v", err)
	}
	if ttl != 45*time.Second {
		t.Fatalf("default lease TTL = %s, want 45s", ttl)
	}

	t.Setenv("SHUNT_QUEUE_LEASE_TTL", "90s")
	ttl, err = queueLeaseTTLFromEnv()
	if err != nil {
		t.Fatalf("queueLeaseTTLFromEnv configured: %v", err)
	}
	if ttl != 90*time.Second {
		t.Fatalf("configured lease TTL = %s, want 90s", ttl)
	}

	t.Setenv("SHUNT_QUEUE_LEASE_TTL", "0s")
	if _, err := queueLeaseTTLFromEnv(); err == nil {
		t.Fatal("queueLeaseTTLFromEnv accepted zero TTL")
	}
	t.Setenv("SHUNT_QUEUE_LEASE_TTL", "1ns")
	if _, err := queueLeaseTTLFromEnv(); err == nil {
		t.Fatal("queueLeaseTTLFromEnv accepted a TTL below one microsecond")
	}
}

func TestNewLeaseHolderID(t *testing.T) {
	first, err := newLeaseHolderID()
	if err != nil {
		t.Fatalf("newLeaseHolderID: %v", err)
	}
	second, err := newLeaseHolderID()
	if err != nil {
		t.Fatalf("newLeaseHolderID: %v", err)
	}
	if len(first) != 32 {
		t.Fatalf("holder ID length = %d, want 32", len(first))
	}
	if _, err := hex.DecodeString(first); err != nil {
		t.Fatalf("holder ID is not hexadecimal: %v", err)
	}
	if first == second {
		t.Fatal("holder IDs must differ")
	}
}

func TestHTTPMuxServesHealthMetricsAndStatus(t *testing.T) {
	c := metrics.New()
	c.ObserveQueueStatus(metrics.Labels{Owner: "o", Repo: "r", Base: "main"}, [][]int{{2}}, [][]int{{1}})
	mux := newHTTPMux(c, webhookConfig{})

	health := httptest.NewRecorder()
	mux.ServeHTTP(health, httptest.NewRequest("GET", "/healthz", nil))
	if got := health.Body.String(); got != "ok" {
		t.Fatalf("/healthz = %q, want ok", got)
	}

	metricsResp := httptest.NewRecorder()
	mux.ServeHTTP(metricsResp, httptest.NewRequest("GET", "/metrics", nil))
	if got := metricsResp.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("/metrics Content-Type = %q", got)
	}
	if body := metricsResp.Body.String(); !strings.Contains(body, `shunt_queue_depth{owner="o",repo="r",base="main"} 2`) {
		t.Fatalf("/metrics missing queue depth in:\n%s", body)
	}

	statusResp := httptest.NewRecorder()
	mux.ServeHTTP(statusResp, httptest.NewRequest("GET", "/status", nil))
	if got := statusResp.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("/status Content-Type = %q", got)
	}
	statusBody := statusResp.Body.String()
	for _, want := range []string{`"owner":"o"`, `"repo":"r"`, `"base":"main"`, `"active_batches":[[1]]`, `"pending_batches":[[2]]`} {
		if !strings.Contains(statusBody, want) {
			t.Fatalf("/status missing %q in:\n%s", want, statusBody)
		}
	}

	statusPageResp := httptest.NewRecorder()
	mux.ServeHTTP(statusPageResp, httptest.NewRequest("GET", "/status.html", nil))
	if got := statusPageResp.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("/status.html Content-Type = %q", got)
	}
	if body := statusPageResp.Body.String(); !strings.Contains(body, "o/r@main") || !strings.Contains(body, "[1]") || !strings.Contains(body, "[2]") {
		t.Fatalf("/status.html missing queue details in:\n%s", body)
	}
}

func TestWebhookWakesForRelevantEvents(t *testing.T) {
	for _, event := range []string{"push", "pull_request", "pull_request_sync"} {
		t.Run(event, func(t *testing.T) {
			wakes := 0
			mux := newHTTPMux(metrics.New(), webhookConfig{Wake: func() { wakes++ }})

			resp := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/webhook", strings.NewReader(`{"ref":"refs/heads/main"}`))
			req.Header.Set("X-Gitea-Event", event)
			mux.ServeHTTP(resp, req)

			if resp.Code != 202 {
				t.Fatalf("/webhook status = %d, want 202", resp.Code)
			}
			if wakes != 1 {
				t.Fatalf("wakes = %d, want 1", wakes)
			}
		})
	}
}

func TestWebhookIgnoresIrrelevantEvents(t *testing.T) {
	wakes := 0
	mux := newHTTPMux(metrics.New(), webhookConfig{Wake: func() { wakes++ }})

	resp := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/webhook", strings.NewReader(`{}`))
	req.Header.Set("X-Forgejo-Event", "issues")
	mux.ServeHTTP(resp, req)

	if resp.Code != 202 {
		t.Fatalf("/webhook status = %d, want 202", resp.Code)
	}
	if body := resp.Body.String(); body != "ignored\n" {
		t.Fatalf("/webhook body = %q, want ignored", body)
	}
	if wakes != 0 {
		t.Fatalf("wakes = %d, want 0", wakes)
	}
}

func TestWebhookRequiresValidSignatureWhenSecretConfigured(t *testing.T) {
	const secret = "shared-secret"
	const body = `{"action":"auto_merge"}`
	wakes := 0
	mux := newHTTPMux(metrics.New(), webhookConfig{Secret: secret, Wake: func() { wakes++ }})

	bad := httptest.NewRecorder()
	badReq := httptest.NewRequest("POST", "/webhook", strings.NewReader(body))
	badReq.Header.Set("X-Gitea-Event", "auto_merge_pull_request")
	mux.ServeHTTP(bad, badReq)
	if bad.Code != 401 {
		t.Fatalf("unsigned /webhook status = %d, want 401", bad.Code)
	}

	good := httptest.NewRecorder()
	goodReq := httptest.NewRequest("POST", "/webhook", strings.NewReader(body))
	goodReq.Header.Set("X-Gitea-Event", "auto_merge_pull_request")
	goodReq.Header.Set("X-Gitea-Signature", webhookSignature(secret, body))
	mux.ServeHTTP(good, goodReq)
	if good.Code != 202 {
		t.Fatalf("signed /webhook status = %d, want 202: %s", good.Code, good.Body.String())
	}
	if wakes != 1 {
		t.Fatalf("wakes = %d, want 1", wakes)
	}
}

func TestWebhookRejectsMissingEvent(t *testing.T) {
	mux := newHTTPMux(metrics.New(), webhookConfig{Wake: func() {}})
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, httptest.NewRequest("POST", "/webhook", strings.NewReader(`{}`)))
	if resp.Code != 400 {
		t.Fatalf("/webhook status = %d, want 400", resp.Code)
	}
}

func TestWebhookRejectsNonPost(t *testing.T) {
	mux := newHTTPMux(metrics.New(), webhookConfig{Wake: func() {}})
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, httptest.NewRequest("GET", "/webhook", nil))
	if resp.Code != 405 {
		t.Fatalf("/webhook status = %d, want 405", resp.Code)
	}
}

func webhookSignature(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = io.WriteString(mac, body)
	return hex.EncodeToString(mac.Sum(nil))
}

func TestNormalizeMergeStyle(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  string
	}{
		{input: "", want: "merge"},
		{input: "merge", want: "merge"},
		{input: "MERGE-COMMIT", want: "merge"},
		{input: "squash", want: "squash"},
		{input: " ReBase ", want: "rebase"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := normalizeMergeStyle(tc.input)
			if err != nil {
				t.Fatalf("normalizeMergeStyle(%q): %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("normalizeMergeStyle(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestNormalizeMergeStyleRejectsUnknown(t *testing.T) {
	if _, err := normalizeMergeStyle("fast-forward"); err == nil {
		t.Fatal("normalizeMergeStyle should reject unsupported styles")
	}
}

func TestEnvBool(t *testing.T) {
	t.Setenv("SHUNT_TEST_BOOL", "")
	got, err := envBool("SHUNT_TEST_BOOL", true)
	if err != nil {
		t.Fatalf("default envBool: %v", err)
	}
	if !got {
		t.Fatal("empty env should return default true")
	}

	for _, tc := range []struct {
		value string
		want  bool
	}{
		{value: "true", want: true},
		{value: "YES", want: true},
		{value: "0", want: false},
		{value: "off", want: false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("SHUNT_TEST_BOOL", tc.value)
			got, err := envBool("SHUNT_TEST_BOOL", !tc.want)
			if err != nil {
				t.Fatalf("envBool(%q): %v", tc.value, err)
			}
			if got != tc.want {
				t.Fatalf("envBool(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}

	t.Setenv("SHUNT_TEST_BOOL", "sometimes")
	if _, err := envBool("SHUNT_TEST_BOOL", false); err == nil {
		t.Fatal("envBool should reject invalid values")
	}
}

func TestOpenCheckpointStoreDisabledByDefault(t *testing.T) {
	t.Setenv("SHUNT_STATE_PATH", "")
	t.Setenv("SHUNT_POSTGRES_DSN", "")
	store, err := openCheckpointStore(context.Background(), slog.Default())
	if err != nil {
		t.Fatalf("open checkpoint store: %v", err)
	}
	if store != nil {
		t.Fatal("store = non-nil, want nil when SHUNT_STATE_PATH is unset")
	}
}

func TestOpenCheckpointStoreUsesStatePath(t *testing.T) {
	t.Setenv("SHUNT_POSTGRES_DSN", "")
	t.Setenv("SHUNT_STATE_PATH", t.TempDir()+"/shunt.db")
	store, err := openCheckpointStore(context.Background(), slog.Default())
	if err != nil {
		t.Fatalf("open checkpoint store: %v", err)
	}
	if store == nil {
		t.Fatal("store = nil, want bbolt store")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
}

func TestOpenCheckpointStoreRejectsMultipleBackends(t *testing.T) {
	t.Setenv("SHUNT_STATE_PATH", t.TempDir()+"/shunt.db")
	t.Setenv("SHUNT_POSTGRES_DSN", "postgres://user:pass@example.invalid/db")
	_, err := openCheckpointStore(context.Background(), slog.Default())
	if err == nil {
		t.Fatal("open checkpoint store should reject multiple backends")
	}
}
