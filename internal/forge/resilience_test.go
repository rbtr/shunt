package forge

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestReadRetriesRetryableServerFailure(t *testing.T) {
	resetProcessLimiter(t)
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/repos/o/r/pulls" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if requests == 1 {
			http.Error(w, "temporary failure", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, testResilienceConfig())
	if _, err := client.ListOpenPRs(context.Background(), "o", "r", ""); err != nil {
		t.Fatalf("ListOpenPRs: %v", err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestMutationDoesNotRetryServerFailure(t *testing.T) {
	resetProcessLimiter(t)
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "temporary failure", http.StatusBadGateway)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, testResilienceConfig())
	err := client.SetCommitStatus(context.Background(), "o", "r", "abc", "queue", "pending", "", "")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("SetCommitStatus error = %v, want unavailable", err)
	}
	var unavailableErr *UnavailableError
	if !errors.As(err, &unavailableErr) {
		t.Fatalf("SetCommitStatus error = %v, want UnavailableError", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
}

func TestCircuitQuietsRequestsProbesAndRecovers(t *testing.T) {
	resetProcessLimiter(t)
	cfg := testResilienceConfig()
	cfg.RetryAttempts = 0
	cfg.OutageInitial = 20 * time.Millisecond
	cfg.OutageMax = 40 * time.Millisecond

	var mu sync.Mutex
	apiRequests := 0
	healthRequests := 0
	healthStarted := make(chan struct{})
	releaseHealth := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/repos/o/r/pulls":
			mu.Lock()
			apiRequests++
			n := apiRequests
			mu.Unlock()
			if n == 1 {
				http.Error(w, "temporary failure", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`[]`))
		case "/api/healthz":
			mu.Lock()
			healthRequests++
			mu.Unlock()
			close(healthStarted)
			<-releaseHealth
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, cfg)
	if _, err := client.ListOpenPRs(context.Background(), "o", "r", ""); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("initial ListOpenPRs error = %v, want unavailable", err)
	}
	if _, err := client.ListOpenPRs(context.Background(), "o", "r", ""); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("quiet ListOpenPRs error = %v, want unavailable", err)
	}

	time.Sleep(cfg.OutageInitial + 5*time.Millisecond)
	first := make(chan error, 1)
	go func() {
		_, err := client.ListOpenPRs(context.Background(), "o", "r", "")
		first <- err
	}()
	<-healthStarted

	_, secondErr := client.ListOpenPRs(context.Background(), "o", "r", "")
	if !errors.Is(secondErr, ErrUnavailable) {
		t.Fatalf("concurrent ListOpenPRs error = %v, want unavailable", secondErr)
	}
	close(releaseHealth)
	if err := <-first; err != nil {
		t.Fatalf("probe ListOpenPRs: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if healthRequests != 1 {
		t.Fatalf("health requests = %d, want 1", healthRequests)
	}
	if apiRequests != 2 {
		t.Fatalf("API requests = %d, want 2", apiRequests)
	}
}

func TestHealthProbeUsesRootURLAndVersionFallback(t *testing.T) {
	resetProcessLimiter(t)
	var rootRequests, versionRequests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/healthz":
			rootRequests++
			if got := r.Header.Get("Authorization"); got != "" {
				t.Fatalf("root health authorization = %q, want empty", got)
			}
			http.NotFound(w, r)
		case "/api/v1/version":
			versionRequests++
			if got := r.Header.Get("Authorization"); got != "token token" {
				t.Fatalf("version authorization = %q", got)
			}
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/repos/o/r/pulls":
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Fatalf("unexpected request path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, testResilienceConfig())
	expireCircuit(client)
	if _, err := client.ListOpenPRs(context.Background(), "o", "r", ""); err != nil {
		t.Fatalf("ListOpenPRs: %v", err)
	}
	if rootRequests != 1 || versionRequests != 1 {
		t.Fatalf("root/version requests = %d/%d, want 1/1", rootRequests, versionRequests)
	}
}

func TestHealthProbeDoesNotFallbackAfterNon404(t *testing.T) {
	resetProcessLimiter(t)
	var versionRequests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/healthz":
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		case "/api/v1/version":
			versionRequests++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, testResilienceConfig())
	expireCircuit(client)
	if _, err := client.ListOpenPRs(context.Background(), "o", "r", ""); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ListOpenPRs error = %v, want unavailable", err)
	}
	if versionRequests != 0 {
		t.Fatalf("version requests = %d, want 0", versionRequests)
	}
}

func TestRateLimitCooldownSurvivesHealthProbe(t *testing.T) {
	resetProcessLimiter(t)
	var apiRequests, healthRequests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/repos/o/r/pulls":
			apiRequests++
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
		case "/api/healthz":
			healthRequests++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, testResilienceConfig())
	if _, err := client.ListOpenPRs(context.Background(), "o", "r", ""); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("initial ListOpenPRs error = %v, want rate limited", err)
	}
	expireCircuit(client)
	if _, err := client.ListOpenPRs(context.Background(), "o", "r", ""); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("post-health ListOpenPRs error = %v, want rate limited", err)
	}
	if healthRequests != 1 {
		t.Fatalf("health requests = %d, want 1", healthRequests)
	}
	if apiRequests != 1 {
		t.Fatalf("API requests = %d, want 1", apiRequests)
	}
}

func TestLimiterIsSharedAcrossClients(t *testing.T) {
	resetProcessLimiter(t)
	cfg := testResilienceConfig()
	first := newTestClient(t, "https://example.invalid", cfg)
	second := newTestClient(t, "https://example.invalid", cfg)
	if first.limiter != second.limiter {
		t.Fatal("clients did not share the process limiter")
	}
}

func TestConfigValidate(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("default config: %v", err)
	}
	cfg := DefaultConfig()
	cfg.OutageMax = cfg.OutageInitial - time.Second
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate accepted reversed outage durations")
	}
	if _, err := NewWithConfig("https://example.invalid", "token", cfg); err == nil {
		t.Fatal("NewWithConfig accepted invalid configuration")
	}
}

func testResilienceConfig() Config {
	return Config{
		RatePerSecond: 1000,
		RateBurst:     100,
		RetryInitial:  time.Millisecond,
		RetryMax:      time.Millisecond,
		RetryAttempts: 1,
		OutageInitial: 10 * time.Millisecond,
		OutageMax:     20 * time.Millisecond,
	}
}

func newTestClient(t *testing.T, instanceURL string, cfg Config) *Client {
	t.Helper()
	client, err := NewWithConfig(instanceURL, "token", cfg)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	return client
}

func expireCircuit(client *Client) {
	client.outage.mu.Lock()
	client.outage.until = time.Now().Add(-time.Millisecond)
	client.outage.backoff = client.cfg.OutageInitial
	client.outage.mu.Unlock()
}

func resetProcessLimiter(t *testing.T) {
	t.Helper()
	processLimiterMu.Lock()
	previous := processLimiter
	processLimiter = nil
	processLimiterMu.Unlock()
	t.Cleanup(func() {
		processLimiterMu.Lock()
		processLimiter = previous
		processLimiterMu.Unlock()
	})
}
