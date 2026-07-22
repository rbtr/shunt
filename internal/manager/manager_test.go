package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbtr/shunt/internal/engine"
	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/metrics"
)

func TestRefreshQuietlySkipsUnavailableForge(t *testing.T) {
	var logs bytes.Buffer
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/v1/repos/search" {
			t.Fatalf("unexpected request while forge is unavailable: %s %s", r.Method, r.URL.Path)
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	resilience := forge.DefaultConfig()
	resilience.RatePerSecond = 1000
	resilience.RateBurst = 100
	resilience.RetryInitial = time.Millisecond
	resilience.RetryMax = time.Millisecond
	resilience.RetryAttempts = 0
	client, err := forge.NewWithConfig(srv.URL, "token", resilience)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	m := New(client, Config{
		Topic: "merge-queue", Metrics: metrics.New(),
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
	})

	if err := m.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned unavailable error: %v", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want one discovery read", requests)
	}
	if len(m.engines) != 0 {
		t.Fatalf("engines = %#v, want none", m.engines)
	}
	if logs.Len() != 0 {
		t.Fatalf("unavailable forge logged unexpectedly: %s", logs.String())
	}
}

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

// ---------------------------------------------------------------------------
// Concurrency-safety tests
// ---------------------------------------------------------------------------

// fakeReconciler is a controllable reconciler for concurrency tests.
type fakeReconciler struct {
	fn func(ctx context.Context) error
}

func (f *fakeReconciler) Reconcile(ctx context.Context) error {
	if f.fn != nil {
		return f.fn(ctx)
	}
	return nil
}

// newTestManager builds a Manager with no forge client, suitable for
// white-box lifecycle tests that inject engines directly.
func newTestManager(t *testing.T, maxConcurrent int) *Manager {
	t.Helper()
	return &Manager{
		logger:        slog.Default(),
		engines:       map[string]*managedEngine{},
		maxConcurrent: maxConcurrent,
		cfg:           Config{Metrics: metrics.New()},
	}
}

// TestTickAtMostOneConcurrentReconcilePerEngine verifies that two overlapping
// Tick calls for the same engine never produce concurrent Reconcile calls.
// The second Tick must skip (not block or call Reconcile a second time) while
// the first Tick's Reconcile is still in flight.
func TestTickAtMostOneConcurrentReconcilePerEngine(t *testing.T) {
	t.Parallel()
	m := newTestManager(t, 2) // concurrency > 1 so it's not the limiter

	started := make(chan struct{})
	gate := make(chan struct{})
	var calls atomic.Int32
	m.engines["o/r@main"] = newManagedEngine(&fakeReconciler{
		fn: func(ctx context.Context) error {
			calls.Add(1)
			started <- struct{}{}
			select {
			case <-gate:
			case <-ctx.Done():
			}
			return nil
		},
	}, engine.Config{})

	// First Tick: starts a blocking Reconcile.
	tick1Done := make(chan struct{})
	go func() {
		defer close(tick1Done)
		m.Tick(context.Background())
	}()

	// Wait for the reconcile to be in flight.
	<-started

	// Second Tick while the first is still blocked: must skip, not block.
	m.Tick(context.Background())

	// Unblock the first Tick.
	close(gate)
	<-tick1Done

	if n := calls.Load(); n != 1 {
		t.Errorf("Reconcile called %d times, want exactly 1 (no concurrent overlap)", n)
	}
}

// TestRefreshDrainsBeforeReplacement verifies that drain() blocks until any
// in-flight Reconcile has returned, mirroring what Refresh does before
// replacing or removing an engine.  The test exercises the drain path
// directly (without concurrent map modification) because in the production
// reconcile loop Refresh and Tick are always sequential: eg.Wait() in Tick
// returns before Refresh ever touches the engines map.
func TestRefreshDrainsBeforeReplacement(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	var reconcileFinished atomic.Bool

	me := newManagedEngine(&fakeReconciler{
		fn: func(ctx context.Context) error {
			close(started)
			<-ctx.Done() // blocks until drain() cancels via lifetimeCtx
			reconcileFinished.Store(true)
			return nil
		},
	}, engine.Config{})

	// Simulate what Tick's goroutine does: hold mu for the Reconcile duration.
	reconcileDone := make(chan struct{})
	go func() {
		defer close(reconcileDone)
		if !me.mu.TryLock() {
			t.Errorf("TryLock failed unexpectedly")
			return
		}
		defer me.mu.Unlock()
		rctx, rcancel := context.WithCancel(context.Background())
		defer rcancel()
		stop := context.AfterFunc(me.lifetimeCtx, rcancel)
		defer stop()
		_ = me.engine.Reconcile(rctx)
	}()
	<-started // Reconcile is now in flight

	// drain() must: cancel the lifetime context (prompting Reconcile to exit
	// via ctx.Done()), then block via mu.Lock() until Reconcile has returned.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		me.drain()
	}()

	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("drain() did not return promptly")
	}
	<-reconcileDone

	if !reconcileFinished.Load() {
		t.Error("Reconcile had not finished when drain() returned")
	}
}

// TestTickConcurrentDifferentEngines verifies that with maxConcurrent > 1,
// multiple engines run concurrently in a single Tick pass.
// N blocking engines with maxConcurrent = N should all start within a short
// window, not take N × block-duration.
func TestTickConcurrentDifferentEngines(t *testing.T) {
	t.Parallel()
	const N = 3
	m := newTestManager(t, N)

	started := make(chan struct{}, N)
	gate := make(chan struct{})
	for i := range N {
		key := "o/r" + strconv.Itoa(i) + "@main"
		m.engines[key] = newManagedEngine(&fakeReconciler{
			fn: func(ctx context.Context) error {
				started <- struct{}{}
				select {
				case <-gate:
				case <-ctx.Done():
				}
				return nil
			},
		}, engine.Config{})
	}

	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		m.Tick(context.Background())
	}()

	// All N reconciles should start before Tick returns (concurrently).
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	for range N {
		select {
		case <-started:
		case <-timeout.C:
			t.Fatal("timed out waiting for all engines to start concurrently")
		}
	}
	// All N are running simultaneously — Tick is still blocked in eg.Wait().
	select {
	case <-tickDone:
		t.Fatal("Tick returned before all engines were released")
	default:
	}

	close(gate)
	<-tickDone
}

// TestTickShutdownCancelsInFlightReconciles verifies that cancelling the
// context passed to Tick causes in-flight Reconcile calls to exit promptly.
func TestTickShutdownCancelsInFlightReconciles(t *testing.T) {
	t.Parallel()
	m := newTestManager(t, 2)

	started := make(chan struct{})
	cancelled := make(chan struct{})
	m.engines["o/r@main"] = newManagedEngine(&fakeReconciler{
		fn: func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		},
	}, engine.Config{})

	ctx, cancel := context.WithCancel(context.Background())
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		m.Tick(ctx)
	}()

	<-started
	cancel() // simulate SIGTERM / process shutdown

	select {
	case <-tickDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Tick did not return promptly after context cancellation")
	}
	select {
	case <-cancelled:
	default:
		t.Error("Reconcile was not notified of context cancellation")
	}
}

// TestTickEngineReplacementCancelsReconcile verifies that calling drain()
// (engine replacement/removal) cancels the in-flight Reconcile via the engine's
// lifetime context even if the process context is still live.
func TestTickEngineReplacementCancelsReconcile(t *testing.T) {
	t.Parallel()
	m := newTestManager(t, 1)

	started := make(chan struct{})
	cancelled := make(chan struct{})
	me := newManagedEngine(&fakeReconciler{
		fn: func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		},
	}, engine.Config{})
	m.engines["o/r@main"] = me

	// Use a live (non-cancelled) process context.
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		m.Tick(context.Background())
	}()
	<-started

	// Draining (as Refresh would do) should cancel the in-flight Reconcile.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		me.drain()
	}()

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("Reconcile was not cancelled by engine drain")
	}
	<-drainDone
	<-tickDone
}
