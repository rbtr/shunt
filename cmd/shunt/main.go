// Command shunt is a Forgejo merge-queue bot (rollup batching + bisection),
// driven by the auto-merge button. It manages either a single repo (SHUNT_REPO)
// or every repo carrying a topic (SHUNT_TOPIC).
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	checkpointbolt "github.com/rbtr/shunt/internal/checkpoint/bolt"
	checkpointpostgres "github.com/rbtr/shunt/internal/checkpoint/postgres"
	"github.com/rbtr/shunt/internal/engine"
	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
	"github.com/rbtr/shunt/internal/manager"
	"github.com/rbtr/shunt/internal/metrics"
	"github.com/rbtr/shunt/internal/repoconfig"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envBool(k string, def bool) (bool, error) {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def, nil
	}
	switch strings.ToLower(v) {
	case "1", "t", "true", "y", "yes", "on":
		return true, nil
	case "0", "f", "false", "n", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("bad %s: must be true or false", k)
	}
}

func forgeConfigFromEnv() (forge.Config, error) {
	cfg := forge.DefaultConfig()
	var err error

	cfg.RatePerSecond, err = strconv.ParseFloat(env("SHUNT_FORGE_RATE_PER_SECOND", strconv.FormatFloat(cfg.RatePerSecond, 'f', -1, 64)), 64)
	if err != nil {
		return cfg, fmt.Errorf("parse SHUNT_FORGE_RATE_PER_SECOND: %w", err)
	}
	cfg.RateBurst, err = strconv.Atoi(env("SHUNT_FORGE_RATE_BURST", strconv.Itoa(cfg.RateBurst)))
	if err != nil {
		return cfg, fmt.Errorf("parse SHUNT_FORGE_RATE_BURST: %w", err)
	}
	cfg.RetryInitial, err = time.ParseDuration(env("SHUNT_FORGE_RETRY_INITIAL", cfg.RetryInitial.String()))
	if err != nil {
		return cfg, fmt.Errorf("parse SHUNT_FORGE_RETRY_INITIAL: %w", err)
	}
	cfg.RetryMax, err = time.ParseDuration(env("SHUNT_FORGE_RETRY_MAX", cfg.RetryMax.String()))
	if err != nil {
		return cfg, fmt.Errorf("parse SHUNT_FORGE_RETRY_MAX: %w", err)
	}
	cfg.RetryAttempts, err = strconv.Atoi(env("SHUNT_FORGE_RETRY_ATTEMPTS", strconv.Itoa(cfg.RetryAttempts)))
	if err != nil {
		return cfg, fmt.Errorf("parse SHUNT_FORGE_RETRY_ATTEMPTS: %w", err)
	}
	cfg.OutageInitial, err = time.ParseDuration(env("SHUNT_FORGE_OUTAGE_INITIAL", cfg.OutageInitial.String()))
	if err != nil {
		return cfg, fmt.Errorf("parse SHUNT_FORGE_OUTAGE_INITIAL: %w", err)
	}
	cfg.OutageMax, err = time.ParseDuration(env("SHUNT_FORGE_OUTAGE_MAX", cfg.OutageMax.String()))
	if err != nil {
		return cfg, fmt.Errorf("parse SHUNT_FORGE_OUTAGE_MAX: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	baseLogger := slog.Default()
	logger := baseLogger.With("component", "main")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	instance := os.Getenv("SHUNT_INSTANCE")
	if instance == "" {
		fatal(logger, "SHUNT_INSTANCE required")
	}
	publicURL := env("SHUNT_PUBLIC_URL", instance)
	token := os.Getenv("SHUNT_TOKEN")
	if token == "" {
		fatal(logger, "SHUNT_TOKEN required")
	}
	botUser := env("SHUNT_BOT_USER", "mq-bot")
	botEmail := env("SHUNT_BOT_EMAIL", botUser+"@noreply.invalid")
	statusCtx := env("SHUNT_STATUS_CONTEXT", "merge-queue")
	mergeStyle, err := normalizeMergeStyle(env("SHUNT_MERGE_STYLE", "merge"))
	if err != nil {
		fatal(logger, "config error", "error", err)
	}
	maxBatch, _ := strconv.Atoi(env("SHUNT_MAX_BATCH", "0"))
	batchTarget, err := strconv.Atoi(env("SHUNT_BATCH_TARGET", "0"))
	if err != nil || batchTarget < 0 {
		fatal(logger, "bad SHUNT_BATCH_TARGET")
	}
	bisectFanout, err := strconv.Atoi(env("SHUNT_BISECT_FANOUT", "1"))
	if err != nil || bisectFanout < 1 {
		fatal(logger, "bad SHUNT_BISECT_FANOUT")
	}
	queueComments, err := envBool("SHUNT_QUEUE_COMMENTS", false)
	if err != nil {
		fatal(logger, "config error", "error", err)
	}
	webhookURL := strings.TrimSpace(os.Getenv("SHUNT_WEBHOOK_URL"))
	webhookSecret := os.Getenv("SHUNT_WEBHOOK_SECRET")
	interval, err := time.ParseDuration(env("SHUNT_POLL_INTERVAL", "10s"))
	if err != nil {
		fatal(logger, "bad SHUNT_POLL_INTERVAL", "error", err)
	}
	batchLinger, err := time.ParseDuration(env("SHUNT_BATCH_LINGER", "0"))
	if err != nil || batchLinger < 0 {
		fatal(logger, "bad SHUNT_BATCH_LINGER")
	}
	forgeCfg, err := forgeConfigFromEnv()
	if err != nil {
		fatal(logger, "forge client config error", "error", err)
	}

	metricsCollector := metrics.New()
	checkpointStore, err := openCheckpointStore(ctx, logger)
	if err != nil {
		fatal(logger, "checkpoint store error", "error", err)
	}
	if checkpointStore != nil {
		defer checkpointStore.Close()
	}
	wake := make(chan struct{}, 1)
	webhook := webhookConfig{
		Secret: webhookSecret,
		Wake:   wakeReconcile(wake),
		Logger: baseLogger.With("component", "webhook"),
	}

	fc, err := forge.NewWithConfig(instance, token, forgeCfg)
	if err != nil {
		fatal(logger, "forge client config error", "error", err)
	}

	if topic := os.Getenv("SHUNT_TOPIC"); topic != "" {
		mgr := manager.New(fc, manager.Config{
			Topic: topic, StatusCtx: statusCtx, MergeStyle: mergeStyle, MaxBatch: maxBatch, BatchLinger: batchLinger, BatchTarget: batchTarget, BisectFanout: bisectFanout, QueueComments: queueComments,
			WebhookURL: webhookURL, WebhookSecret: webhookSecret,
			InstanceURL: instance, PublicURL: publicURL, Token: token, BotUser: botUser, BotEmail: botEmail,
			Metrics: metricsCollector, Checkpoint: checkpointStore,
			Logger:       baseLogger.With("component", "manager"),
			EngineLogger: baseLogger.With("component", "engine"),
		})
		logger.Info("multi-repo mode", "topic", topic, "interval", interval)
		if err := runDaemon(ctx, baseLogger, env("SHUNT_LISTEN", ":8080"), metricsCollector, webhook, interval, wake, func(ctx context.Context) {
			if err := mgr.Refresh(ctx); err != nil {
				logger.Error("discovery failed", "error", err)
			}
			mgr.Tick(ctx)
		}); err != nil {
			fatal(logger, "daemon failed", "error", err)
		}
		return
	}

	// Single-repo mode.
	parts := strings.SplitN(os.Getenv("SHUNT_REPO"), "/", 2)
	if len(parts) != 2 {
		fatal(logger, "set SHUNT_TOPIC or SHUNT_REPO")
	}
	owner, repo := parts[0], parts[1]
	base := env("SHUNT_BASE", "main")
	settings := repoconfig.Settings{
		Base: base, StatusCtx: statusCtx, MergeStyle: mergeStyle, MaxBatch: maxBatch, BatchLinger: batchLinger, BatchTarget: batchTarget, BisectFanout: bisectFanout,
	}
	if data, err := fc.ReadFile(ctx, owner, repo, base, repoconfig.FileName); errors.Is(err, forge.ErrNotFound) {
		// No per-repo config; keep global defaults.
	} else if err != nil {
		fatal(logger, "repo config read failed", "owner", owner, "repo", repo, "path", repoconfig.FileName, "error", err)
	} else if settings, err = repoconfig.Apply(data, settings); err != nil {
		fatal(logger, "repo config invalid", "owner", owner, "repo", repo, "path", repoconfig.FileName, "error", err)
	}
	base = settings.Base
	queueLogger := logger.With("owner", owner, "repo", repo, "base", base)
	if changed, err := fc.EnsureStagingBranchProtection(ctx, owner, repo, base, botUser); err != nil {
		fatal(queueLogger, "ensure staging branch protection failed", "error", err)
	} else if changed {
		queueLogger.Info("staging branch protection configured")
	}
	if changed, err := fc.EnsureWebhook(ctx, owner, repo, webhookURL, webhookSecret); err != nil {
		fatal(queueLogger, "ensure webhook failed", "error", err)
	} else if changed {
		queueLogger.Info("webhook configured")
	}
	cloneURL := strings.TrimRight(instance, "/") + "/" + owner + "/" + repo + ".git"
	eng := engine.New(engine.Config{
		Owner: owner, Repo: repo, Base: base,
		StatusCtx: settings.StatusCtx, MergeStyle: settings.MergeStyle, MaxBatch: settings.MaxBatch, BatchLinger: settings.BatchLinger, BatchTarget: settings.BatchTarget, BisectFanout: settings.BisectFanout, QueueComments: queueComments, BotUser: botUser,
		StagingBranch: "mq/" + base + "/staging", InstanceURL: instance, PublicURL: publicURL,
		Metrics: metricsCollector, Checkpoint: checkpointStore, Logger: baseLogger.With("component", "engine"),
	}, fc, gitops.NewStager(cloneURL, botUser, token, botUser, botEmail))
	queueLogger.Info("single-repo mode", "interval", interval)
	if err := runDaemon(ctx, baseLogger, env("SHUNT_LISTEN", ":8080"), metricsCollector, webhook, interval, wake, func(ctx context.Context) {
		if err := eng.Reconcile(ctx); err != nil {
			queueLogger.Error("reconcile failed", "error", err)
		}
	}); err != nil {
		fatal(logger, "daemon failed", "error", err)
	}
}

func fatal(logger *slog.Logger, msg string, args ...any) {
	logger.Error(msg, args...)
	os.Exit(1)
}

type checkpointStoreCloser interface {
	engine.CheckpointStore
	Close() error
}

type postgresCheckpointStore struct {
	*checkpointpostgres.Store
	db *sql.DB
}

func (s *postgresCheckpointStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func openCheckpointStore(ctx context.Context, logger *slog.Logger) (checkpointStoreCloser, error) {
	path := strings.TrimSpace(os.Getenv("SHUNT_STATE_PATH"))
	dsn := strings.TrimSpace(os.Getenv("SHUNT_POSTGRES_DSN"))
	if path != "" && dsn != "" {
		return nil, fmt.Errorf("set only one of SHUNT_STATE_PATH or SHUNT_POSTGRES_DSN")
	}
	if path == "" && dsn == "" {
		return nil, nil
	}
	if dsn != "" {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return nil, fmt.Errorf("open SHUNT_POSTGRES_DSN: %w", err)
		}
		configurePostgresDB(db)
		store := checkpointpostgres.New(db)
		startupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := db.PingContext(startupCtx); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("ping SHUNT_POSTGRES_DSN: %w", err)
		}
		if err := store.ApplyMigrations(startupCtx); err != nil {
			_ = db.Close()
			return nil, err
		}
		logger.Info("using postgres queue state")
		return &postgresCheckpointStore{Store: store, db: db}, nil
	}
	store, err := checkpointbolt.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open SHUNT_STATE_PATH: %w", err)
	}
	logger.Info("using bbolt queue state", "path", path)
	return store, nil
}

func configurePostgresDB(db *sql.DB) {
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
}

func runDaemon(ctx context.Context, logger *slog.Logger, addr string, metricsCollector *metrics.Collector, webhook webhookConfig, interval time.Duration, wake <-chan struct{}, reconcile func(context.Context)) error {
	errc := make(chan error, 2)
	go func() {
		errc <- serveHealth(ctx, logger.With("component", "http"), addr, metricsCollector, webhook)
	}()
	go func() {
		errc <- runReconcileLoop(ctx, interval, wake, reconcile)
	}()

	err := <-errc
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func serveHealth(ctx context.Context, logger *slog.Logger, addr string, metricsCollector *metrics.Collector, webhook webhookConfig) error {
	server := &http.Server{
		Addr:    addr,
		Handler: newHTTPMux(metricsCollector, webhook),
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn("HTTP shutdown failed", "error", err)
		}
	}()

	logger.Info("HTTP listener started", "addr", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", err)
	}
	return nil
}

func newHTTPMux(metricsCollector *metrics.Collector, webhook webhookConfig) *http.ServeMux {
	if metricsCollector == nil {
		metricsCollector = metrics.New()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", metricsCollector.Handler())
	mux.Handle("/status", metricsCollector.StatusHandler())
	mux.Handle("/status.html", metricsCollector.StatusPageHandler())
	mux.HandleFunc("/webhook", webhook.handler)
	return mux
}

type webhookConfig struct {
	Secret string
	Wake   func()
	Logger *slog.Logger
}

func (c webhookConfig) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	if c.Secret != "" && !validWebhookSignature(c.Secret, body, r.Header) {
		http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
		return
	}

	event := webhookEvent(r.Header)
	if event == "" {
		http.Error(w, "missing webhook event header", http.StatusBadRequest)
		return
	}
	if !webhookWakes(event) {
		c.logger().Debug("webhook ignored", "event", event)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ignored\n"))
		return
	}
	if c.Wake == nil {
		http.Error(w, "webhook wake unavailable", http.StatusServiceUnavailable)
		return
	}
	c.Wake()
	c.logger().Info("webhook wake requested", "event", event)
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("wake\n"))
}

func (c webhookConfig) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default().With("component", "webhook")
}

func webhookEvent(h http.Header) string {
	for _, name := range []string{"X-Forgejo-Event", "X-Gitea-Event"} {
		if v := strings.TrimSpace(h.Get(name)); v != "" {
			return strings.ToLower(v)
		}
	}
	return ""
}

func webhookWakes(event string) bool {
	switch strings.ToLower(strings.TrimSpace(event)) {
	case "auto_merge_pull_request", "pull_request", "pull_request_sync", "push", "status":
		return true
	case "pull_request_review_approved", "pull_request_review_rejected", "pull_request_review_comment":
		return true
	default:
		return false
	}
}

func validWebhookSignature(secret string, body []byte, h http.Header) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	want := mac.Sum(nil)
	for _, header := range []string{"X-Forgejo-Signature", "X-Gitea-Signature", "X-Hub-Signature-256"} {
		got := strings.TrimSpace(h.Get(header))
		got = strings.TrimPrefix(got, "sha256=")
		if got == "" {
			continue
		}
		decoded, err := hex.DecodeString(got)
		if err != nil {
			continue
		}
		if hmac.Equal(decoded, want) {
			return true
		}
	}
	return false
}

func wakeReconcile(wake chan<- struct{}) func() {
	return func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func runReconcileLoop(ctx context.Context, interval time.Duration, wake <-chan struct{}, reconcile func(context.Context)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		reconcile(ctx)
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		case <-wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			drainWake(wake)
		}
	}
}

func drainWake(wake <-chan struct{}) {
	for {
		select {
		case <-wake:
		default:
			return
		}
	}
}

func normalizeMergeStyle(style string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(style)) {
	case "", "merge", "merge-commit", "merge_commit":
		return "merge", nil
	case "squash":
		return "squash", nil
	case "rebase":
		return "rebase", nil
	default:
		return "", fmt.Errorf("unsupported SHUNT_MERGE_STYLE %q: use merge, squash, or rebase", style)
	}
}
