// Command shunt is a Forgejo merge-queue bot (rollup batching + bisection),
// driven by the auto-merge button. It manages either a single repo (SHUNT_REPO)
// or every repo carrying a topic (SHUNT_TOPIC).
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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

func main() {
	instance := os.Getenv("SHUNT_INSTANCE")
	if instance == "" {
		log.Fatal("SHUNT_INSTANCE required (e.g. https://forge.example.com)")
	}
	publicURL := env("SHUNT_PUBLIC_URL", instance)
	token := os.Getenv("SHUNT_TOKEN")
	if token == "" {
		log.Fatal("SHUNT_TOKEN required")
	}
	botUser := env("SHUNT_BOT_USER", "mq-bot")
	botEmail := env("SHUNT_BOT_EMAIL", botUser+"@noreply.invalid")
	statusCtx := env("SHUNT_STATUS_CONTEXT", "merge-queue")
	mergeStyle, err := normalizeMergeStyle(env("SHUNT_MERGE_STYLE", "merge"))
	if err != nil {
		log.Fatal(err)
	}
	maxBatch, _ := strconv.Atoi(env("SHUNT_MAX_BATCH", "0"))
	batchTarget, err := strconv.Atoi(env("SHUNT_BATCH_TARGET", "0"))
	if err != nil || batchTarget < 0 {
		log.Fatalf("bad SHUNT_BATCH_TARGET: must be a non-negative integer")
	}
	bisectFanout, err := strconv.Atoi(env("SHUNT_BISECT_FANOUT", "1"))
	if err != nil || bisectFanout < 1 {
		log.Fatalf("bad SHUNT_BISECT_FANOUT: must be a positive integer")
	}
	queueComments, err := envBool("SHUNT_QUEUE_COMMENTS", false)
	if err != nil {
		log.Fatal(err)
	}
	interval, err := time.ParseDuration(env("SHUNT_POLL_INTERVAL", "10s"))
	if err != nil {
		log.Fatalf("bad SHUNT_POLL_INTERVAL: %v", err)
	}
	batchLinger, err := time.ParseDuration(env("SHUNT_BATCH_LINGER", "0"))
	if err != nil || batchLinger < 0 {
		log.Fatalf("bad SHUNT_BATCH_LINGER: must be a non-negative duration")
	}

	metricsCollector := metrics.New()
	wake := make(chan struct{}, 1)
	go serveHealth(env("SHUNT_LISTEN", ":8080"), metricsCollector, webhookConfig{
		Secret: os.Getenv("SHUNT_WEBHOOK_SECRET"),
		Wake:   wakeReconcile(wake),
	})

	fc := forge.New(instance, token)

	if topic := os.Getenv("SHUNT_TOPIC"); topic != "" {
		mgr := manager.New(fc, manager.Config{
			Topic: topic, StatusCtx: statusCtx, MergeStyle: mergeStyle, MaxBatch: maxBatch, BatchLinger: batchLinger, BatchTarget: batchTarget, BisectFanout: bisectFanout, QueueComments: queueComments,
			InstanceURL: instance, PublicURL: publicURL, Token: token, BotUser: botUser, BotEmail: botEmail,
			Metrics: metricsCollector,
		})
		log.Printf("shunt: multi-repo mode, topic=%q every %s", topic, interval)
		runReconcileLoop(interval, wake, func() {
			if err := mgr.Refresh(); err != nil {
				log.Printf("discovery error: %v", err)
			}
			mgr.Tick()
		})
	}

	// Single-repo mode.
	parts := strings.SplitN(os.Getenv("SHUNT_REPO"), "/", 2)
	if len(parts) != 2 {
		log.Fatal("set SHUNT_TOPIC or SHUNT_REPO=owner/repo")
	}
	owner, repo := parts[0], parts[1]
	base := env("SHUNT_BASE", "main")
	settings := repoconfig.Settings{
		Base: base, StatusCtx: statusCtx, MergeStyle: mergeStyle, MaxBatch: maxBatch, BatchLinger: batchLinger, BatchTarget: batchTarget, BisectFanout: bisectFanout,
	}
	if data, err := fc.ReadFile(owner, repo, base, repoconfig.FileName); errors.Is(err, forge.ErrNotFound) {
		// No per-repo config; keep global defaults.
	} else if err != nil {
		log.Fatalf("%s/%s: read %s: %v", owner, repo, repoconfig.FileName, err)
	} else if settings, err = repoconfig.Apply(data, settings); err != nil {
		log.Fatalf("%s/%s: invalid %s: %v", owner, repo, repoconfig.FileName, err)
	}
	base = settings.Base
	if deleted, err := fc.PruneStagingBranches(owner, repo, base); err != nil {
		log.Printf("shunt: %s/%s@%s: staging branch gc: %v", owner, repo, base, err)
	} else if len(deleted) > 0 {
		log.Printf("shunt: %s/%s@%s: deleted stale staging branches: %s", owner, repo, base, strings.Join(deleted, ", "))
	}
	cloneURL := strings.TrimRight(instance, "/") + "/" + owner + "/" + repo + ".git"
	eng := engine.New(engine.Config{
		Owner: owner, Repo: repo, Base: base,
		StatusCtx: settings.StatusCtx, MergeStyle: settings.MergeStyle, MaxBatch: settings.MaxBatch, BatchLinger: settings.BatchLinger, BatchTarget: settings.BatchTarget, BisectFanout: settings.BisectFanout, QueueComments: queueComments, BotUser: botUser,
		StagingBranch: "mq/" + base + "/staging", InstanceURL: instance, PublicURL: publicURL,
		Metrics: metricsCollector,
	}, fc, gitops.NewStager(cloneURL, botUser, token, botUser, botEmail))
	log.Printf("shunt: watching %s/%s base=%s every %s", owner, repo, base, interval)
	runReconcileLoop(interval, wake, func() {
		if err := eng.Reconcile(); err != nil {
			log.Printf("reconcile error: %v", err)
		}
	})
}

func serveHealth(addr string, metricsCollector *metrics.Collector, webhook webhookConfig) {
	if err := http.ListenAndServe(addr, newHTTPMux(metricsCollector, webhook)); err != nil {
		log.Printf("health server: %v", err)
	}
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
	mux.HandleFunc("/webhook", webhook.handler)
	return mux
}

type webhookConfig struct {
	Secret string
	Wake   func()
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
		log.Print("webhook: ignored event")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ignored\n"))
		return
	}
	if c.Wake == nil {
		http.Error(w, "webhook wake unavailable", http.StatusServiceUnavailable)
		return
	}
	c.Wake()
	log.Print("webhook: wake requested")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("wake\n"))
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

func runReconcileLoop(interval time.Duration, wake <-chan struct{}, reconcile func()) {
	for {
		reconcile()
		timer := time.NewTimer(interval)
		select {
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
