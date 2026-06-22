// Command shunt is a Forgejo merge-queue bot (rollup batching + bisection),
// driven by the auto-merge button. It manages either a single repo (SHUNT_REPO)
// or every repo carrying a topic (SHUNT_TOPIC).
package main

import (
	"errors"
	"fmt"
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
	go serveHealth(env("SHUNT_LISTEN", ":8080"), metricsCollector)

	fc := forge.New(instance, token)

	if topic := os.Getenv("SHUNT_TOPIC"); topic != "" {
		mgr := manager.New(fc, manager.Config{
			Topic: topic, StatusCtx: statusCtx, MergeStyle: mergeStyle, MaxBatch: maxBatch, BatchLinger: batchLinger, BatchTarget: batchTarget, BisectFanout: bisectFanout, QueueComments: queueComments,
			InstanceURL: instance, PublicURL: publicURL, Token: token, BotUser: botUser, BotEmail: botEmail,
			Metrics: metricsCollector,
		})
		log.Printf("shunt: multi-repo mode, topic=%q every %s", topic, interval)
		for {
			if err := mgr.Refresh(); err != nil {
				log.Printf("discovery error: %v", err)
			}
			mgr.Tick()
			time.Sleep(interval)
		}
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
	for {
		if err := eng.Reconcile(); err != nil {
			log.Printf("reconcile error: %v", err)
		}
		time.Sleep(interval)
	}
}

func serveHealth(addr string, metricsCollector *metrics.Collector) {
	if err := http.ListenAndServe(addr, newHTTPMux(metricsCollector)); err != nil {
		log.Printf("health server: %v", err)
	}
}

func newHTTPMux(metricsCollector *metrics.Collector) *http.ServeMux {
	if metricsCollector == nil {
		metricsCollector = metrics.New()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", metricsCollector.Handler())
	return mux
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
