// Command shunt is a Forgejo merge-queue bot (rollup batching + bisection),
// driven by the auto-merge button. It manages either a single repo (SHUNT_REPO)
// or every repo carrying a topic (SHUNT_TOPIC).
package main

import (
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
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
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
	mergeStyle := strings.ToLower(env("SHUNT_MERGE_STYLE", "merge"))
	if mergeStyle != "merge" {
		log.Fatalf("unsupported SHUNT_MERGE_STYLE %q: v0.1 supports only %q", mergeStyle, "merge")
	}
	maxBatch, _ := strconv.Atoi(env("SHUNT_MAX_BATCH", "0"))
	batchTarget, err := strconv.Atoi(env("SHUNT_BATCH_TARGET", "0"))
	if err != nil || batchTarget < 0 {
		log.Fatalf("bad SHUNT_BATCH_TARGET: must be a non-negative integer")
	}
	interval, err := time.ParseDuration(env("SHUNT_POLL_INTERVAL", "10s"))
	if err != nil {
		log.Fatalf("bad SHUNT_POLL_INTERVAL: %v", err)
	}
	batchLinger, err := time.ParseDuration(env("SHUNT_BATCH_LINGER", "0"))
	if err != nil || batchLinger < 0 {
		log.Fatalf("bad SHUNT_BATCH_LINGER: must be a non-negative duration")
	}

	go serveHealth(env("SHUNT_LISTEN", ":8080"))

	fc := forge.New(instance, token)

	if topic := os.Getenv("SHUNT_TOPIC"); topic != "" {
		mgr := manager.New(fc, manager.Config{
			Topic: topic, StatusCtx: statusCtx, MergeStyle: mergeStyle, MaxBatch: maxBatch, BatchLinger: batchLinger, BatchTarget: batchTarget,
			InstanceURL: instance, PublicURL: publicURL, Token: token, BotUser: botUser, BotEmail: botEmail,
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
	cloneURL := strings.TrimRight(instance, "/") + "/" + owner + "/" + repo + ".git"
	eng := engine.New(engine.Config{
		Owner: owner, Repo: repo, Base: base,
		StatusCtx: statusCtx, MergeStyle: mergeStyle, MaxBatch: maxBatch, BatchLinger: batchLinger, BatchTarget: batchTarget,
		StagingBranch: "mq/" + base + "/staging", InstanceURL: instance, PublicURL: publicURL,
	}, fc, gitops.NewStager(cloneURL, botUser, token, botUser, botEmail))

	log.Printf("shunt: watching %s/%s base=%s every %s", owner, repo, base, interval)
	for {
		if err := eng.Reconcile(); err != nil {
			log.Printf("reconcile error: %v", err)
		}
		time.Sleep(interval)
	}
}

func serveHealth(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("health server: %v", err)
	}
}
