package forge

import (
	"context"
	"os"
	"strconv"
	"testing"
)

type forgeIntegrationConfig struct {
	instance string
	token    string
	owner    string
	repo     string
	base     string
}

func TestForgeIntegrationHarness(t *testing.T) {
	if os.Getenv("SHUNT_FORGE_INTEGRATION") != "1" {
		t.Skip("set SHUNT_FORGE_INTEGRATION=1 and forge test env vars to run live Forgejo/Gitea integration tests")
	}

	cfg := requireForgeIntegrationConfig(t)
	client := New(cfg.instance, cfg.token)
	ctx := context.Background()

	t.Run("list pull requests", func(t *testing.T) {
		prs, err := client.ListOpenPRs(ctx, cfg.owner, cfg.repo, cfg.base)
		if err != nil {
			t.Fatalf("ListOpenPRs: %v", err)
		}
		t.Logf("found %d open pull requests targeting %q", len(prs), cfg.base)
	})

	t.Run("pull request timeline automerge detection", func(t *testing.T) {
		index := optionalIntEnv(t, "SHUNT_FORGE_PR_INDEX")
		if index == 0 {
			t.Skip("set SHUNT_FORGE_PR_INDEX to exercise PR fetch and timeline auto-merge detection")
		}

		pr, err := client.GetPR(ctx, cfg.owner, cfg.repo, index)
		if err != nil {
			t.Fatalf("GetPR(%d): %v", index, err)
		}
		if pr.Number != index {
			t.Fatalf("PR number = %d, want %d", pr.Number, index)
		}

		scheduled, err := client.AutomergeScheduled(ctx, cfg.owner, cfg.repo, index)
		if err != nil {
			t.Fatalf("AutomergeScheduled(%d): %v", index, err)
		}
		t.Logf("PR #%d automerge scheduled = %v", index, scheduled)
	})

	t.Run("post commit status", func(t *testing.T) {
		sha := os.Getenv("SHUNT_FORGE_STATUS_SHA")
		if sha == "" {
			t.Skip("set SHUNT_FORGE_STATUS_SHA to post a non-required integration-test commit status")
		}

		statusContext := envOrDefault("SHUNT_FORGE_STATUS_CONTEXT", "shunt-integration")
		state := envOrDefault("SHUNT_FORGE_STATUS_STATE", "pending")
		desc := envOrDefault("SHUNT_FORGE_STATUS_DESCRIPTION", "shunt forge integration harness")
		targetURL := os.Getenv("SHUNT_FORGE_STATUS_TARGET_URL")
		if err := client.SetCommitStatus(ctx, cfg.owner, cfg.repo, sha, statusContext, state, desc, targetURL); err != nil {
			t.Fatalf("SetCommitStatus(%s): %v", sha, err)
		}
	})

	t.Run("lookup workflow run status", func(t *testing.T) {
		sha := os.Getenv("SHUNT_FORGE_RUN_SHA")
		if sha == "" {
			t.Skip("set SHUNT_FORGE_RUN_SHA to look up a workflow run status")
		}

		status, err := client.RunStatus(ctx, cfg.owner, cfg.repo, sha, os.Getenv("SHUNT_FORGE_RUN_BRANCH"))
		if err != nil {
			t.Fatalf("RunStatus(%s): %v", sha, err)
		}
		t.Logf("workflow run status for %s = %q", sha, status)
	})

	t.Run("ensure branch protection", func(t *testing.T) {
		if os.Getenv("SHUNT_FORGE_ALLOW_BRANCH_PROTECTION_WRITE") != "1" {
			t.Skip("set SHUNT_FORGE_ALLOW_BRANCH_PROTECTION_WRITE=1 to allow branch-protection mutation")
		}

		branch := requiredEnv(t, "SHUNT_FORGE_BRANCH_PROTECTION_BRANCH")
		statusContext := envOrDefault("SHUNT_FORGE_BRANCH_PROTECTION_STATUS_CONTEXT", "merge-queue")
		botUser := requiredEnv(t, "SHUNT_FORGE_BRANCH_PROTECTION_BOT_USER")
		changed, err := client.EnsureBranchProtection(ctx, cfg.owner, cfg.repo, branch, statusContext, botUser)
		if err != nil {
			t.Fatalf("EnsureBranchProtection(%s): %v", branch, err)
		}
		t.Logf("branch protection changed = %v", changed)
	})
}

func requireForgeIntegrationConfig(t *testing.T) forgeIntegrationConfig {
	t.Helper()
	return forgeIntegrationConfig{
		instance: requiredEnv(t, "SHUNT_FORGE_INSTANCE"),
		token:    requiredEnv(t, "SHUNT_FORGE_TOKEN"),
		owner:    requiredEnv(t, "SHUNT_FORGE_OWNER"),
		repo:     requiredEnv(t, "SHUNT_FORGE_REPO"),
		base:     envOrDefault("SHUNT_FORGE_BASE", "main"),
	}
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required when SHUNT_FORGE_INTEGRATION=1", name)
	}
	return value
}

func optionalIntEnv(t *testing.T, name string) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive integer", name)
	}
	return parsed
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
