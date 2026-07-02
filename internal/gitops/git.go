// Package gitops builds integration ("staging") branches by merging PR head
// refs onto a base branch in a throwaway local clone, shelling out to git.
package gitops

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

type Stager struct {
	remoteURL   string // credential-free HTTPS clone URL
	gitUser     string
	gitToken    string
	authorName  string
	authorEmail string
}

func NewStager(remoteURL, gitUser, gitToken, name, email string) *Stager {
	return &Stager{
		remoteURL:   remoteURL,
		gitUser:     gitUser,
		gitToken:    gitToken,
		authorName:  name,
		authorEmail: email,
	}
}

// MergedRef identifies a PR head to merge.
type MergedRef struct {
	PR  int
	Ref string // e.g. refs/pull/12/head
}

// BuildStaging creates stagingBranch from base, merges each ref in order, pushes
// it, and returns the resulting SHA. The caller must pass a fresh branch name for
// each attempt. On a merge conflict it returns the offending PR number
// (conflictPR > 0) with an error.
func (s *Stager) BuildStaging(ctx context.Context, base, stagingBranch string, refs []MergedRef) (sha string, conflictPR int, err error) {
	parent, err := os.MkdirTemp("", "shunt-stage-")
	if err != nil {
		return "", 0, err
	}
	defer os.RemoveAll(parent)

	dir := parent + "/repo"
	askpass := parent + "/askpass.sh"
	tokenFile := parent + "/token"
	if err := os.WriteFile(tokenFile, []byte(s.gitToken), 0600); err != nil {
		return "", 0, err
	}
	if err := os.WriteFile(askpass, []byte(`#!/bin/sh
case "$1" in
*Username*) printf '%s\n' "$SHUNT_GIT_USER" ;;
*Password*) cat "$SHUNT_GIT_TOKEN_FILE" ;;
*) exit 1 ;;
esac
`), 0700); err != nil {
		return "", 0, err
	}

	run := func(args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = parent
		if len(args) > 0 && args[0] != "clone" {
			cmd.Dir = dir
		}
		cmd.Env = append(filteredEnv(),
			"GIT_ASKPASS="+askpass,
			"GIT_TERMINAL_PROMPT=0",
			"SHUNT_GIT_USER="+s.gitUser,
			"SHUNT_GIT_TOKEN_FILE="+tokenFile,
		)
		out, err := cmd.CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}

	if out, err := run("clone", "--quiet", "--no-tags", s.remoteURL, dir); err != nil {
		return "", 0, fmt.Errorf("clone: %v: %s", err, out)
	}
	if _, err := run("config", "user.name", s.authorName); err != nil {
		return "", 0, err
	}
	if _, err := run("config", "user.email", s.authorEmail); err != nil {
		return "", 0, err
	}
	if out, err := run("checkout", "-B", stagingBranch, "origin/"+base); err != nil {
		return "", 0, fmt.Errorf("checkout base %q: %v: %s", base, err, out)
	}
	for _, r := range refs {
		if out, err := run("fetch", "--quiet", "origin", r.Ref); err != nil {
			return "", r.PR, fmt.Errorf("fetch pr %d (%s): %v: %s", r.PR, r.Ref, err, out)
		}
		if out, err := run("merge", "--no-ff", "-m", fmt.Sprintf("mq: merge PR #%d", r.PR), "FETCH_HEAD"); err != nil {
			_, _ = run("merge", "--abort")
			return "", r.PR, fmt.Errorf("merge pr %d conflict: %s", r.PR, out)
		}
	}
	sha, err = run("rev-parse", "HEAD")
	if err != nil {
		return "", 0, err
	}
	if out, err := run("push", "--quiet", "origin", stagingBranch); err != nil {
		return "", 0, fmt.Errorf("push staging: %v: %s", err, out)
	}
	return sha, 0, nil
}

func filteredEnv() []string {
	var out []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "SHUNT_TOKEN=") ||
			strings.HasPrefix(kv, "SHUNT_GIT_TOKEN=") ||
			strings.HasPrefix(kv, "SHUNT_GIT_TOKEN_FILE=") {
			continue
		}
		out = append(out, kv)
	}
	return out
}
