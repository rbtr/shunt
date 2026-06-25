package engine

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/rbtr/shunt/internal/forge"
	"github.com/rbtr/shunt/internal/gitops"
)

func TestBurnInBisectionWithRealGitStaging(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	configureBurnInGitEnv(t)

	ctx := context.Background()
	repoDir := initBurnInRepo(t)
	burn := newBurnInForge(t, repoDir, 1, 2, 3)
	burn.addPRRef(t, 1, map[string]string{"good-1.txt": "good 1\n"})
	burn.addPRRef(t, 2, map[string]string{"gate.txt": "fail\n", "bad.txt": "bad\n"})
	burn.addPRRef(t, 3, map[string]string{"good-3.txt": "good 3\n"})

	eng := New(Config{
		Owner:         "o",
		Repo:          "r",
		Base:          "main",
		StatusCtx:     "merge-queue",
		MergeStyle:    "merge",
		StagingBranch: "mq/main/staging",
		BisectFanout:  1,
	}, burn, gitops.NewStager(repoDir, "bot", "unused-token", "bot", "bot@example.invalid"))

	for i := 0; i < 12 && (len(burn.merged) < 2 || !burn.bounced[2]); i++ {
		if err := eng.Reconcile(ctx); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	if got := burn.merged; !equalInts(got, []int{1, 3}) {
		t.Fatalf("merged PRs = %v, want [1 3]", got)
	}
	if !burn.bounced[2] {
		t.Fatal("bad PR #2 was not bounced")
	}
	for _, path := range []string{"good-1.txt", "good-3.txt"} {
		if got := gitOutput(t, repoDir, "show", "main:"+path); got == "" {
			t.Fatalf("%s missing from main", path)
		}
	}
	if _, err := gitOutputErr(repoDir, "show", "main:bad.txt"); err == nil {
		t.Fatal("bad PR content landed on main")
	}
	if len(burn.staged) < 4 {
		t.Fatalf("staged batches = %v, want bisection burn-in with at least four staging runs", burn.staged)
	}
}

type burnInForge struct {
	t         *testing.T
	repoDir   string
	prs       map[int]*forge.PullRequest
	automerge map[int]bool
	merged    []int
	bounced   map[int]bool
	staged    [][]int
}

func newBurnInForge(t *testing.T, repoDir string, nums ...int) *burnInForge {
	t.Helper()
	b := &burnInForge{
		t:         t,
		repoDir:   repoDir,
		prs:       map[int]*forge.PullRequest{},
		automerge: map[int]bool{},
		bounced:   map[int]bool{},
	}
	for _, n := range nums {
		pr := &forge.PullRequest{Number: n, State: "open"}
		pr.Base.Ref = "main"
		b.prs[n] = pr
		b.automerge[n] = true
	}
	return b
}

func (b *burnInForge) addPRRef(t *testing.T, n int, files map[string]string) {
	t.Helper()
	branch := fmt.Sprintf("pr-%d", n)
	git(t, b.repoDir, "checkout", "-B", branch, "main")
	for path, body := range files {
		full := filepath.Join(b.repoDir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", full, err)
		}
	}
	git(t, b.repoDir, "add", ".")
	git(t, b.repoDir, "commit", "-m", fmt.Sprintf("PR %d", n))
	sha := gitOutput(t, b.repoDir, "rev-parse", "HEAD")
	git(t, b.repoDir, "update-ref", fmt.Sprintf("refs/pull/%d/head", n), sha)
	git(t, b.repoDir, "checkout", "main")
	b.prs[n].Head.Sha = sha
	b.prs[n].Head.Ref = branch
}

func (b *burnInForge) ListOpenPRs(_ context.Context, _, _, base string) ([]forge.PullRequest, error) {
	var out []forge.PullRequest
	for _, pr := range b.prs {
		if pr.State == "open" && (base == "" || pr.Base.Ref == base) {
			out = append(out, *pr)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out, nil
}

func (b *burnInForge) GetPR(_ context.Context, _, _ string, index int) (forge.PullRequest, error) {
	return *b.prs[index], nil
}

func (b *burnInForge) AutomergeScheduled(_ context.Context, _, _ string, index int) (bool, error) {
	return b.automerge[index], nil
}

func (b *burnInForge) RunStatus(_ context.Context, _, _, sha, _ string) (string, error) {
	prs := b.stagedPRs(sha)
	if len(prs) > 0 {
		b.staged = append(b.staged, prs)
	}
	gate, err := gitOutputErr(b.repoDir, "show", sha+":gate.txt")
	if err == nil && strings.Contains(gate, "fail") {
		return "failure", nil
	}
	return "success", nil
}

func (b *burnInForge) RunTargetURL(_ context.Context, _, _, sha, _ string) (string, error) {
	return "file://" + sha, nil
}

func (b *burnInForge) SetCommitStatus(_ context.Context, _, _, _, _, _, _, _ string) error {
	return nil
}

func (b *burnInForge) MergePR(_ context.Context, _, _ string, index int, _, headSHA string) error {
	pr := b.prs[index]
	if pr.Head.Sha != headSHA {
		return fmt.Errorf("head mismatch: got %s want %s", headSHA, pr.Head.Sha)
	}
	git(b.t, b.repoDir, "checkout", "main")
	git(b.t, b.repoDir, "merge", "--no-ff", "-m", fmt.Sprintf("merge PR %d", index), fmt.Sprintf("refs/pull/%d/head", index))
	pr.State = "closed"
	pr.Merged = true
	b.automerge[index] = false
	b.merged = append(b.merged, index)
	return nil
}

func (b *burnInForge) CancelAutomerge(_ context.Context, _, _ string, index int) error {
	b.automerge[index] = false
	b.bounced[index] = true
	return nil
}

func (b *burnInForge) Comment(_ context.Context, _, _ string, _ int, _ string) error {
	return nil
}

func (b *burnInForge) UpsertComment(_ context.Context, _, _ string, _ int, _, _, _ string) error {
	return nil
}

func (b *burnInForge) DeleteBranch(_ context.Context, _, _, branch string) error {
	_, _ = gitOutputErr(b.repoDir, "branch", "-D", branch)
	return nil
}

func (b *burnInForge) stagedPRs(sha string) []int {
	var out []int
	for n, pr := range b.prs {
		if _, err := gitOutputErr(b.repoDir, "merge-base", "--is-ancestor", pr.Head.Sha, sha); err == nil {
			out = append(out, n)
		}
	}
	sort.Ints(out)
	return out
}

func initBurnInRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, "init", "-b", "main")
	git(t, dir, "config", "user.name", "test")
	git(t, dir, "config", "user.email", "test@example.invalid")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("base\n"), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	git(t, dir, "add", ".")
	git(t, dir, "commit", "-m", "base")
	return dir
}

func configureBurnInGitEnv(t *testing.T) {
	t.Helper()
	home := t.TempDir()
	globalConfig := filepath.Join(home, ".gitconfig")
	if err := os.WriteFile(globalConfig, nil, 0o644); err != nil {
		t.Fatalf("write git config: %v", err)
	}
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", globalConfig)
	t.Setenv("GIT_CONFIG_COUNT", "3")
	t.Setenv("GIT_CONFIG_KEY_0", "commit.gpgsign")
	t.Setenv("GIT_CONFIG_VALUE_0", "false")
	t.Setenv("GIT_CONFIG_KEY_1", "merge.verifySignatures")
	t.Setenv("GIT_CONFIG_VALUE_1", "false")
	t.Setenv("GIT_CONFIG_KEY_2", "protocol.file.allow")
	t.Setenv("GIT_CONFIG_VALUE_2", "always")
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	if out, err := gitOutputErr(dir, args...); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := gitOutputErr(dir, args...)
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(out)
}

func gitOutputErr(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func equalInts(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
