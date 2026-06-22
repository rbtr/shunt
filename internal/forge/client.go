// Package forge is a minimal Forgejo (Gitea-compatible) API client covering
// exactly what the merge queue needs: discovering auto-merge PRs, driving the
// required status check, merging, and reading gate-CI results.
package forge

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var ErrNotFound = errors.New("forge: not found")

type Client struct {
	apiBase string
	token   string
	http    *http.Client
}

func New(instanceURL, token string) *Client {
	return &Client{
		apiBase: strings.TrimRight(instanceURL, "/") + "/api/v1",
		token:   token,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

type PullRequest struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	State  string `json:"state"`
	Merged bool   `json:"merged"`
	Head   struct {
		Sha string `json:"sha"`
		Ref string `json:"ref"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
}

type Branch struct {
	Name string `json:"name"`
}

type timelineComment struct {
	Type string `json:"type"`
}

type workflowRun struct {
	HeadSHA    string `json:"head_sha"`
	HeadBranch string `json:"head_branch"`
	Status     string `json:"status"`
	Event      string `json:"event"`
}

type tasksResponse struct {
	WorkflowRuns []workflowRun `json:"workflow_runs"`
}

const branchPageLimit = 50

func (c *Client) do(method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.apiBase+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "token "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: http %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func (c *Client) doRaw(method, path string, body any) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.apiBase+path, rdr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "token "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %s %s", ErrNotFound, method, path)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: http %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}

// ListOpenPRs returns open PRs, optionally filtered to those targeting base.
func (c *Client) ListOpenPRs(owner, repo, base string) ([]PullRequest, error) {
	var prs []PullRequest
	if err := c.do(http.MethodGet, fmt.Sprintf("/repos/%s/pulls?state=open&limit=50", repoPath(owner, repo)), nil, &prs); err != nil {
		return nil, err
	}
	if base == "" {
		return prs, nil
	}
	var out []PullRequest
	for _, p := range prs {
		if p.Base.Ref == base {
			out = append(out, p)
		}
	}
	return out, nil
}

// GetPR fetches a single pull request.
func (c *Client) GetPR(owner, repo string, index int) (PullRequest, error) {
	var pr PullRequest
	err := c.do(http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d", repoPath(owner, repo), index), nil, &pr)
	return pr, err
}

func (c *Client) ReadFile(owner, repo, ref, path string) ([]byte, error) {
	u := fmt.Sprintf("/repos/%s/raw/%s", repoPath(owner, repo), url.PathEscape(path))
	if ref != "" {
		u += "?ref=" + url.QueryEscape(ref)
	}
	return c.doRaw(http.MethodGet, u, nil)
}

// AutomergeScheduled scans the PR timeline newest-first for the canonical
// auto-merge signals (Forgejo does not expose this on the PR object).
func (c *Client) AutomergeScheduled(owner, repo string, index int) (bool, error) {
	var tl []timelineComment
	if err := c.do(http.MethodGet, fmt.Sprintf("/repos/%s/issues/%d/timeline", repoPath(owner, repo), index), nil, &tl); err != nil {
		return false, err
	}
	for i := len(tl) - 1; i >= 0; i-- {
		switch tl[i].Type {
		case "pull_scheduled_merge":
			return true, nil
		case "pull_cancel_scheduled_merge":
			return false, nil
		}
	}
	return false, nil
}

// RunStatus returns the gate workflow run status for (sha, branch), or "" if no
// run exists yet. Forgejo populates `status` (success/failure/running/...); the
// `conclusion` field is unused in this version.
func (c *Client) RunStatus(owner, repo, sha, branch string) (string, error) {
	var tr tasksResponse
	if err := c.do(http.MethodGet, fmt.Sprintf("/repos/%s/actions/tasks?limit=50", repoPath(owner, repo)), nil, &tr); err != nil {
		return "", err
	}
	for _, r := range tr.WorkflowRuns {
		if r.HeadSHA == sha && (branch == "" || r.HeadBranch == branch) {
			return r.Status, nil
		}
	}
	return "", nil
}

func (c *Client) SetCommitStatus(owner, repo, sha, context, state, desc, targetURL string) error {
	return c.do(http.MethodPost, fmt.Sprintf("/repos/%s/statuses/%s", repoPath(owner, repo), url.PathEscape(sha)), map[string]string{
		"state": state, "context": context, "description": desc, "target_url": targetURL,
	}, nil)
}

func (c *Client) MergePR(owner, repo string, index int, style, headSHA string) error {
	return c.do(http.MethodPost, fmt.Sprintf("/repos/%s/pulls/%d/merge", repoPath(owner, repo), index), map[string]any{
		"Do": style, "head_commit_id": headSHA,
	}, nil)
}

// CancelAutomerge removes a scheduled auto-merge; a 404 (none scheduled) is ok.
func (c *Client) CancelAutomerge(owner, repo string, index int) error {
	err := c.do(http.MethodDelete, fmt.Sprintf("/repos/%s/pulls/%d/merge", repoPath(owner, repo), index), nil, nil)
	if err != nil && strings.Contains(err.Error(), "http 404") {
		return nil
	}
	return err
}

func (c *Client) Comment(owner, repo string, index int, body string) error {
	return c.do(http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/comments", repoPath(owner, repo), index), map[string]string{"body": body}, nil)
}

func (c *Client) ListBranches(owner, repo string) ([]Branch, error) {
	var out []Branch
	for page := 1; ; page++ {
		var branches []Branch
		if err := c.do(http.MethodGet, fmt.Sprintf("/repos/%s/branches?limit=%d&page=%d", repoPath(owner, repo), branchPageLimit, page), nil, &branches); err != nil {
			return nil, err
		}
		out = append(out, branches...)
		if len(branches) < branchPageLimit {
			return out, nil
		}
	}
}

// PruneStagingBranches deletes stale shunt-owned staging branches for base.
func (c *Client) PruneStagingBranches(owner, repo, base string) ([]string, error) {
	branches, err := c.ListBranches(owner, repo)
	if err != nil {
		return nil, err
	}
	var deleted []string
	for _, branch := range branches {
		if !isShuntStagingBranch(base, branch.Name) {
			continue
		}
		if err := c.DeleteBranch(owner, repo, branch.Name); err != nil {
			return deleted, fmt.Errorf("delete branch %q: %w", branch.Name, err)
		}
		deleted = append(deleted, branch.Name)
	}
	return deleted, nil
}

func (c *Client) DeleteBranch(owner, repo, branch string) error {
	err := c.do(http.MethodDelete, fmt.Sprintf("/repos/%s/branches/%s", repoPath(owner, repo), url.PathEscape(branch)), nil, nil)
	if err != nil && strings.Contains(err.Error(), "http 404") {
		return nil
	}
	return err
}

func isShuntStagingBranch(base, branch string) bool {
	staging := "mq/" + base + "/staging"
	if branch == staging {
		return true
	}
	suffix, ok := strings.CutPrefix(branch, staging+"-")
	if !ok || suffix == "" {
		return false
	}
	for _, r := range suffix {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func repoPath(owner, repo string) string {
	return url.PathEscape(owner) + "/" + url.PathEscape(repo)
}
