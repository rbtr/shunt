// Package forge is a minimal Forgejo (Gitea-compatible) API client covering
// exactly what the merge queue needs: discovering auto-merge PRs, driving the
// required status check, merging, and reading gate-CI results.
package forge

import (
	"bytes"
	"context"
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

type IssueComment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
	User struct {
		UserName  string `json:"username"`
		Login     string `json:"login"`
		LoginName string `json:"login_name"`
	} `json:"user"`
}

type AutomergeState struct {
	Scheduled bool
	UpdatedAt time.Time
}

type CommitStatus struct {
	ID          int64     `json:"id"`
	Status      string    `json:"status"`
	Description string    `json:"description"`
	Context     string    `json:"context"`
	CreatedAt   time.Time `json:"created_at"`
}

type timelineComment struct {
	ID        int64     `json:"id"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
}

type workflowTask struct {
	HeadSHA    string `json:"head_sha"`
	HeadBranch string `json:"head_branch"`
	RunNumber  int    `json:"run_number"`
	WorkflowID string `json:"workflow_id"`
	Status     string `json:"status"`
	Event      string `json:"event"`
	HTMLURL    string `json:"html_url"`
	TargetURL  string `json:"target_url"`
}

type tasksResponse struct {
	WorkflowRuns []workflowTask `json:"workflow_runs"`
}

type workflowRun struct {
	CommitSHA   string `json:"commit_sha"`
	PrettyRef   string `json:"prettyref"`
	IndexInRepo int    `json:"index_in_repo"`
	WorkflowID  string `json:"workflow_id"`
	Status      string `json:"status"`
	HTMLURL     string `json:"html_url"`
}

type runsResponse struct {
	WorkflowRuns []workflowRun `json:"workflow_runs"`
}

const taskPageLimit = 100
const runPageLimit = 50
const issuePageLimit = 50

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.apiBase+path, rdr)
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

func (c *Client) doRaw(ctx context.Context, method, path string, body any) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.apiBase+path, rdr)
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
func (c *Client) ListOpenPRs(ctx context.Context, owner, repo, base string) ([]PullRequest, error) {
	var prs []PullRequest
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls?state=open&limit=50", repoPath(owner, repo)), nil, &prs); err != nil {
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
func (c *Client) GetPR(ctx context.Context, owner, repo string, index int) (PullRequest, error) {
	var pr PullRequest
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d", repoPath(owner, repo), index), nil, &pr)
	return pr, err
}

func (c *Client) ReadFile(ctx context.Context, owner, repo, ref, path string) ([]byte, error) {
	u := fmt.Sprintf("/repos/%s/raw/%s", repoPath(owner, repo), url.PathEscape(path))
	if ref != "" {
		u += "?ref=" + url.QueryEscape(ref)
	}
	return c.doRaw(ctx, http.MethodGet, u, nil)
}

// AutomergeState returns the latest scheduled-auto-merge transition recorded in
// the PR timeline. Forgejo does not expose the live state on the PR object.
func (c *Client) AutomergeState(ctx context.Context, owner, repo string, index int) (AutomergeState, error) {
	var latest timelineComment
	for page := 1; ; page++ {
		var comments []timelineComment
		path := fmt.Sprintf("/repos/%s/issues/%d/timeline?limit=%d&page=%d", repoPath(owner, repo), index, issuePageLimit, page)
		if err := c.do(ctx, http.MethodGet, path, nil, &comments); err != nil {
			return AutomergeState{}, err
		}
		for _, comment := range comments {
			if comment.Type != "pull_scheduled_merge" && comment.Type != "pull_cancel_scheduled_merge" {
				continue
			}
			if later(comment.ID, comment.CreatedAt, latest.ID, latest.CreatedAt) {
				latest = comment
			}
		}
		if len(comments) < issuePageLimit {
			break
		}
	}
	return AutomergeState{
		Scheduled: latest.Type == "pull_scheduled_merge",
		UpdatedAt: latest.CreatedAt,
	}, nil
}

func (c *Client) AutomergeScheduled(ctx context.Context, owner, repo string, index int) (bool, error) {
	state, err := c.AutomergeState(ctx, owner, repo, index)
	return state.Scheduled, err
}

func (c *Client) LatestCommitStatus(ctx context.Context, owner, repo, sha, statusContext string) (CommitStatus, bool, error) {
	for page := 1; ; page++ {
		var statuses []CommitStatus
		path := fmt.Sprintf(
			"/repos/%s/commits/%s/statuses?limit=%d&page=%d",
			repoPath(owner, repo),
			url.PathEscape(sha),
			issuePageLimit,
			page,
		)
		if err := c.do(ctx, http.MethodGet, path, nil, &statuses); err != nil {
			return CommitStatus{}, false, err
		}
		var latest CommitStatus
		for _, status := range statuses {
			if status.Context != statusContext {
				continue
			}
			if latest.ID == 0 || later(status.ID, status.CreatedAt, latest.ID, latest.CreatedAt) {
				latest = status
			}
		}
		if latest.ID != 0 {
			return latest, true, nil
		}
		if len(statuses) < issuePageLimit {
			break
		}
	}
	return CommitStatus{}, false, nil
}

// RunStatus returns the aggregate gate workflow status for (sha, branch), or ""
// if no run exists yet. Prefer Forgejo's run-level status because dependent task
// rows are materialized lazily in multi-job workflows.
func (c *Client) RunStatus(ctx context.Context, owner, repo, sha, branch string) (string, error) {
	runs, err := c.listActionRuns(ctx, owner, repo)
	if err == nil {
		if run := latestMatchingRun(runs, sha, branch); run != nil {
			if run.Status != "" {
				return run.Status, nil
			}
		}
		return "", nil
	} else if !strings.Contains(err.Error(), "http 404") {
		return "", err
	}

	tasks, err := c.listActionTasks(ctx, owner, repo)
	if err != nil {
		return "", err
	}

	var runNumber int
	var workflowID string
	for _, task := range tasks {
		if task.HeadSHA == sha && (branch == "" || task.HeadBranch == branch) {
			if task.RunNumber > runNumber {
				runNumber = task.RunNumber
				workflowID = task.WorkflowID
			}
		}
	}
	if runNumber == 0 {
		return "", nil
	}

	sawTerminal := false
	pendingStatus := ""
	for _, task := range tasks {
		if task.HeadSHA != sha || (branch != "" && task.HeadBranch != branch) {
			continue
		}
		if task.RunNumber != runNumber || task.WorkflowID != workflowID {
			continue
		}
		switch task.Status {
		case "failure", "cancelled", "error":
			return task.Status, nil
		case "success", "skipped":
			sawTerminal = true
		default:
			if pendingStatus == "" {
				pendingStatus = task.Status
			}
		}
	}
	if pendingStatus != "" {
		return pendingStatus, nil
	}
	if sawTerminal {
		return "success", nil
	}
	return "", nil
}

// RunTargetURL returns a browser/debug URL for the newest matching staging run
// when Forgejo/Gitea exposes one. Not every version includes this in the task
// payload, so an empty URL is a valid "not available" result.
func (c *Client) RunTargetURL(ctx context.Context, owner, repo, sha, branch string) (string, error) {
	runs, err := c.listActionRuns(ctx, owner, repo)
	if err == nil {
		if run := latestMatchingRun(runs, sha, branch); run != nil {
			if run.HTMLURL != "" {
				return run.HTMLURL, nil
			}
		}
	} else if !strings.Contains(err.Error(), "http 404") {
		return "", err
	}

	tasks, err := c.listActionTasks(ctx, owner, repo)
	if err != nil {
		return "", err
	}
	runNumber, workflowID := latestRun(tasks, sha, branch)
	if runNumber == 0 {
		return "", nil
	}
	for _, task := range tasks {
		if task.HeadSHA != sha || (branch != "" && task.HeadBranch != branch) {
			continue
		}
		if task.RunNumber != runNumber || task.WorkflowID != workflowID {
			continue
		}
		if task.HTMLURL != "" {
			return task.HTMLURL, nil
		}
		if task.TargetURL != "" {
			return task.TargetURL, nil
		}
	}
	return "", nil
}

func latestMatchingRun(runs []workflowRun, sha, branch string) *workflowRun {
	var out *workflowRun
	for i := range runs {
		run := &runs[i]
		if run.CommitSHA != sha || (branch != "" && run.PrettyRef != branch) {
			continue
		}
		if out == nil || run.IndexInRepo > out.IndexInRepo {
			out = run
		}
	}
	return out
}

func latestRun(tasks []workflowTask, sha, branch string) (int, string) {
	var runNumber int
	var workflowID string
	for _, task := range tasks {
		if task.HeadSHA == sha && (branch == "" || task.HeadBranch == branch) {
			if task.RunNumber > runNumber {
				runNumber = task.RunNumber
				workflowID = task.WorkflowID
			}
		}
	}
	return runNumber, workflowID
}

func (c *Client) listActionRuns(ctx context.Context, owner, repo string) ([]workflowRun, error) {
	var out []workflowRun
	for page := 1; ; page++ {
		var rr runsResponse
		if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/actions/runs?limit=%d&page=%d", repoPath(owner, repo), runPageLimit, page), nil, &rr); err != nil {
			return nil, err
		}
		out = append(out, rr.WorkflowRuns...)
		if len(rr.WorkflowRuns) < runPageLimit {
			return out, nil
		}
	}
}

func (c *Client) listActionTasks(ctx context.Context, owner, repo string) ([]workflowTask, error) {
	var out []workflowTask
	for page := 1; ; page++ {
		var tr tasksResponse
		if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/actions/tasks?limit=%d&page=%d", repoPath(owner, repo), taskPageLimit, page), nil, &tr); err != nil {
			return nil, err
		}
		out = append(out, tr.WorkflowRuns...)
		if len(tr.WorkflowRuns) < taskPageLimit {
			return out, nil
		}
	}
}

func (c *Client) SetCommitStatus(ctx context.Context, owner, repo, sha, statusContext, state, desc, targetURL string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/statuses/%s", repoPath(owner, repo), url.PathEscape(sha)), map[string]string{
		"state": state, "context": statusContext, "description": desc, "target_url": targetURL,
	}, nil)
}

func (c *Client) ScheduleAutomerge(ctx context.Context, owner, repo string, index int, style, headSHA string) error {
	err := c.do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/pulls/%d/merge", repoPath(owner, repo), index), map[string]any{
		"Do":                        style,
		"head_commit_id":            headSHA,
		"merge_when_checks_succeed": true,
	}, nil)
	if err != nil &&
		strings.Contains(err.Error(), "http 409") &&
		strings.Contains(strings.ToLower(err.Error()), "already scheduled") {
		return nil
	}
	return err
}

// CancelAutomerge reports whether a live scheduled merge was removed.
func (c *Client) CancelAutomerge(ctx context.Context, owner, repo string, index int) (bool, error) {
	_, err := c.doRaw(ctx, http.MethodDelete, fmt.Sprintf("/repos/%s/pulls/%d/merge", repoPath(owner, repo), index), nil)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (c *Client) Comment(ctx context.Context, owner, repo string, index int, body string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/comments", repoPath(owner, repo), index), map[string]string{"body": body}, nil)
}

func (c *Client) UpsertComment(ctx context.Context, owner, repo string, index int, marker, botUser, body string) error {
	comments, err := c.listIssueComments(ctx, owner, repo, index)
	if err != nil {
		return err
	}
	var existing IssueComment
	for _, comment := range comments {
		if !strings.Contains(comment.Body, marker) {
			continue
		}
		if botUser != "" && !commentByUser(comment, botUser) {
			continue
		}
		if comment.ID > existing.ID {
			existing = comment
		}
	}
	if existing.ID == 0 {
		return c.Comment(ctx, owner, repo, index, body)
	}
	if existing.Body == body {
		return nil
	}
	return c.do(ctx, http.MethodPatch, fmt.Sprintf("/repos/%s/issues/comments/%d", repoPath(owner, repo), existing.ID), map[string]string{"body": body}, nil)
}

func (c *Client) listIssueComments(ctx context.Context, owner, repo string, index int) ([]IssueComment, error) {
	var out []IssueComment
	for page := 1; ; page++ {
		var comments []IssueComment
		path := fmt.Sprintf("/repos/%s/issues/%d/comments?limit=%d&page=%d", repoPath(owner, repo), index, issuePageLimit, page)
		if err := c.do(ctx, http.MethodGet, path, nil, &comments); err != nil {
			return nil, err
		}
		out = append(out, comments...)
		if len(comments) < issuePageLimit {
			return out, nil
		}
	}
}

func repoPath(owner, repo string) string {
	return url.PathEscape(owner) + "/" + url.PathEscape(repo)
}

func commentByUser(comment IssueComment, botUser string) bool {
	return comment.User.UserName == botUser || comment.User.Login == botUser || comment.User.LoginName == botUser
}

func later(id int64, createdAt time.Time, otherID int64, otherCreatedAt time.Time) bool {
	return createdAt.After(otherCreatedAt) || (createdAt.Equal(otherCreatedAt) && id > otherID)
}
