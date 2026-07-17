package forge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMergePRSendsStyleAndHeadCommitID(t *testing.T) {
	for _, style := range []string{"merge", "squash", "rebase"} {
		t.Run(style, func(t *testing.T) {
			var got map[string]string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("method = %s, want POST", r.Method)
				}
				if r.URL.Path != "/api/v1/repos/o/r/pulls/7/merge" {
					t.Errorf("path = %s, want /api/v1/repos/o/r/pulls/7/merge", r.URL.Path)
				}
				if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
					t.Errorf("decode body: %v", err)
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			c := New(srv.URL, "token")
			if err := c.MergePR(context.Background(), "o", "r", 7, style, "abc123"); err != nil {
				t.Fatalf("MergePR: %v", err)
			}

			if got["Do"] != style {
				t.Errorf("Do = %q, want %q", got["Do"], style)
			}
			if got["head_commit_id"] != "abc123" {
				t.Errorf("head_commit_id = %q, want abc123", got["head_commit_id"])
			}
		})
	}
}

func TestScheduleAutomergeSendsQueuedMergeRequest(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/repos/o/r/pulls/7/merge" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.String())
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := New(srv.URL, "token")
	if err := c.ScheduleAutomerge(context.Background(), "o", "r", 7, "rebase", "abc123"); err != nil {
		t.Fatalf("ScheduleAutomerge: %v", err)
	}
	if got["Do"] != "rebase" || got["head_commit_id"] != "abc123" || got["merge_when_checks_succeed"] != true {
		t.Fatalf("request body = %#v", got)
	}
}

func TestScheduleAutomergeTreatsAlreadyScheduledAsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "pull request is already scheduled to auto merge when checks succeed", http.StatusConflict)
	}))
	defer srv.Close()

	c := New(srv.URL, "token")
	if err := c.ScheduleAutomerge(context.Background(), "o", "r", 7, "rebase", "abc123"); err != nil {
		t.Fatalf("ScheduleAutomerge: %v", err)
	}
}

func TestCancelAutomergeReportsWhetherScheduleExisted(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		cancelled bool
	}{
		{name: "cancelled", status: http.StatusNoContent, cancelled: true},
		{name: "missing", status: http.StatusNotFound, cancelled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/repos/o/r/pulls/7/merge" {
					t.Fatalf("unexpected %s %s", r.Method, r.URL.String())
				}
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			c := New(srv.URL, "token")
			cancelled, err := c.CancelAutomerge(context.Background(), "o", "r", 7)
			if err != nil {
				t.Fatalf("CancelAutomerge: %v", err)
			}
			if cancelled != tc.cancelled {
				t.Fatalf("cancelled = %v, want %v", cancelled, tc.cancelled)
			}
		})
	}
}

func TestAutomergeStatePaginatesTimeline(t *testing.T) {
	scheduledAt := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	cancelledAt := scheduledAt.Add(time.Minute)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/repos/o/r/issues/7/timeline" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("limit"); got != "50" {
			t.Fatalf("limit = %q, want 50", got)
		}
		switch r.URL.Query().Get("page") {
		case "1":
			comments := make([]timelineComment, issuePageLimit)
			comments[0] = timelineComment{ID: 1, Type: "pull_scheduled_merge", CreatedAt: scheduledAt}
			if err := json.NewEncoder(w).Encode(comments); err != nil {
				t.Fatal(err)
			}
		case "2":
			if err := json.NewEncoder(w).Encode([]timelineComment{{
				ID:        51,
				Type:      "pull_cancel_scheduled_merge",
				CreatedAt: cancelledAt,
			}}); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unexpected page %q", r.URL.Query().Get("page"))
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "token")
	state, err := c.AutomergeState(context.Background(), "o", "r", 7)
	if err != nil {
		t.Fatalf("AutomergeState: %v", err)
	}
	if state.Scheduled || !state.UpdatedAt.Equal(cancelledAt) {
		t.Fatalf("state = %#v, want latest cancellation", state)
	}
}

func TestLatestCommitStatusPaginates(t *testing.T) {
	wantTime := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/repos/o/r/commits/abc123/statuses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		switch r.URL.Query().Get("page") {
		case "1":
			statuses := make([]CommitStatus, issuePageLimit)
			for i := range statuses {
				statuses[i] = CommitStatus{ID: int64(i + 1), Context: "other"}
			}
			if err := json.NewEncoder(w).Encode(statuses); err != nil {
				t.Fatal(err)
			}
		case "2":
			if err := json.NewEncoder(w).Encode([]CommitStatus{{
				ID:          51,
				Status:      "error",
				Description: "merge queue: PR rejected",
				Context:     "merge-queue",
				CreatedAt:   wantTime,
			}}); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unexpected page %q", r.URL.Query().Get("page"))
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "token")
	status, ok, err := c.LatestCommitStatus(context.Background(), "o", "r", "abc123", "merge-queue")
	if err != nil {
		t.Fatalf("LatestCommitStatus: %v", err)
	}
	if !ok || status.Status != "error" || !status.CreatedAt.Equal(wantTime) {
		t.Fatalf("status = %#v, ok = %v", status, ok)
	}
}

func TestReadFileUsesRawEndpointAndRef(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/api/v1/repos/o/r/raw/.shunt.yml" {
			t.Errorf("path = %s, want /api/v1/repos/o/r/raw/.shunt.yml", r.URL.Path)
		}
		if got := r.URL.Query().Get("ref"); got != "main" {
			t.Errorf("ref = %q, want main", got)
		}
		_, _ = w.Write([]byte("max_batch: 3\n"))
	}))
	defer srv.Close()

	c := New(srv.URL, "token")
	got, err := c.ReadFile(context.Background(), "o", "r", "main", ".shunt.yml")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "max_batch: 3\n" {
		t.Fatalf("ReadFile = %q", got)
	}
}

func TestReadFileReturnsErrNotFound(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	c := New(srv.URL, "token")
	_, err := c.ReadFile(context.Background(), "o", "r", "main", ".shunt.yml")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReadFile error = %v, want ErrNotFound", err)
	}
}

func TestUpsertCommentCreatesWhenMarkerIsMissing(t *testing.T) {
	var posted map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/repos/o/r/issues/7/comments":
			if got := r.URL.Query().Get("limit"); got != "50" {
				t.Errorf("limit = %q, want 50", got)
			}
			_, _ = w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/repos/o/r/issues/7/comments":
			if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
				t.Errorf("decode body: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "token")
	if err := c.UpsertComment(context.Background(), "o", "r", 7, "<!-- marker -->", "mq-bot", "<!-- marker -->\nbody"); err != nil {
		t.Fatalf("UpsertComment: %v", err)
	}
	if got := posted["body"]; got != "<!-- marker -->\nbody" {
		t.Fatalf("posted body = %q", got)
	}
}

func TestUpsertCommentEditsExistingBotComment(t *testing.T) {
	var patched map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/repos/o/r/issues/7/comments":
			_, _ = w.Write([]byte(`[{"id":42,"body":"<!-- marker --> old","user":{"username":"mq-bot"}}]`))
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/repos/o/r/issues/comments/42":
			if err := json.NewDecoder(r.Body).Decode(&patched); err != nil {
				t.Errorf("decode body: %v", err)
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "token")
	if err := c.UpsertComment(context.Background(), "o", "r", 7, "<!-- marker -->", "mq-bot", "<!-- marker --> new"); err != nil {
		t.Fatalf("UpsertComment: %v", err)
	}
	if got := patched["body"]; got != "<!-- marker --> new" {
		t.Fatalf("patched body = %q", got)
	}
}

func TestUpsertCommentDoesNotEditAnotherUsersMarker(t *testing.T) {
	var sawPatch bool
	var posted map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/repos/o/r/issues/7/comments":
			_, _ = w.Write([]byte(`[{"id":42,"body":"<!-- marker --> old","user":{"username":"someone-else"}}]`))
		case r.Method == http.MethodPatch && strings.Contains(r.URL.Path, "/issues/comments/42"):
			sawPatch = true
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/repos/o/r/issues/7/comments":
			if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
				t.Errorf("decode body: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "token")
	if err := c.UpsertComment(context.Background(), "o", "r", 7, "<!-- marker -->", "mq-bot", "<!-- marker --> new"); err != nil {
		t.Fatalf("UpsertComment: %v", err)
	}
	if sawPatch {
		t.Fatal("UpsertComment patched another user's marker comment")
	}
	if got := posted["body"]; got != "<!-- marker --> new" {
		t.Fatalf("posted body = %q", got)
	}
}

func TestUpsertCommentFindsMarkerOnLaterPage(t *testing.T) {
	var patched map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/repos/o/r/issues/7/comments":
			switch r.URL.Query().Get("page") {
			case "1":
				comments := make([]IssueComment, issuePageLimit)
				for i := range comments {
					comments[i].ID = int64(i + 1)
					comments[i].Body = "unrelated"
				}
				if err := json.NewEncoder(w).Encode(comments); err != nil {
					t.Fatal(err)
				}
			case "2":
				_, _ = w.Write([]byte(`[{"id":99,"body":"<!-- marker --> old","user":{"username":"mq-bot"}}]`))
			default:
				t.Fatalf("unexpected page %q", r.URL.Query().Get("page"))
			}
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/repos/o/r/issues/comments/99":
			if err := json.NewDecoder(r.Body).Decode(&patched); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "token")
	if err := c.UpsertComment(context.Background(), "o", "r", 7, "<!-- marker -->", "mq-bot", "<!-- marker --> new"); err != nil {
		t.Fatalf("UpsertComment: %v", err)
	}
	if got := patched["body"]; got != "<!-- marker --> new" {
		t.Fatalf("patched body = %q", got)
	}
}

func TestRunStatusAggregatesNewestMatchingTaskRun(t *testing.T) {
	payload := tasksResponse{WorkflowRuns: []workflowTask{
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "running"},
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success"},
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success"},
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 7, WorkflowID: "ci.yaml", Status: "success"},
	}}
	c := newRunStatusTestClient(t, payload)

	status, err := c.RunStatus(context.Background(), "o", "r", "sha", "mq/main/staging")
	if err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("RunStatus = %q, want running", status)
	}
}

func TestRunStatusFailsIfAnyNewestMatchingTaskFails(t *testing.T) {
	payload := tasksResponse{WorkflowRuns: []workflowTask{
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "running"},
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "failure"},
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 7, WorkflowID: "ci.yaml", Status: "success"},
	}}
	c := newRunStatusTestClient(t, payload)

	status, err := c.RunStatus(context.Background(), "o", "r", "sha", "mq/main/staging")
	if err != nil {
		t.Fatal(err)
	}
	if status != "failure" {
		t.Fatalf("RunStatus = %q, want failure", status)
	}
}

func TestRunStatusSucceedsWhenNewestMatchingTasksAreTerminalGreen(t *testing.T) {
	payload := tasksResponse{WorkflowRuns: []workflowTask{
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "skipped"},
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success"},
		{HeadSHA: "other", HeadBranch: "mq/main/staging", RunNumber: 9, WorkflowID: "ci.yaml", Status: "running"},
	}}
	c := newRunStatusTestClient(t, payload)

	status, err := c.RunStatus(context.Background(), "o", "r", "sha", "mq/main/staging")
	if err != nil {
		t.Fatal(err)
	}
	if status != "success" {
		t.Fatalf("RunStatus = %q, want success", status)
	}
}

func TestRunStatusUsesRunAggregateBeforeMaterializedTaskRows(t *testing.T) {
	c := newRunStatusDualEndpointClient(t,
		runsResponse{WorkflowRuns: []workflowRun{
			{CommitSHA: "sha", PrettyRef: "mq/main/staging", IndexInRepo: 8, WorkflowID: "ci.yaml", Status: "running"},
		}},
		tasksResponse{WorkflowRuns: []workflowTask{
			{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success"},
			{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success"},
		}},
	)

	status, err := c.RunStatus(context.Background(), "o", "r", "sha", "mq/main/staging")
	if err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("RunStatus = %q, want running", status)
	}
}

func TestRunStatusWaitsWhenRunEndpointHasNoMatchingRun(t *testing.T) {
	c := newRunStatusDualEndpointClient(t,
		runsResponse{WorkflowRuns: []workflowRun{
			{CommitSHA: "other", PrettyRef: "mq/main/staging", IndexInRepo: 9, WorkflowID: "ci.yaml", Status: "success"},
		}},
		tasksResponse{WorkflowRuns: []workflowTask{
			{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success"},
		}},
	)

	status, err := c.RunStatus(context.Background(), "o", "r", "sha", "mq/main/staging")
	if err != nil {
		t.Fatal(err)
	}
	if status != "" {
		t.Fatalf("RunStatus = %q, want wait/unknown", status)
	}
}

func TestRunStatusReadsPaginatedRuns(t *testing.T) {
	firstPage := make([]workflowRun, runPageLimit)
	for i := range firstPage {
		firstPage[i] = workflowRun{CommitSHA: "other", PrettyRef: "mq/main/staging", IndexInRepo: i + 1, WorkflowID: "ci.yaml", Status: "success"}
	}
	c := newRunStatusPagedDualEndpointClient(t,
		[]runsResponse{
			{WorkflowRuns: firstPage},
			{WorkflowRuns: []workflowRun{
				{CommitSHA: "sha", PrettyRef: "mq/main/staging", IndexInRepo: runPageLimit + 1, WorkflowID: "ci.yaml", Status: "running"},
			}},
		},
		tasksResponse{WorkflowRuns: []workflowTask{
			{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success"},
		}},
	)

	status, err := c.RunStatus(context.Background(), "o", "r", "sha", "mq/main/staging")
	if err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("RunStatus = %q, want running from paginated run", status)
	}
}

func TestRunStatusReadsPaginatedTasks(t *testing.T) {
	firstPage := make([]workflowTask, taskPageLimit)
	for i := range firstPage {
		firstPage[i] = workflowTask{HeadSHA: "other", HeadBranch: "mq/main/staging", RunNumber: 9, WorkflowID: "ci.yaml", Status: "success"}
	}
	c := newRunStatusPagedTestClient(t,
		tasksResponse{WorkflowRuns: firstPage},
		tasksResponse{WorkflowRuns: []workflowTask{
			{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success"},
		}},
	)

	status, err := c.RunStatus(context.Background(), "o", "r", "sha", "mq/main/staging")
	if err != nil {
		t.Fatal(err)
	}
	if status != "success" {
		t.Fatalf("RunStatus = %q, want success", status)
	}
}

func TestRunTargetURLReturnsNewestMatchingHTMLURL(t *testing.T) {
	payload := tasksResponse{WorkflowRuns: []workflowTask{
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success", HTMLURL: "https://forge/o/r/actions/runs/8"},
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 7, WorkflowID: "ci.yaml", Status: "success", HTMLURL: "https://forge/o/r/actions/runs/7"},
		{HeadSHA: "other", HeadBranch: "mq/main/staging", RunNumber: 9, WorkflowID: "ci.yaml", Status: "success", HTMLURL: "https://forge/o/r/actions/runs/9"},
	}}
	c := newRunStatusTestClient(t, payload)

	u, err := c.RunTargetURL(context.Background(), "o", "r", "sha", "mq/main/staging")
	if err != nil {
		t.Fatal(err)
	}
	if u != "https://forge/o/r/actions/runs/8" {
		t.Fatalf("RunTargetURL = %q, want newest matching run URL", u)
	}
}

func TestRunTargetURLUsesRunAggregateURL(t *testing.T) {
	c := newRunStatusDualEndpointClient(t,
		runsResponse{WorkflowRuns: []workflowRun{
			{CommitSHA: "sha", PrettyRef: "mq/main/staging", IndexInRepo: 8, WorkflowID: "ci.yaml", Status: "success", HTMLURL: "https://forge/o/r/actions/runs/8"},
		}},
		tasksResponse{WorkflowRuns: []workflowTask{
			{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success", HTMLURL: "https://forge/o/r/actions/tasks/8"},
		}},
	)

	u, err := c.RunTargetURL(context.Background(), "o", "r", "sha", "mq/main/staging")
	if err != nil {
		t.Fatal(err)
	}
	if u != "https://forge/o/r/actions/runs/8" {
		t.Fatalf("RunTargetURL = %q, want run aggregate URL", u)
	}
}

func TestRunTargetURLFallsBackToTargetURL(t *testing.T) {
	payload := tasksResponse{WorkflowRuns: []workflowTask{
		{HeadSHA: "sha", HeadBranch: "mq/main/staging", RunNumber: 8, WorkflowID: "ci.yaml", Status: "success", TargetURL: "https://forge/o/r/actions/runs/8"},
	}}
	c := newRunStatusTestClient(t, payload)

	u, err := c.RunTargetURL(context.Background(), "o", "r", "sha", "mq/main/staging")
	if err != nil {
		t.Fatal(err)
	}
	if u != "https://forge/o/r/actions/runs/8" {
		t.Fatalf("RunTargetURL = %q, want target URL", u)
	}
}

func newRunStatusTestClient(t *testing.T, payload tasksResponse) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/repos/o/r/actions/runs" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path != "/api/v1/repos/o/r/actions/tasks" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Fatal(err)
		}
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, "token")
}

func newRunStatusDualEndpointClient(t *testing.T, runs runsResponse, tasks tasksResponse) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/repos/o/r/actions/runs":
			if err := json.NewEncoder(w).Encode(runs); err != nil {
				t.Fatal(err)
			}
		case "/api/v1/repos/o/r/actions/tasks":
			if err := json.NewEncoder(w).Encode(tasks); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, "token")
}

func newRunStatusPagedDualEndpointClient(t *testing.T, runs []runsResponse, tasks tasksResponse) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/repos/o/r/actions/runs":
			page := r.URL.Query().Get("page")
			if page == "" {
				page = "1"
			}
			pageNum, err := strconv.Atoi(page)
			if err != nil {
				t.Fatalf("invalid page %q", page)
			}
			idx := pageNum - 1
			if idx < 0 || idx >= len(runs) {
				if err := json.NewEncoder(w).Encode(runsResponse{}); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err := json.NewEncoder(w).Encode(runs[idx]); err != nil {
				t.Fatal(err)
			}
		case "/api/v1/repos/o/r/actions/tasks":
			if err := json.NewEncoder(w).Encode(tasks); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, "token")
}

func newRunStatusPagedTestClient(t *testing.T, pages ...tasksResponse) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/repos/o/r/actions/runs" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path != "/api/v1/repos/o/r/actions/tasks" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		page := r.URL.Query().Get("page")
		index := 0
		if page == "2" {
			index = 1
		}
		if index >= len(pages) {
			t.Fatalf("unexpected page %s", page)
		}
		if err := json.NewEncoder(w).Encode(pages[index]); err != nil {
			t.Fatal(err)
		}
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, "token")
}
