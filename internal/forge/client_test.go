package forge

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
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
			if err := c.MergePR("o", "r", 7, style, "abc123"); err != nil {
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

func TestPruneStagingBranchesDeletesOnlyShuntStagingBranches(t *testing.T) {
	branches := []Branch{
		{Name: "main"},
		{Name: "mq/main/staging"},
		{Name: "mq/main/staging-1"},
		{Name: "mq/main/staging-27"},
		{Name: "mq/main/staging-old"},
		{Name: "mq/main/other"},
		{Name: "mq/release/staging"},
		{Name: "feature/mq/main/staging"},
	}
	var deleted []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/repos/o/r/branches":
			if r.URL.Query().Get("page") == "1" {
				_ = json.NewEncoder(w).Encode(branches)
				return
			}
			_ = json.NewEncoder(w).Encode([]Branch{})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.EscapedPath(), "/api/v1/repos/o/r/branches/"):
			name, err := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/api/v1/repos/o/r/branches/"))
			if err != nil {
				t.Errorf("unescape branch path: %v", err)
			}
			deleted = append(deleted, name)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, "token")
	got, err := c.PruneStagingBranches("o", "r", "main")
	if err != nil {
		t.Fatalf("PruneStagingBranches: %v", err)
	}

	sort.Strings(got)
	sort.Strings(deleted)
	want := []string{"mq/main/staging", "mq/main/staging-1", "mq/main/staging-27"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("pruned = %v, want %v", got, want)
	}
	if strings.Join(deleted, ",") != strings.Join(want, ",") {
		t.Fatalf("deleted = %v, want %v", deleted, want)
	}
}

func TestIsShuntStagingBranchSupportsSlashedBases(t *testing.T) {
	for _, tc := range []struct {
		branch string
		want   bool
	}{
		{branch: "mq/release/v1/staging", want: true},
		{branch: "mq/release/v1/staging-2", want: true},
		{branch: "mq/release/v1/staging-old", want: false},
		{branch: "mq/release/staging", want: false},
	} {
		if got := isShuntStagingBranch("release/v1", tc.branch); got != tc.want {
			t.Errorf("isShuntStagingBranch(%q) = %v, want %v", tc.branch, got, tc.want)
		}
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
	got, err := c.ReadFile("o", "r", "main", ".shunt.yml")
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
	_, err := c.ReadFile("o", "r", "main", ".shunt.yml")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReadFile error = %v, want ErrNotFound", err)
	}
}
