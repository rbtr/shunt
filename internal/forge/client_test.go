package forge

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
