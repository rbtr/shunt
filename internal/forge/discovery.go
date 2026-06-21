package forge

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// RepoRef identifies a repository discovered for queue management.
type RepoRef struct {
	Owner         string
	Name          string
	DefaultBranch string
}

type repoSearchItem struct {
	Name          string `json:"name"`
	DefaultBranch string `json:"default_branch"`
	Archived      bool   `json:"archived"`
	Owner         struct {
		Login string `json:"login"`
	} `json:"owner"`
}

type repoSearchResp struct {
	Data []repoSearchItem `json:"data"`
}

// SearchReposByTopic returns non-archived repos carrying the given topic.
func (c *Client) SearchReposByTopic(topic string) ([]RepoRef, error) {
	var r repoSearchResp
	if err := c.do(http.MethodGet, fmt.Sprintf("/repos/search?q=%s&topic=true&limit=50", url.QueryEscape(topic)), nil, &r); err != nil {
		return nil, err
	}
	var out []RepoRef
	for _, it := range r.Data {
		if it.Archived {
			continue
		}
		out = append(out, RepoRef{Owner: it.Owner.Login, Name: it.Name, DefaultBranch: it.DefaultBranch})
	}
	return out, nil
}

// BranchProtection is the subset of a Forgejo branch-protection rule we manage.
type BranchProtection struct {
	EnableStatusCheck      bool     `json:"enable_status_check"`
	StatusCheckContexts    []string `json:"status_check_contexts"`
	EnablePush             bool     `json:"enable_push"`
	EnablePushWhitelist    bool     `json:"enable_push_whitelist"`
	PushWhitelistUsernames []string `json:"push_whitelist_usernames"`
}

// EnsureBranchProtection makes sure base requires statusCtx and that botUser may
// push. It is additive: it never removes existing contexts or whitelist entries.
func (c *Client) EnsureBranchProtection(owner, repo, base, statusCtx, botUser string) (changed bool, err error) {
	var bp BranchProtection
	getErr := c.do(http.MethodGet, fmt.Sprintf("/repos/%s/%s/branch_protections/%s", owner, repo, base), nil, &bp)
	if getErr != nil {
		if !strings.Contains(getErr.Error(), "http 404") {
			return false, getErr
		}
		body := map[string]any{
			"rule_name":                base,
			"enable_status_check":      true,
			"status_check_contexts":    []string{statusCtx},
			"enable_push":              true,
			"enable_push_whitelist":    true,
			"push_whitelist_usernames": []string{botUser},
			"required_approvals":       0,
			"block_on_outdated_branch": false,
		}
		return true, c.do(http.MethodPost, fmt.Sprintf("/repos/%s/%s/branch_protections", owner, repo), body, nil)
	}
	ctxs, wl := bp.StatusCheckContexts, bp.PushWhitelistUsernames
	need := false
	if !contains(ctxs, statusCtx) {
		ctxs = append(ctxs, statusCtx)
		need = true
	}
	if !contains(wl, botUser) {
		wl = append(wl, botUser)
		need = true
	}
	if !bp.EnableStatusCheck || !bp.EnablePush || !bp.EnablePushWhitelist {
		need = true
	}
	if !need {
		return false, nil
	}
	body := map[string]any{
		"enable_status_check":      true,
		"status_check_contexts":    ctxs,
		"enable_push":              true,
		"enable_push_whitelist":    true,
		"push_whitelist_usernames": wl,
	}
	return true, c.do(http.MethodPatch, fmt.Sprintf("/repos/%s/%s/branch_protections/%s", owner, repo, base), body, nil)
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
