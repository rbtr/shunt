package forge

import (
	"context"
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
func (c *Client) SearchReposByTopic(ctx context.Context, topic string) ([]RepoRef, error) {
	var r repoSearchResp
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/search?q=%s&topic=true&limit=50", url.QueryEscape(topic)), nil, &r); err != nil {
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

// Hook is the subset of a Forgejo/Gitea repository webhook we manage.
type Hook struct {
	ID     int64             `json:"id"`
	Type   string            `json:"type"`
	Config map[string]string `json:"config"`
	Events []string          `json:"events"`
	Active bool              `json:"active"`
}

var shuntWebhookEvents = []string{
	"auto_merge_pull_request",
	"pull_request",
	"pull_request_sync",
	"pull_request_review_approved",
	"pull_request_review_rejected",
	"pull_request_review_comment",
	"push",
	"status",
}

// EnsureBranchProtection makes sure base requires statusCtx and that botUser may
// push. It is additive: it never removes existing contexts or whitelist entries.
func (c *Client) EnsureBranchProtection(ctx context.Context, owner, repo, base, statusCtx, botUser string) (changed bool, err error) {
	var bp BranchProtection
	path := repoPath(owner, repo)
	branch := url.PathEscape(base)
	getErr := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/branch_protections/%s", path, branch), nil, &bp)
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
		return true, c.do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/branch_protections", path), body, nil)
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
	return true, c.do(ctx, http.MethodPatch, fmt.Sprintf("/repos/%s/branch_protections/%s", path, branch), body, nil)
}

// EnsureWebhook makes sure the repository has one active shunt webhook pointing
// at targetURL. It only manages hooks with the same URL, so unrelated operator
// hooks are left alone.
func (c *Client) EnsureWebhook(ctx context.Context, owner, repo, targetURL, secret string) (changed bool, err error) {
	targetURL = strings.TrimSpace(targetURL)
	if targetURL == "" {
		return false, nil
	}
	path := repoPath(owner, repo)
	var hooks []Hook
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/hooks", path), nil, &hooks); err != nil {
		return false, err
	}
	for _, hook := range hooks {
		if !isGiteaWebhook(hook.Type) || hook.Config["url"] != targetURL {
			continue
		}
		if webhookMatches(hook, targetURL, secret) {
			return false, nil
		}
		return true, c.do(ctx, http.MethodPatch, fmt.Sprintf("/repos/%s/hooks/%d", path, hook.ID), webhookBody(targetURL, secret, false), nil)
	}
	return true, c.do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/hooks", path), webhookBody(targetURL, secret, true), nil)
}

func isGiteaWebhook(hookType string) bool {
	return hookType == "gitea" || hookType == "forgejo"
}

func webhookBody(targetURL, secret string, includeType bool) map[string]any {
	body := map[string]any{
		"active": true,
		"events": shuntWebhookEvents,
		"config": map[string]string{
			"url":          targetURL,
			"content_type": "json",
			"secret":       secret,
		},
	}
	if includeType {
		body["type"] = "gitea"
	}
	return body
}

func webhookMatches(hook Hook, targetURL, secret string) bool {
	returnedSecret, hasReturnedSecret := hook.Config["secret"]
	return hook.Active &&
		sameStringSet(hook.Events, shuntWebhookEvents) &&
		hook.Config["url"] == targetURL &&
		hook.Config["content_type"] == "json" &&
		(!hasReturnedSecret || returnedSecret == secret)
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		counts[v]--
	}
	for _, n := range counts {
		if n != 0 {
			return false
		}
	}
	return true
}
