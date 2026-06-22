package main

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rbtr/shunt/internal/metrics"
)

func TestHTTPMuxServesHealthAndMetrics(t *testing.T) {
	c := metrics.New()
	c.ObserveQueue(metrics.Labels{Owner: "o", Repo: "r", Base: "main"}, 1, true)
	mux := newHTTPMux(c)

	health := httptest.NewRecorder()
	mux.ServeHTTP(health, httptest.NewRequest("GET", "/healthz", nil))
	if got := health.Body.String(); got != "ok" {
		t.Fatalf("/healthz = %q, want ok", got)
	}

	metricsResp := httptest.NewRecorder()
	mux.ServeHTTP(metricsResp, httptest.NewRequest("GET", "/metrics", nil))
	if got := metricsResp.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("/metrics Content-Type = %q", got)
	}
	if body := metricsResp.Body.String(); !strings.Contains(body, `shunt_queue_depth{owner="o",repo="r",base="main"} 1`) {
		t.Fatalf("/metrics missing queue depth in:\n%s", body)
	}
}

func TestNormalizeMergeStyle(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  string
	}{
		{input: "", want: "merge"},
		{input: "merge", want: "merge"},
		{input: "MERGE-COMMIT", want: "merge"},
		{input: "squash", want: "squash"},
		{input: " ReBase ", want: "rebase"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := normalizeMergeStyle(tc.input)
			if err != nil {
				t.Fatalf("normalizeMergeStyle(%q): %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("normalizeMergeStyle(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestNormalizeMergeStyleRejectsUnknown(t *testing.T) {
	if _, err := normalizeMergeStyle("fast-forward"); err == nil {
		t.Fatal("normalizeMergeStyle should reject unsupported styles")
	}
}
