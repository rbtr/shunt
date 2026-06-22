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
