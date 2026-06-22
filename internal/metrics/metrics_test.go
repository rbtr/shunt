package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerExposesPrometheusText(t *testing.T) {
	c := New()
	labels := Labels{Owner: "o", Repo: "r", Base: "main"}
	c.ObserveQueue(labels, 3, true)
	c.IncBatchesStarted(labels)
	c.IncPRMerge(labels)
	c.IncBounce(labels)
	c.IncStagingConflict(labels)
	c.IncReconcileError(labels)
	c.IncGateOutcome(labels, "success")

	rr := httptest.NewRecorder()
	c.Handler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))

	if got := rr.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"# TYPE shunt_queue_depth gauge\n",
		`shunt_queue_depth{owner="o",repo="r",base="main"} 3`,
		`shunt_active_batch{owner="o",repo="r",base="main"} 1`,
		`shunt_batches_started_total{owner="o",repo="r",base="main"} 1`,
		`shunt_pr_merges_total{owner="o",repo="r",base="main"} 1`,
		`shunt_bounces_total{owner="o",repo="r",base="main"} 1`,
		`shunt_staging_conflicts_total{owner="o",repo="r",base="main"} 1`,
		`shunt_reconcile_errors_total{owner="o",repo="r",base="main"} 1`,
		`shunt_gate_outcomes_total{owner="o",repo="r",base="main",outcome="success"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q in:\n%s", want, body)
		}
	}
}

func TestLabelEscaping(t *testing.T) {
	if got, want := labelSet(Labels{Owner: `a"b`, Repo: "r\ns", Base: `m\n`}), `{owner="a\"b",repo="r\ns",base="m\\n"}`; got != want {
		t.Fatalf("labelSet = %q, want %q", got, want)
	}
}

func TestForgetQueueRemovesMetrics(t *testing.T) {
	c := New()
	labels := Labels{Owner: "o", Repo: "r", Base: "main"}
	c.ObserveQueue(labels, 1, true)
	c.ForgetQueue(labels)

	rr := httptest.NewRecorder()
	c.Handler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(rr.Body.String(), `owner="o"`) {
		t.Fatalf("forgotten queue still present in:\n%s", rr.Body.String())
	}
}
