package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestHandlerExposesPrometheusText(t *testing.T) {
	c := New()
	labels := Labels{Owner: "o", Repo: "r", Base: "main"}
	c.ObserveQueue(labels, 3, true)
	c.ObserveQueueAge(labels, 90*time.Second)
	c.ObserveTimeInQueue(labels, "merged", 90*time.Second)
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
		`shunt_queue_oldest_age_seconds{owner="o",repo="r",base="main"} 90`,
		`shunt_batches_started_total{owner="o",repo="r",base="main"} 1`,
		`shunt_pr_merges_total{owner="o",repo="r",base="main"} 1`,
		`shunt_bounces_total{owner="o",repo="r",base="main"} 1`,
		`shunt_staging_conflicts_total{owner="o",repo="r",base="main"} 1`,
		`shunt_reconcile_errors_total{owner="o",repo="r",base="main"} 1`,
		`shunt_gate_outcomes_total{owner="o",repo="r",base="main",outcome="success"} 1`,
		`shunt_time_in_queue_seconds_bucket{owner="o",repo="r",base="main",outcome="merged",le="60"} 0`,
		`shunt_time_in_queue_seconds_bucket{owner="o",repo="r",base="main",outcome="merged",le="300"} 1`,
		`shunt_time_in_queue_seconds_bucket{owner="o",repo="r",base="main",outcome="merged",le="+Inf"} 1`,
		`shunt_time_in_queue_seconds_sum{owner="o",repo="r",base="main",outcome="merged"} 90`,
		`shunt_time_in_queue_seconds_count{owner="o",repo="r",base="main",outcome="merged"} 1`,
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

func TestStatusHandlerExposesSafeQueueSnapshot(t *testing.T) {
	c := New()
	labels := Labels{Owner: "octo", Repo: "widgets", Base: "main"}
	c.ObserveQueueStatus(labels, [][]int{{3}, {4, 5}}, [][]int{{1, 2}})

	rr := httptest.NewRecorder()
	c.StatusHandler().ServeHTTP(rr, httptest.NewRequest("GET", "/status", nil))

	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q", got)
	}
	body := rr.Body.String()
	var raw struct {
		Queues []map[string]json.RawMessage `json:"queues"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("status JSON did not decode: %v\n%s", err, body)
	}
	if len(raw.Queues) != 1 {
		t.Fatalf("queues = %d, want 1 in %s", len(raw.Queues), body)
	}
	allowed := map[string]bool{
		"owner": true, "repo": true, "base": true, "queue_depth": true,
		"active_batch": true, "active_batches": true, "pending_batches": true,
	}
	for key := range raw.Queues[0] {
		if !allowed[key] {
			t.Fatalf("status exposes unexpected field %q in %s", key, body)
		}
	}
	var snap StatusSnapshot
	if err := json.Unmarshal(rr.Body.Bytes(), &snap); err != nil {
		t.Fatalf("status JSON did not decode into snapshot: %v", err)
	}
	got := snap.Queues[0]
	if got.Owner != "octo" || got.Repo != "widgets" || got.Base != "main" {
		t.Fatalf("queue identity = %#v", got)
	}
	if got.QueueDepth != 5 || !got.ActiveBatch {
		t.Fatalf("queue state = depth %d active %t, want depth 5 active true", got.QueueDepth, got.ActiveBatch)
	}
	if want := [][]int{{1, 2}}; !reflect.DeepEqual(got.ActiveBatches, want) {
		t.Fatalf("active batches = %v, want %v", got.ActiveBatches, want)
	}
	if want := [][]int{{3}, {4, 5}}; !reflect.DeepEqual(got.PendingBatches, want) {
		t.Fatalf("pending batches = %v, want %v", got.PendingBatches, want)
	}
	for _, forbidden := range []string{"token", "clone", "internal", ".git", "staging_sha", "staging_branch"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("status body contains forbidden value %q in %s", forbidden, body)
		}
	}
}

func TestStatusSnapshotCopiesBatches(t *testing.T) {
	c := New()
	labels := Labels{Owner: "o", Repo: "r", Base: "main"}
	pending := [][]int{{2, 3}}
	active := [][]int{{1}}
	c.ObserveQueueStatus(labels, pending, active)
	pending[0][0] = 99
	active[0][0] = 88

	snap := c.StatusSnapshot()
	if got, want := snap.Queues[0].PendingBatches, [][]int{{2, 3}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pending batches = %v, want %v", got, want)
	}
	if got, want := snap.Queues[0].ActiveBatches, [][]int{{1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("active batches = %v, want %v", got, want)
	}

	snap.Queues[0].PendingBatches[0][0] = 77
	again := c.StatusSnapshot()
	if got, want := again.Queues[0].PendingBatches, [][]int{{2, 3}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot mutation changed collector: got %v, want %v", got, want)
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
