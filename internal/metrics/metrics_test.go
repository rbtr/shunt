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
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c.ObserveQueueStatus(labels, [][]int{{3}, {4, 5}}, []ActiveBatchState{
		{PRs: []int{1, 2}, Phase: "waiting_gate", PhaseSince: now},
	}, time.Time{}, nil)

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
	// Verify only safe fields appear. New fields active_batch_states, linger_since,
	// and config are allowed; credentials and internal details are not.
	allowed := map[string]bool{
		"owner": true, "repo": true, "base": true, "queue_depth": true,
		"active_batch": true, "active_batches": true, "pending_batches": true,
		"active_batch_states": true, "linger_since": true, "config": true,
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

// TestOldJSONShapeUnchanged is a regression test ensuring the backward-compatible
// active_batches and pending_batches fields remain [][]int and are not altered.
func TestOldJSONShapeUnchanged(t *testing.T) {
	c := New()
	labels := Labels{Owner: "o", Repo: "r", Base: "main"}
	c.ObserveQueueStatus(labels, [][]int{{3}, {4, 5}}, []ActiveBatchState{
		{PRs: []int{1, 2}, Phase: "waiting_gate", PhaseSince: time.Now()},
	}, time.Time{}, nil)

	rr := httptest.NewRecorder()
	c.StatusHandler().ServeHTTP(rr, httptest.NewRequest("GET", "/status", nil))

	// Deserialize with a struct that uses only the old fields. If the type changed
	// from [][]int to something else, this unmarshal would fail.
	var raw struct {
		Queues []struct {
			ActiveBatches  [][]int `json:"active_batches"`
			PendingBatches [][]int `json:"pending_batches"`
		} `json:"queues"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("old-shape unmarshal failed: %v\nbody: %s", err, rr.Body.String())
	}
	if len(raw.Queues) != 1 {
		t.Fatalf("queues = %d, want 1", len(raw.Queues))
	}
	if want := [][]int{{1, 2}}; !reflect.DeepEqual(raw.Queues[0].ActiveBatches, want) {
		t.Fatalf("active_batches = %v, want %v", raw.Queues[0].ActiveBatches, want)
	}
	if want := [][]int{{3}, {4, 5}}; !reflect.DeepEqual(raw.Queues[0].PendingBatches, want) {
		t.Fatalf("pending_batches = %v, want %v", raw.Queues[0].PendingBatches, want)
	}
}

// TestNewStatusFieldsAppear verifies that active_batch_states, linger_since, and
// config appear correctly in the JSON /status output.
func TestNewStatusFieldsAppear(t *testing.T) {
	c := New()
	labels := Labels{Owner: "o", Repo: "r", Base: "main"}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	linger := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	cfg := &EffectiveConfig{
		ConfigSource: "repo",
		Base:         "main",
		MergeStyle:   "squash",
		MaxBatch:     5,
		BatchLinger:  30 * time.Second,
		BatchTarget:  3,
		BisectFanout: 2,
	}
	c.ObserveQueueStatus(labels, [][]int{{3}}, []ActiveBatchState{
		{PRs: []int{1, 2}, Phase: "bisecting", PhaseSince: now},
	}, linger, cfg)

	snap := c.StatusSnapshot()
	if len(snap.Queues) != 1 {
		t.Fatalf("queues = %d, want 1", len(snap.Queues))
	}
	q := snap.Queues[0]

	// active_batch_states
	if len(q.ActiveBatchStates) != 1 {
		t.Fatalf("active_batch_states len = %d, want 1", len(q.ActiveBatchStates))
	}
	s := q.ActiveBatchStates[0]
	if want := []int{1, 2}; !reflect.DeepEqual(s.PRs, want) {
		t.Fatalf("active_batch_states[0].prs = %v, want %v", s.PRs, want)
	}
	if s.Phase != "bisecting" {
		t.Fatalf("phase = %q, want %q", s.Phase, "bisecting")
	}
	if s.PhaseSince != now.UTC().Format(time.RFC3339) {
		t.Fatalf("phase_since = %q, want %q", s.PhaseSince, now.UTC().Format(time.RFC3339))
	}

	// linger_since
	if q.LingerSince == nil {
		t.Fatal("linger_since is nil, want non-nil")
	}
	if *q.LingerSince != linger.UTC().Format(time.RFC3339) {
		t.Fatalf("linger_since = %q, want %q", *q.LingerSince, linger.UTC().Format(time.RFC3339))
	}

	// config
	if q.Config == nil {
		t.Fatal("config is nil, want non-nil")
	}
	if q.Config.ConfigSource != "repo" {
		t.Fatalf("config_source = %q, want %q", q.Config.ConfigSource, "repo")
	}
	if q.Config.MergeStyle != "squash" {
		t.Fatalf("merge_style = %q, want %q", q.Config.MergeStyle, "squash")
	}
	if q.Config.MaxBatch != 5 {
		t.Fatalf("max_batch = %d, want 5", q.Config.MaxBatch)
	}
	if q.Config.BatchLinger != "30s" {
		t.Fatalf("batch_linger = %q, want \"30s\"", q.Config.BatchLinger)
	}
	if q.Config.BatchTarget != 3 {
		t.Fatalf("batch_target = %d, want 3", q.Config.BatchTarget)
	}
	if q.Config.BisectFanout != 2 {
		t.Fatalf("bisect_fanout = %d, want 2", q.Config.BisectFanout)
	}

	// linger_since is absent when queue is not lingering
	c2 := New()
	c2.ObserveQueueStatus(labels, nil, nil, time.Time{}, nil)
	snap2 := c2.StatusSnapshot()
	if len(snap2.Queues) != 1 {
		t.Fatalf("queues = %d, want 1", len(snap2.Queues))
	}
	if snap2.Queues[0].LingerSince != nil {
		t.Fatalf("linger_since expected nil when not lingering, got %v", snap2.Queues[0].LingerSince)
	}

	// Confirm sensitive fields are absent from JSON
	rr := httptest.NewRecorder()
	c.StatusHandler().ServeHTTP(rr, httptest.NewRequest("GET", "/status", nil))
	body := rr.Body.String()
	for _, forbidden := range []string{"token", "instance_url", "webhook_secret", "bot_email", "lease_holder"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("status exposes forbidden field %q: %s", forbidden, body)
		}
	}
}

// TestPrometheusNewHistograms verifies that the four new histograms appear in /metrics.
func TestPrometheusNewHistograms(t *testing.T) {
	c := New()
	labels := Labels{Owner: "o", Repo: "r", Base: "main"}
	c.ObserveLingerDuration(labels, 45*time.Second)
	c.ObserveGateDuration(labels, "success", 300*time.Second)
	c.ObserveNativeMergeDuration(labels, 20*time.Second)
	c.ObserveReconcileDuration(labels, 150*time.Millisecond)

	rr := httptest.NewRecorder()
	c.Handler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	body := rr.Body.String()

	for _, want := range []string{
		"# TYPE shunt_linger_seconds histogram",
		`shunt_linger_seconds_bucket{owner="o",repo="r",base="main",le="30"} 0`,
		`shunt_linger_seconds_bucket{owner="o",repo="r",base="main",le="60"} 1`,
		`shunt_linger_seconds_count{owner="o",repo="r",base="main"} 1`,
		`shunt_linger_seconds_sum{owner="o",repo="r",base="main"} 45`,

		"# TYPE shunt_gate_seconds histogram",
		`shunt_gate_seconds_bucket{owner="o",repo="r",base="main",outcome="success",le="300"} 1`,
		`shunt_gate_seconds_count{owner="o",repo="r",base="main",outcome="success"} 1`,
		`shunt_gate_seconds_sum{owner="o",repo="r",base="main",outcome="success"} 300`,

		"# TYPE shunt_native_merge_seconds histogram",
		`shunt_native_merge_seconds_bucket{owner="o",repo="r",base="main",le="10"} 0`,
		`shunt_native_merge_seconds_bucket{owner="o",repo="r",base="main",le="30"} 1`,
		`shunt_native_merge_seconds_count{owner="o",repo="r",base="main"} 1`,

		"# TYPE shunt_reconcile_seconds histogram",
		`shunt_reconcile_seconds_bucket{owner="o",repo="r",base="main",le="0.1"} 0`,
		`shunt_reconcile_seconds_bucket{owner="o",repo="r",base="main",le="0.25"} 1`,
		`shunt_reconcile_seconds_count{owner="o",repo="r",base="main"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q in:\n%s", want, body)
		}
	}
}

func TestStatusPageHandlerExposesHumanQueueSnapshot(t *testing.T) {
	c := New()
	labels := Labels{Owner: "octo", Repo: "widgets", Base: "main"}
	c.ObserveQueueStatus(labels, [][]int{{3}, {4, 5}}, []ActiveBatchState{
		{PRs: []int{1, 2}, Phase: "waiting_gate", PhaseSince: time.Now()},
	}, time.Time{}, nil)

	rr := httptest.NewRecorder()
	c.StatusPageHandler().ServeHTTP(rr, httptest.NewRequest("GET", "/status.html", nil))

	if got := rr.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"shunt queue status",
		"octo/widgets@main",
		"<td>5</td>",
		"[1 2]",
		"[3] [4 5]",
		"/status",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("status page missing %q in:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{"token", "clone", ".git", "staging_sha", "staging_branch"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("status page contains forbidden value %q in %s", forbidden, body)
		}
	}
}

func TestStatusPageHandlerEscapesQueueIdentity(t *testing.T) {
	c := New()
	labels := Labels{Owner: `<script>`, Repo: `widgets&tools`, Base: `main"branch`}
	c.ObserveQueueStatus(labels, [][]int{{1}}, nil, time.Time{}, nil)

	rr := httptest.NewRecorder()
	c.StatusPageHandler().ServeHTTP(rr, httptest.NewRequest("GET", "/status.html", nil))

	body := rr.Body.String()
	for _, forbidden := range []string{`<script>`, `widgets&tools`, `main"branch`} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("status page did not escape %q in:\n%s", forbidden, body)
		}
	}
	for _, want := range []string{`&lt;script&gt;`, `widgets&amp;tools`, `main&#34;branch`} {
		if !strings.Contains(body, want) {
			t.Fatalf("status page missing escaped value %q in:\n%s", want, body)
		}
	}
}

func TestStatusSnapshotCopiesBatches(t *testing.T) {
	c := New()
	labels := Labels{Owner: "o", Repo: "r", Base: "main"}
	pending := [][]int{{2, 3}}
	active := []ActiveBatchState{{PRs: []int{1}, Phase: "waiting_gate"}}
	c.ObserveQueueStatus(labels, pending, active, time.Time{}, nil)
	pending[0][0] = 99
	active[0].PRs[0] = 88

	snap := c.StatusSnapshot()
	if got, want := snap.Queues[0].PendingBatches, [][]int{{2, 3}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pending batches = %v, want %v", got, want)
	}
	if got, want := snap.Queues[0].ActiveBatches, [][]int{{1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("active batches = %v, want %v", got, want)
	}
	if got, want := snap.Queues[0].ActiveBatchStates[0].PRs, []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("active_batch_states[0].prs = %v after caller mutation, want %v", got, want)
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
