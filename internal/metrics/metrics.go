// Package metrics exposes dependency-free Prometheus text metrics for shunt.
package metrics

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Labels identify one managed merge queue.
type Labels struct {
	Owner string
	Repo  string
	Base  string
}

// ActiveBatchState is the observable state of one active (staged) batch.
type ActiveBatchState struct {
	PRs        []int
	Phase      string    // "waiting_gate", "waiting_merge", or "bisecting"
	PhaseSince time.Time // when the current phase started
}

// EffectiveConfig is the safe, resolved subset of per-queue configuration.
// Sensitive fields (token, bot email, instance URL, webhook secret) are
// intentionally excluded.
type EffectiveConfig struct {
	ConfigSource string // "repo" (.shunt.yml present) or "default"
	Base         string
	MergeStyle   string
	MaxBatch     int
	BatchLinger  time.Duration
	BatchTarget  int
	BisectFanout int
}

type queueMetrics struct {
	QueueDepth            int
	ActiveBatch           bool
	PendingBatches        [][]int
	ActiveBatches         [][]int
	ActiveBatchStates     []ActiveBatchState
	LingerSince           time.Time // zero if not in linger window
	Config                *EffectiveConfig
	OldestQueueAgeSeconds float64
	BatchesStarted        uint64
	PRMerges              uint64
	Bounces               uint64
	StagingConflicts      uint64
	ReconcileErrors       uint64
	GateOutcomes          map[string]uint64
	TimeInQueue           map[string]timeInQueueHistogram
	LingerDuration        timeInQueueHistogram
	GateDuration          map[string]timeInQueueHistogram
	NativeMergeDuration   timeInQueueHistogram
	ReconcileDuration     timeInQueueHistogram
}

type timeInQueueHistogram struct {
	Buckets []uint64
	Count   uint64
	Sum     float64
}

var timeInQueueBuckets = []float64{
	60,
	300,
	900,
	1800,
	3600,
	7200,
	21600,
	43200,
	86400,
}

var lingerBuckets = []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800}

var gateBuckets = []float64{60, 300, 900, 1800, 3600, 7200, 21600, 43200, 86400}

var nativeMergeBuckets = []float64{10, 30, 60, 120, 300, 600, 1800}

var reconcileBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 5, 10, 60}

// Collector stores process-local metrics and renders Prometheus text exposition.
type Collector struct {
	mu     sync.Mutex
	queues map[Labels]*queueMetrics
}

// New returns an empty metrics collector.
func New() *Collector {
	return &Collector{queues: map[Labels]*queueMetrics{}}
}

// Handler returns an HTTP handler for the Prometheus text endpoint.
func (c *Collector) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		c.WritePrometheus(w)
	})
}

// StatusHandler returns an HTTP handler for the JSON queue status endpoint.
func (c *Collector) StatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(c.StatusSnapshot()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

// StatusPageHandler returns a small human-readable queue status page.
func (c *Collector) StatusPageHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := statusPageTemplate.Execute(w, c.StatusSnapshot()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

// ObserveQueue records the current in-memory queue state.
func (c *Collector) ObserveQueue(labels Labels, depth int, active bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.ensureLocked(labels)
	q.QueueDepth = depth
	q.ActiveBatch = active
	q.PendingBatches = nil
	q.ActiveBatches = nil
}

// ObserveQueueStatus records the current in-memory queue batches with phase detail.
// pending is the list of PR-number candidates waiting to be staged.
// active carries per-batch phase detail including phase and phase start time.
// lingerSince is non-zero when the queue is in the batch-linger window.
// cfg is the safe resolved configuration for this queue (may be nil on first call).
func (c *Collector) ObserveQueueStatus(labels Labels, pending [][]int, active []ActiveBatchState, lingerSince time.Time, cfg *EffectiveConfig) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.ensureLocked(labels)
	q.PendingBatches = cloneBatches(pending)
	activeBatches := make([][]int, len(active))
	for i, a := range active {
		activeBatches[i] = append([]int(nil), a.PRs...)
	}
	q.ActiveBatches = activeBatches
	states := make([]ActiveBatchState, len(active))
	for i, s := range active {
		s.PRs = append([]int(nil), s.PRs...)
		states[i] = s
	}
	q.ActiveBatchStates = states
	q.LingerSince = lingerSince
	if cfg != nil {
		cfgCopy := *cfg
		q.Config = &cfgCopy
	}
	q.QueueDepth = batchDepth(q.PendingBatches) + batchDepth(q.ActiveBatches)
	q.ActiveBatch = len(q.ActiveBatches) > 0
}

// ObserveQueueAge records the oldest process-local age among currently queued PRs.
func (c *Collector) ObserveQueueAge(labels Labels, oldest time.Duration) {
	if c == nil {
		return
	}
	if oldest < 0 {
		oldest = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureLocked(labels).OldestQueueAgeSeconds = oldest.Seconds()
}

// ObserveTimeInQueue records how long a PR spent in the process-local queue.
func (c *Collector) ObserveTimeInQueue(labels Labels, outcome string, age time.Duration) {
	if c == nil || outcome == "" {
		return
	}
	if age < 0 {
		age = 0
	}
	seconds := age.Seconds()
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.ensureLocked(labels)
	h := q.TimeInQueue[outcome]
	if h.Buckets == nil {
		h.Buckets = make([]uint64, len(timeInQueueBuckets))
	}
	for i, le := range timeInQueueBuckets {
		if seconds <= le {
			h.Buckets[i]++
		}
	}
	h.Count++
	h.Sum += seconds
	q.TimeInQueue[outcome] = h
}

// IncBatchesStarted records a batch that was staged and sent to the gate.
func (c *Collector) IncBatchesStarted(labels Labels) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureLocked(labels).BatchesStarted++
}

// IncPRMerge records a PR landed through shunt's queue.
func (c *Collector) IncPRMerge(labels Labels) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureLocked(labels).PRMerges++
}

// IncBounce records a PR bounced from the queue.
func (c *Collector) IncBounce(labels Labels) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureLocked(labels).Bounces++
}

// IncStagingConflict records a staging merge conflict.
func (c *Collector) IncStagingConflict(labels Labels) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureLocked(labels).StagingConflicts++
}

// IncReconcileError records a reconcile loop error.
func (c *Collector) IncReconcileError(labels Labels) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureLocked(labels).ReconcileErrors++
}

// IncGateOutcome records a terminal gate result for a staging batch.
func (c *Collector) IncGateOutcome(labels Labels, outcome string) {
	if c == nil || outcome == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureLocked(labels).GateOutcomes[outcome]++
}

// ObserveLingerDuration records the duration a queue spent in the batch-linger
// window before forming a batch. Only called when linger was actually active.
func (c *Collector) ObserveLingerDuration(labels Labels, d time.Duration) {
	if c == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.ensureLocked(labels)
	q.LingerDuration = observeHistogram(q.LingerDuration, lingerBuckets, d.Seconds())
}

// ObserveGateDuration records how long a staged batch ran under gate test before
// reaching a terminal outcome. Labels the observation by outcome for alignment
// with shunt_gate_outcomes_total.
func (c *Collector) ObserveGateDuration(labels Labels, outcome string, d time.Duration) {
	if c == nil || outcome == "" {
		return
	}
	if d < 0 {
		d = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.ensureLocked(labels)
	if q.GateDuration == nil {
		q.GateDuration = map[string]timeInQueueHistogram{}
	}
	q.GateDuration[outcome] = observeHistogram(q.GateDuration[outcome], gateBuckets, d.Seconds())
}

// ObserveNativeMergeDuration records the time from when shunt released a PR to
// the forge's scheduled auto-merge worker until that PR was observed merged.
func (c *Collector) ObserveNativeMergeDuration(labels Labels, d time.Duration) {
	if c == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.ensureLocked(labels)
	q.NativeMergeDuration = observeHistogram(q.NativeMergeDuration, nativeMergeBuckets, d.Seconds())
}

// ObserveReconcileDuration records the wall-clock time spent in one Reconcile
// call for this queue.
func (c *Collector) ObserveReconcileDuration(labels Labels, d time.Duration) {
	if c == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.ensureLocked(labels)
	q.ReconcileDuration = observeHistogram(q.ReconcileDuration, reconcileBuckets, d.Seconds())
}

// ForgetQueue removes metrics for a queue that is no longer managed.
func (c *Collector) ForgetQueue(labels Labels) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.queues, labels)
}

func (c *Collector) ensureLocked(labels Labels) *queueMetrics {
	if c.queues == nil {
		c.queues = map[Labels]*queueMetrics{}
	}
	q, ok := c.queues[labels]
	if !ok {
		q = &queueMetrics{
			GateOutcomes: map[string]uint64{},
			TimeInQueue:  map[string]timeInQueueHistogram{},
			GateDuration: map[string]timeInQueueHistogram{},
		}
		c.queues[labels] = q
	}
	if q.GateOutcomes == nil {
		q.GateOutcomes = map[string]uint64{}
	}
	if q.TimeInQueue == nil {
		q.TimeInQueue = map[string]timeInQueueHistogram{}
	}
	if q.GateDuration == nil {
		q.GateDuration = map[string]timeInQueueHistogram{}
	}
	return q
}

type snapshotQueue struct {
	labels  Labels
	metrics queueMetrics
}

// WritePrometheus writes a full Prometheus text-format snapshot.
func (c *Collector) WritePrometheus(w io.Writer) {
	for _, line := range []string{
		"# HELP shunt_queue_depth Number of PRs known in the in-memory queue, including active batches and queued bisection candidates.",
		"# TYPE shunt_queue_depth gauge",
		"# HELP shunt_active_batch Whether a queue currently has a batch under gate test.",
		"# TYPE shunt_active_batch gauge",
		"# HELP shunt_queue_oldest_age_seconds Oldest process-local age of any PR currently known in the in-memory queue.",
		"# TYPE shunt_queue_oldest_age_seconds gauge",
		"# HELP shunt_batches_started_total Number of batches staged and sent to the gate.",
		"# TYPE shunt_batches_started_total counter",
		"# HELP shunt_pr_merges_total Number of pull requests landed through shunt's queue.",
		"# TYPE shunt_pr_merges_total counter",
		"# HELP shunt_bounces_total Number of pull requests bounced from the queue.",
		"# TYPE shunt_bounces_total counter",
		"# HELP shunt_staging_conflicts_total Number of staging merge conflicts detected.",
		"# TYPE shunt_staging_conflicts_total counter",
		"# HELP shunt_reconcile_errors_total Number of reconcile loop errors.",
		"# TYPE shunt_reconcile_errors_total counter",
		"# HELP shunt_gate_outcomes_total Number of terminal gate outcomes by result.",
		"# TYPE shunt_gate_outcomes_total counter",
		"# HELP shunt_time_in_queue_seconds Process-local time a PR spent in the queue before leaving, partitioned by outcome.",
		"# TYPE shunt_time_in_queue_seconds histogram",
		"# HELP shunt_linger_seconds Duration of the batch-linger window from first ready PR until the batch was formed.",
		"# TYPE shunt_linger_seconds histogram",
		"# HELP shunt_gate_seconds Duration of a gate run from staging until a terminal outcome, partitioned by outcome.",
		"# TYPE shunt_gate_seconds histogram",
		"# HELP shunt_native_merge_seconds Time from releasing a PR to the forge auto-merge worker until the PR was observed merged.",
		"# TYPE shunt_native_merge_seconds histogram",
		"# HELP shunt_reconcile_seconds Wall-clock time spent in one Reconcile call for this queue.",
		"# TYPE shunt_reconcile_seconds histogram",
	} {
		fmt.Fprintln(w, line)
	}

	for _, q := range c.snapshot() {
		labels := labelSet(q.labels)
		fmt.Fprintf(w, "shunt_queue_depth%s %d\n", labels, q.metrics.QueueDepth)
		active := 0
		if q.metrics.ActiveBatch {
			active = 1
		}
		fmt.Fprintf(w, "shunt_active_batch%s %d\n", labels, active)
		fmt.Fprintf(w, "shunt_queue_oldest_age_seconds%s %s\n", labels, formatFloat(q.metrics.OldestQueueAgeSeconds))
		fmt.Fprintf(w, "shunt_batches_started_total%s %d\n", labels, q.metrics.BatchesStarted)
		fmt.Fprintf(w, "shunt_pr_merges_total%s %d\n", labels, q.metrics.PRMerges)
		fmt.Fprintf(w, "shunt_bounces_total%s %d\n", labels, q.metrics.Bounces)
		fmt.Fprintf(w, "shunt_staging_conflicts_total%s %d\n", labels, q.metrics.StagingConflicts)
		fmt.Fprintf(w, "shunt_reconcile_errors_total%s %d\n", labels, q.metrics.ReconcileErrors)

		outcomes := make([]string, 0, len(q.metrics.GateOutcomes))
		for outcome := range q.metrics.GateOutcomes {
			outcomes = append(outcomes, outcome)
		}
		sort.Strings(outcomes)
		for _, outcome := range outcomes {
			fmt.Fprintf(w, "shunt_gate_outcomes_total%s %d\n", labelSet(q.labels, "outcome", outcome), q.metrics.GateOutcomes[outcome])
		}

		timeOutcomes := make([]string, 0, len(q.metrics.TimeInQueue))
		for outcome := range q.metrics.TimeInQueue {
			timeOutcomes = append(timeOutcomes, outcome)
		}
		sort.Strings(timeOutcomes)
		for _, outcome := range timeOutcomes {
			h := q.metrics.TimeInQueue[outcome]
			for i, le := range timeInQueueBuckets {
				var count uint64
				if i < len(h.Buckets) {
					count = h.Buckets[i]
				}
				fmt.Fprintf(w, "shunt_time_in_queue_seconds_bucket%s %d\n", labelSet(q.labels, "outcome", outcome, "le", formatFloat(le)), count)
			}
			fmt.Fprintf(w, "shunt_time_in_queue_seconds_bucket%s %d\n", labelSet(q.labels, "outcome", outcome, "le", "+Inf"), h.Count)
			fmt.Fprintf(w, "shunt_time_in_queue_seconds_sum%s %s\n", labelSet(q.labels, "outcome", outcome), formatFloat(h.Sum))
			fmt.Fprintf(w, "shunt_time_in_queue_seconds_count%s %d\n", labelSet(q.labels, "outcome", outcome), h.Count)
		}

		writeHistogram(w, q.labels, "shunt_linger_seconds", lingerBuckets, q.metrics.LingerDuration)

		gateOutcomes := make([]string, 0, len(q.metrics.GateDuration))
		for outcome := range q.metrics.GateDuration {
			gateOutcomes = append(gateOutcomes, outcome)
		}
		sort.Strings(gateOutcomes)
		for _, outcome := range gateOutcomes {
			writeHistogramWithExtra(w, q.labels, "shunt_gate_seconds", gateBuckets, q.metrics.GateDuration[outcome], "outcome", outcome)
		}

		writeHistogram(w, q.labels, "shunt_native_merge_seconds", nativeMergeBuckets, q.metrics.NativeMergeDuration)
		writeHistogram(w, q.labels, "shunt_reconcile_seconds", reconcileBuckets, q.metrics.ReconcileDuration)
	}
}

func (c *Collector) snapshot() []snapshotQueue {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]snapshotQueue, 0, len(c.queues))
	for labels, metrics := range c.queues {
		copyMetrics := *metrics
		copyMetrics.GateOutcomes = make(map[string]uint64, len(metrics.GateOutcomes))
		for outcome, n := range metrics.GateOutcomes {
			copyMetrics.GateOutcomes[outcome] = n
		}
		copyMetrics.PendingBatches = cloneBatches(metrics.PendingBatches)
		copyMetrics.ActiveBatches = cloneBatches(metrics.ActiveBatches)
		copyMetrics.ActiveBatchStates = append([]ActiveBatchState(nil), metrics.ActiveBatchStates...)
		copyMetrics.TimeInQueue = make(map[string]timeInQueueHistogram, len(metrics.TimeInQueue))
		for outcome, h := range metrics.TimeInQueue {
			h.Buckets = append([]uint64(nil), h.Buckets...)
			copyMetrics.TimeInQueue[outcome] = h
		}
		copyMetrics.GateDuration = make(map[string]timeInQueueHistogram, len(metrics.GateDuration))
		for outcome, h := range metrics.GateDuration {
			h.Buckets = append([]uint64(nil), h.Buckets...)
			copyMetrics.GateDuration[outcome] = h
		}
		copyMetrics.LingerDuration.Buckets = append([]uint64(nil), metrics.LingerDuration.Buckets...)
		copyMetrics.NativeMergeDuration.Buckets = append([]uint64(nil), metrics.NativeMergeDuration.Buckets...)
		copyMetrics.ReconcileDuration.Buckets = append([]uint64(nil), metrics.ReconcileDuration.Buckets...)
		if metrics.Config != nil {
			cfgCopy := *metrics.Config
			copyMetrics.Config = &cfgCopy
		}
		out = append(out, snapshotQueue{labels: labels, metrics: copyMetrics})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].labels, out[j].labels
		if a.Owner != b.Owner {
			return a.Owner < b.Owner
		}
		if a.Repo != b.Repo {
			return a.Repo < b.Repo
		}
		return a.Base < b.Base
	})
	return out
}

type StatusSnapshot struct {
	Queues []QueueStatus `json:"queues"`
}

// ActiveBatchStateJSON is the per-batch phase detail included in QueueStatus.
type ActiveBatchStateJSON struct {
	PRs        []int  `json:"prs"`
	Phase      string `json:"phase"`
	PhaseSince string `json:"phase_since"` // RFC3339
}

// EffectiveConfigJSON is the safe, resolved queue configuration included in QueueStatus.
// Sensitive fields (token, bot email, instance URL, webhook secret) are excluded.
type EffectiveConfigJSON struct {
	ConfigSource string `json:"config_source"` // "repo" or "default"
	Base         string `json:"base"`
	MergeStyle   string `json:"merge_style"`
	MaxBatch     int    `json:"max_batch"`
	BatchLinger  string `json:"batch_linger"` // Go duration string, e.g. "30s"
	BatchTarget  int    `json:"batch_target"`
	BisectFanout int    `json:"bisect_fanout"`
}

type QueueStatus struct {
	Owner          string  `json:"owner"`
	Repo           string  `json:"repo"`
	Base           string  `json:"base"`
	QueueDepth     int     `json:"queue_depth"`
	ActiveBatch    bool    `json:"active_batch"`
	ActiveBatches  [][]int `json:"active_batches"`
	PendingBatches [][]int `json:"pending_batches"`
	// New additive fields. Consumers relying only on the fields above are unaffected.
	ActiveBatchStates []ActiveBatchStateJSON `json:"active_batch_states,omitempty"`
	LingerSince       *string                `json:"linger_since,omitempty"` // RFC3339 while lingering
	Config            *EffectiveConfigJSON   `json:"config,omitempty"`
}

var statusPageTemplate = template.Must(template.New("status").Funcs(template.FuncMap{
	"batchList": formatBatchList,
}).Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>shunt queue status</title>
<style>
body{font-family:system-ui,sans-serif;margin:2rem;line-height:1.4}
table{border-collapse:collapse;width:100%;max-width:72rem}
th,td{border:1px solid #ddd;padding:.5rem;text-align:left;vertical-align:top}
th{background:#f6f8fa}
code{white-space:pre-wrap}
</style>
</head>
<body>
<h1>shunt queue status</h1>
{{if .Queues}}
<table>
<thead><tr><th>Queue</th><th>Depth</th><th>Active</th><th>Active batches</th><th>Pending batches</th></tr></thead>
<tbody>
{{range .Queues}}
<tr>
<td><code>{{.Owner}}/{{.Repo}}@{{.Base}}</code></td>
<td>{{.QueueDepth}}</td>
<td>{{.ActiveBatch}}</td>
<td><code>{{batchList .ActiveBatches}}</code></td>
<td><code>{{batchList .PendingBatches}}</code></td>
</tr>
{{end}}
</tbody>
</table>
{{else}}
<p>No queues are currently known to this process.</p>
{{end}}
<p>This page is process-local and exposes only queue identity and PR numbers. JSON is available at <a href="/status">/status</a>.</p>
</body>
</html>
`))

// StatusSnapshot returns a safe, machine-readable snapshot of queue state.
func (c *Collector) StatusSnapshot() StatusSnapshot {
	queues := c.snapshot()
	out := StatusSnapshot{Queues: make([]QueueStatus, 0, len(queues))}
	for _, q := range queues {
		qs := QueueStatus{
			Owner:          q.labels.Owner,
			Repo:           q.labels.Repo,
			Base:           q.labels.Base,
			QueueDepth:     q.metrics.QueueDepth,
			ActiveBatch:    q.metrics.ActiveBatch,
			ActiveBatches:  cloneBatches(q.metrics.ActiveBatches),
			PendingBatches: cloneBatches(q.metrics.PendingBatches),
		}
		if len(q.metrics.ActiveBatchStates) > 0 {
			qs.ActiveBatchStates = make([]ActiveBatchStateJSON, len(q.metrics.ActiveBatchStates))
			for i, s := range q.metrics.ActiveBatchStates {
				qs.ActiveBatchStates[i] = ActiveBatchStateJSON{
					PRs:        append([]int(nil), s.PRs...),
					Phase:      s.Phase,
					PhaseSince: s.PhaseSince.UTC().Format(time.RFC3339),
				}
			}
		}
		if !q.metrics.LingerSince.IsZero() {
			ts := q.metrics.LingerSince.UTC().Format(time.RFC3339)
			qs.LingerSince = &ts
		}
		if q.metrics.Config != nil {
			cfg := q.metrics.Config
			qs.Config = &EffectiveConfigJSON{
				ConfigSource: cfg.ConfigSource,
				Base:         cfg.Base,
				MergeStyle:   cfg.MergeStyle,
				MaxBatch:     cfg.MaxBatch,
				BatchLinger:  cfg.BatchLinger.String(),
				BatchTarget:  cfg.BatchTarget,
				BisectFanout: cfg.BisectFanout,
			}
		}
		out.Queues = append(out.Queues, qs)
	}
	return out
}

func cloneBatches(in [][]int) [][]int {
	out := make([][]int, len(in))
	for i, batch := range in {
		out[i] = append([]int(nil), batch...)
	}
	return out
}

func batchDepth(batches [][]int) int {
	depth := 0
	for _, batch := range batches {
		depth += len(batch)
	}
	return depth
}

func formatBatchList(batches [][]int) string {
	if len(batches) == 0 {
		return "none"
	}
	parts := make([]string, len(batches))
	for i, batch := range batches {
		items := make([]string, len(batch))
		for j, n := range batch {
			items[j] = strconv.Itoa(n)
		}
		parts[i] = "[" + strings.Join(items, " ") + "]"
	}
	return strings.Join(parts, " ")
}

func labelSet(labels Labels, extra ...string) string {
	parts := []string{
		fmt.Sprintf("owner=\"%s\"", escape(labels.Owner)),
		fmt.Sprintf("repo=\"%s\"", escape(labels.Repo)),
		fmt.Sprintf("base=\"%s\"", escape(labels.Base)),
	}
	for i := 0; i+1 < len(extra); i += 2 {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", extra[i], escape(extra[i+1])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escape(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
	return replacer.Replace(s)
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func observeHistogram(h timeInQueueHistogram, buckets []float64, seconds float64) timeInQueueHistogram {
	if h.Buckets == nil {
		h.Buckets = make([]uint64, len(buckets))
	}
	for i, le := range buckets {
		if seconds <= le {
			h.Buckets[i]++
		}
	}
	h.Count++
	h.Sum += seconds
	return h
}

func writeHistogram(w io.Writer, labels Labels, name string, buckets []float64, h timeInQueueHistogram) {
	writeHistogramWithExtra(w, labels, name, buckets, h)
}

func writeHistogramWithExtra(w io.Writer, labels Labels, name string, buckets []float64, h timeInQueueHistogram, extra ...string) {
	for i, le := range buckets {
		var count uint64
		if i < len(h.Buckets) {
			count = h.Buckets[i]
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", name, labelSet(labels, append(extra, "le", formatFloat(le))...), count)
	}
	fmt.Fprintf(w, "%s_bucket%s %d\n", name, labelSet(labels, append(extra, "le", "+Inf")...), h.Count)
	fmt.Fprintf(w, "%s_sum%s %s\n", name, labelSet(labels, extra...), formatFloat(h.Sum))
	fmt.Fprintf(w, "%s_count%s %d\n", name, labelSet(labels, extra...), h.Count)
}
