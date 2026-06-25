// Package metrics exposes dependency-free Prometheus text metrics for shunt.
package metrics

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Labels identify one managed merge queue.
type Labels struct {
	Owner string
	Repo  string
	Base  string
}

type queueMetrics struct {
	QueueDepth       int
	ActiveBatch      bool
	PendingBatches   [][]int
	ActiveBatches    [][]int
	BatchesStarted   uint64
	PRMerges         uint64
	Bounces          uint64
	StagingConflicts uint64
	ReconcileErrors  uint64
	GateOutcomes     map[string]uint64
}

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

// ObserveQueueStatus records the current in-memory queue batches.
func (c *Collector) ObserveQueueStatus(labels Labels, pending, active [][]int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.ensureLocked(labels)
	q.PendingBatches = cloneBatches(pending)
	q.ActiveBatches = cloneBatches(active)
	q.QueueDepth = batchDepth(q.PendingBatches) + batchDepth(q.ActiveBatches)
	q.ActiveBatch = len(q.ActiveBatches) > 0
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

// IncPRMerge records a PR merged by shunt.
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
		q = &queueMetrics{GateOutcomes: map[string]uint64{}}
		c.queues[labels] = q
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
		"# HELP shunt_batches_started_total Number of batches staged and sent to the gate.",
		"# TYPE shunt_batches_started_total counter",
		"# HELP shunt_pr_merges_total Number of pull requests merged by shunt.",
		"# TYPE shunt_pr_merges_total counter",
		"# HELP shunt_bounces_total Number of pull requests bounced from the queue.",
		"# TYPE shunt_bounces_total counter",
		"# HELP shunt_staging_conflicts_total Number of staging merge conflicts detected.",
		"# TYPE shunt_staging_conflicts_total counter",
		"# HELP shunt_reconcile_errors_total Number of reconcile loop errors.",
		"# TYPE shunt_reconcile_errors_total counter",
		"# HELP shunt_gate_outcomes_total Number of terminal gate outcomes by result.",
		"# TYPE shunt_gate_outcomes_total counter",
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

type QueueStatus struct {
	Owner          string  `json:"owner"`
	Repo           string  `json:"repo"`
	Base           string  `json:"base"`
	QueueDepth     int     `json:"queue_depth"`
	ActiveBatch    bool    `json:"active_batch"`
	ActiveBatches  [][]int `json:"active_batches"`
	PendingBatches [][]int `json:"pending_batches"`
}

// StatusSnapshot returns a safe, machine-readable snapshot of queue state.
func (c *Collector) StatusSnapshot() StatusSnapshot {
	queues := c.snapshot()
	out := StatusSnapshot{Queues: make([]QueueStatus, 0, len(queues))}
	for _, q := range queues {
		out.Queues = append(out.Queues, QueueStatus{
			Owner:          q.labels.Owner,
			Repo:           q.labels.Repo,
			Base:           q.labels.Base,
			QueueDepth:     q.metrics.QueueDepth,
			ActiveBatch:    q.metrics.ActiveBatch,
			ActiveBatches:  cloneBatches(q.metrics.ActiveBatches),
			PendingBatches: cloneBatches(q.metrics.PendingBatches),
		})
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
