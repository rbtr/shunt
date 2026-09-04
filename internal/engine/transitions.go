package engine

import (
	"sort"
	"strconv"
	"strings"
)

// Transition is a structured record of one merge-queue lifecycle event that
// happened during a single Reconcile call: a batch was staged, its gate
// passed, it was bisected, a PR bounced, or a PR landed. It exists so a
// caller can persist what actually happened without parsing log text.
//
// Additive to the engine's existing logger.Info/Warn calls at the same
// sites — recording a Transition never changes what gets logged.
type Transition struct {
	// Kind is one of "staged", "gate_success", "bisected", "bounced", "landed",
	// "held" (a leaf reached a terminal gate result but its source decision is
	// deferred until the whole bisection root is terminal — Reason carries the
	// held outcome, "success" or "failure"/"error"), "root_invalidated" (a
	// whole bisection root was torn down after its base branch advanced or a
	// pinned candidate changed mid-test; its candidates are re-queued as a
	// fresh root and no source decision is published), or "node_superseded" (a
	// speculatively-staged bisection node whose gate ran against an accumulator
	// the resolved frontier no longer matches; re-staged on the correct
	// baseline, no source decision).
	Kind string `json:"kind"`
	// PRs is the batch's PR set for "staged"/"gate_success"/"bisected"/"held",
	// or the single terminated/landed PR (as a length-1 slice) for
	// "bounced"/"landed".
	PRs           []int  `json:"prs"`
	StagingBranch string `json:"staging_branch"`
	// RunID and LineagePath identify this batch's position in a bisection
	// tree (see stagingBranchFor/markLineage) — a "staged" transition's
	// LineagePath minus its last character is its parent's LineagePath,
	// which is how a caller resolves parentage without re-parsing branch
	// name strings from scratch.
	RunID       string `json:"run_id"`
	LineagePath string `json:"lineage_path"`
	// Reason is set for "bounced" (why the PR was rejected) and for "held"
	// (the held gate outcome: "success", "failure", or "error").
	Reason string `json:"reason,omitempty"`
	// EventID is a deterministic key for transitions that drive an
	// irreversible side effect (a merge counter, a bounce notification): the
	// consumer records it and skips a redelivery carrying the same id. It is
	// stable across restarts because it is derived only from the durable root
	// run id, the action, and the PR(s) — not from a clock or a slice index.
	// Empty for transitions whose application is already idempotent by branch
	// name ("staged", "gate_success", "bisected").
	EventID string `json:"event_id,omitempty"`
}

// Transitions returns the lifecycle records the caller should persist for the
// most recently completed Reconcile call: this tick's single-shot records plus
// every still-pending terminal record in the outbox (re-sent until a dropped
// reconcile response can no longer have lost it). Redelivery is safe — the
// consumer de-dups terminal records by EventID.
func (e *Engine) Transitions() []Transition {
	out := append([]Transition(nil), e.transitions...)
	for _, entry := range e.outbox {
		out = append(out, entry.t)
	}
	return out
}

// ageOutbox runs at the start of every Reconcile: it counts one more send
// against each pending terminal transition and drops the ones that have had
// their full run of retransmits. outboxMaxAttempts is a safety net for a
// consumer that never acks — the primary removal is AckTransitions.
func (e *Engine) ageOutbox() {
	kept := e.outbox[:0]
	for _, entry := range e.outbox {
		entry.attempts++
		if entry.attempts < outboxMaxAttempts {
			kept = append(kept, entry)
		}
	}
	e.outbox = kept
}

// AckTransitions records the EventIDs the consumer confirmed it persisted, to
// be dropped from the outbox. Called before Reconcile with the ids carried on
// the reconcile request. The drop is deferred to applyPendingAcks (run just
// after the checkpoint is loaded) because on a fresh process the outbox does
// not exist yet — it is restored from the checkpoint inside Reconcile, and an
// ack applied before that would be silently lost and the entry redelivered
// forever. Unknown ids are ignored.
func (e *Engine) AckTransitions(ids []string) {
	e.pendingAcks = append(e.pendingAcks, ids...)
}

// applyPendingAcks drops every outbox entry whose EventID the consumer has
// acknowledged. Run once per Reconcile, immediately after loadCheckpoint.
func (e *Engine) applyPendingAcks() {
	if len(e.pendingAcks) == 0 {
		return
	}
	acked := make(map[string]bool, len(e.pendingAcks))
	for _, id := range e.pendingAcks {
		acked[id] = true
	}
	e.pendingAcks = e.pendingAcks[:0]
	if len(e.outbox) == 0 {
		return
	}
	kept := e.outbox[:0]
	for _, entry := range e.outbox {
		if !acked[entry.t.EventID] {
			kept = append(kept, entry)
		}
	}
	e.outbox = kept
}

func (e *Engine) recordTransition(t Transition) {
	if t.EventID != "" {
		e.outbox = append(e.outbox, outboxEntry{t: t})
		return
	}
	e.transitions = append(e.transitions, t)
}

// terminalEventID builds the deterministic de-dup key for a transition that
// drives an irreversible side effect. Finalization performs each action once
// (the persisted cursor guarantees it), so runID + kind + the ordered PR list
// uniquely names it regardless of how many times the reconcile response is
// redelivered.
func terminalEventID(runID, kind string, prs []int) string {
	sorted := append([]int(nil), prs...)
	sort.Ints(sorted)
	parts := make([]string, len(sorted))
	for i, n := range sorted {
		parts[i] = strconv.Itoa(n)
	}
	return runID + "|" + kind + "|" + strings.Join(parts, ",")
}
