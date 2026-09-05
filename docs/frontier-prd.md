# Frontier merge-queue model

**Status:** Proposed
**Audience:** Shunt maintainers and contributors
**Scope:** The queue engine and its durable state model

## Summary

Shunt needs a merge-queue model that can test an ordered group of pull
requests, isolate failures, and keep making progress without losing the
ordering users requested. The model must remain correct across restarts,
source-head updates, and base-branch movement.

This document is the product and behavior contract for that work. It is a
clean-room specification for Shunt: implementation details are deliberately
not prescribed, and existing prototypes are not the source of truth. The
implementation may be discarded and rebuilt from these requirements.

## Problem

A merge queue normally learns only whether a staged group passed or failed.
When a group fails, testing every pull request individually is slow; testing
many speculative groups concurrently is faster but can produce results that no
longer describe the queue after an earlier group is accepted or rejected.

Shunt needs to preserve three properties at the same time:

1. **Safety:** never report success for a pull request using evidence that does
   not include its current head and the current base.
2. **Order:** never release a later pull request before an earlier queued pull
   request is resolved.
3. **Progress:** a failed group should be reduced to useful smaller tests rather
   than blocking all later work indefinitely.

## Goals

- Represent unresolved work as an ordered frontier of candidate tests.
- Reuse valid test results while making invalid or superseded results harmless.
- Isolate a failing pull request with fewer tests than one test per pull request.
- Resume safely after a process restart.
- Keep Bolt/bbolt the primary local durable store with no new runtime
  dependency.
- Make behavior observable through existing statuses, comments, logs, and
  metrics.
- Keep the design usable by an independent Shunt installation against
  Forgejo-compatible APIs.

## Non-goals

- Replacing Forgejo/Gitea's merge worker or branch protection.
- Directly pushing the base branch or bypassing forge merge requirements.
- Multi-replica coordination in the first implementation.
- Making Postgres a prerequisite or changing its schema as part of the Bolt
  implementation. Postgres compatibility is a separate follow-up and must not
  weaken the Bolt-first path.
- Speculative fan-out before the serial frontier behavior is correct and
  measurable.
- A new public API solely for this feature.

## Product behavior

### Queue admission

- A pull request enters the queue only if it is open, still eligible for
  auto-merge, and passes the repository's configured admission rules.
- Admission records the source head SHA and the base identity used for the
  decision.
- A source-head update removes the old evidence from consideration and causes
  the current pull request to be tested again.

### Candidate tests

A candidate is an ordered list of pull requests plus the immutable inputs that
make its result meaningful:

- repository and base reference;
- base revision or generation;
- ordered pull-request numbers and source head SHAs;
- merge method and relevant queue configuration;
- a unique staging attempt identity.

A candidate is never considered reusable merely because it has the same pull
request numbers. Its recorded inputs must match the inputs being evaluated.

Each candidate has one of three externally useful states:

- **unknown:** not yet tested or no longer trustworthy;
- **passing:** its staged result passed the configured gate;
- **failing:** its staged result failed or could not be produced.

Infrastructure errors are not silently converted into a passing or failing
candidate. They remain retryable or become an explicit terminal error through
the existing queue outcome path.

### Frontier reduction

The engine maintains ordered unresolved work. At each step it may:

1. test the earliest unresolved candidate;
2. accept a passing candidate when all earlier work is resolved;
3. split a failing candidate into smaller ordered candidates; or
4. bounce a candidate that cannot be staged because of a deterministic conflict.

A split must preserve queue order and must not drop, duplicate, or reorder pull
requests. A result for a later candidate may be retained as evidence, but it
cannot release or mark success for a later pull request while an earlier
candidate remains unresolved.

The initial implementation should use a deterministic serial frontier. Any
concurrent fan-out is a later optimization with explicit invalidation rules
and measurements showing that it improves throughput without changing the
serial behavior.

### Base and source-head changes

Evidence is invalid when any of its inputs change, including:

- the base branch advances in a way that changes the staging result;
- a pull-request head SHA changes;
- the pull request closes, merges, or loses queue eligibility;
- merge method or other result-affecting queue configuration changes.

Invalid evidence must not release a pull request. The engine deletes or
abandons the obsolete staging attempt, re-resolves the current queue entries,
and creates fresh work as needed.

### Landing

- Only the earliest resolved pull request is released at a time.
- A passing queue status is written only after the candidate's evidence is
  valid and all earlier work is resolved.
- Shunt waits for the forge to report the pull request merged before releasing
  the next one.
- Existing cancellation, timeout, rejection, and outcome-comment behavior
  remains the single path for terminal outcomes.

## Durable state

The checkpoint must be sufficient to recover without trusting process memory:

- pending ordered candidates;
- active candidate inputs and staging identity;
- candidate outcome and phase timestamps;
- base identity/generation;
- queue sequence or attempt identity needed to avoid branch collisions;
- any frontier evidence required to continue reduction.

A restart must either resume a candidate whose recorded inputs still match or
safely discard it and requeue current work. It must never resume a gate result
for an old source head or old base.

Bolt/bbolt stores one JSON snapshot per queue in the existing queue bucket.
Adding fields to that JSON does not require a Bolt schema migration. If the
logical checkpoint format changes incompatibly, the loader must reject unsafe
in-flight state explicitly and rederive or requeue it rather than guessing.

## Acceptance criteria

The implementation is ready for a follow-up implementation PR when these
scenarios pass as engine tests:

1. **All pass:** an ordered batch is staged once and lands in queue order.
2. **Single failure:** a failed multi-PR candidate is reduced and the failing
   pull request is isolated without losing the remaining entries.
3. **Prefix conflict:** a staging conflict bounces the conflicting entry and
   preserves the order of entries after it.
4. **Head update:** changing one source head invalidates its old evidence and
   prevents the old result from releasing it.
5. **Base update:** advancing the base invalidates affected evidence and causes
   fresh staging.
6. **Restart:** a checkpoint resumes only matching work; unsafe or incompatible
   in-flight state is not treated as passing.
7. **Ordering:** a later passing candidate cannot release while an earlier
   candidate is unresolved.
8. **Closure:** closed or merged entries are removed without leaving an
   untracked staging branch.
9. **Persistence:** a Bolt save/load round trip preserves every field needed
   for the frontier and active-candidate recovery.
10. **No queue:** an idle queue removes its checkpoint using the existing
    lifecycle behavior.

## Operational requirements

- Emit enough structured logging to identify queue, candidate, staging attempt,
  and invalidation reason without logging credentials or private payloads.
- Expose metrics for active candidates, pending candidates, staging attempts,
  reductions, invalidations, bounces, and time spent waiting on gates.
- Document configuration only after behavior is implemented and tested.
- Keep the public deployment path dependency-light and compatible with the
  existing `make lint test build` validation.

## Implementation sequence

1. Land this PRD as the behavior contract.
2. Implement the neutral checkpoint/domain model with Bolt round-trip tests.
3. Implement deterministic serial frontier reduction in the engine.
4. Add restart, head-change, base-change, ordering, and failure-isolation
   tests before considering concurrency.
5. Measure serial behavior in representative queues.
6. Evaluate optional fan-out as a separate proposal and implementation.
7. Evaluate Postgres persistence and multi-replica coordination separately,
   with compatibility tests against the same domain model.

The PRD is complete when the implementation can be reviewed against the
acceptance criteria above without relying on a historical implementation or
private deployment assumptions.
