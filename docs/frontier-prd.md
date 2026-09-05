# Frontier merge-queue model

**Status:** Proposed
**Audience:** Shunt maintainers and contributors
**Scope:** The queue engine and its durable state model

## Summary

Shunt needs a merge-queue model that tests an ordered group of pull requests,
isolates failures, and keeps making progress without losing the requested
order. The model must stay correct across restarts, source-head updates, and
base-branch movement.

This document is the product and behavior contract for that work. It is a
clean-room specification for Shunt. It does not prescribe implementation
details. Existing prototypes are not the source of truth. An implementer may
discard the current implementation and rebuild from these requirements.

## Problem

A merge queue normally learns only whether a staged group passed or failed.
When a group fails, testing every pull request separately is slow. Testing
many speculative groups at the same time is faster, but the results can stop
describing the queue after Shunt accepts or rejects an earlier group.

Shunt must preserve three properties together:

1. **Safety:** never report success for a pull request using evidence that
   does not include its current head and the current base.
2. **Order:** never release a later pull request before Shunt resolves an
   earlier queued pull request.
3. **Progress:** reduce a failed group to useful smaller tests. Do not block
   all later work without limit.

## Goals

- Represent unresolved work as an ordered frontier of candidate tests.
- Reuse valid test results. Make invalid or superseded results harmless.
- Isolate a failing pull request with fewer tests than one test per pull
  request.
- Resume safely after a process restart.
- Keep Bolt/bbolt as the primary local durable store. Add no new runtime
  dependency.
- Keep behavior observable through existing statuses, comments, logs, and
  metrics.
- Keep the design usable by an independent Shunt installation against
  Forgejo-compatible APIs.

## Non-goals

- Do not replace the Forgejo or Gitea merge worker or branch protection.
- Do not push the base branch directly. Do not bypass forge merge
  requirements.
- Do not coordinate multiple replicas in the first implementation.
- Do not make Postgres a prerequisite. Do not change its schema in the Bolt
  implementation. Postgres compatibility is a separate follow-up. It must not
  weaken the Bolt-first path.
- Do not add speculative fan-out before the serial frontier behavior is
  correct and measured.
- Do not add a new public API only for this feature.

## Product behavior

### Queue admission

- Shunt admits a pull request only if it is open, eligible for auto-merge, and
  passes the repository admission rules.
- Admission records the source head SHA and the base identity used for the
  decision.
- A source-head update clears the old evidence. The queue tests the current
  pull request again.

### Candidate tests

A candidate is an ordered list of pull requests plus the immutable inputs that
make its result meaningful:

- repository and base reference,
- base revision or generation,
- ordered pull-request numbers and source head SHAs,
- merge method and other queue configuration that affects the result,
- a unique staging attempt identity.

A candidate is not reusable because it has the same pull request numbers. Its
recorded inputs must match the inputs under evaluation.

Each candidate has one of three externally useful states:

- **unknown:** not yet tested, or no longer trustworthy,
- **passing:** the staged result passed the configured gate,
- **failing:** the staged result failed, or Shunt could not produce it.

Infrastructure errors do not silently become a passing or failing candidate.
They stay retryable, or they become an explicit terminal error through the
existing queue outcome path.

### Frontier reduction

The engine keeps ordered unresolved work. At each step it may:

1. test the earliest unresolved candidate,
2. accept a passing candidate when all earlier work is resolved,
3. split a failing candidate into smaller ordered candidates, or
4. bounce a candidate that cannot be staged because of a deterministic
   conflict.

A split must preserve queue order. It must not drop, duplicate, or reorder
pull requests. A result for a later candidate can stay as evidence. It cannot
release a later pull request, or mark success, while an earlier candidate
stays unresolved.

The first implementation uses a deterministic serial frontier. Any concurrent
fan-out is a later optimization. Fan-out needs explicit invalidation rules and
measurements that show it improves throughput without changing serial
behavior.

### Base and source-head changes

Evidence is invalid when any input changes:

- the base branch advances in a way that changes the staging result,
- a pull-request head SHA changes,
- the pull request closes, merges, or loses queue eligibility,
- the merge method or other result-affecting configuration changes.

Invalid evidence must not release a pull request. The engine deletes or
abandons the obsolete staging attempt, resolves the current queue entries
again, and creates fresh work as needed.

### Landing

- Shunt releases only the earliest resolved pull request at a time.
- Shunt writes a passing queue status only after the candidate evidence is
  valid and all earlier work is resolved.
- Shunt waits for the forge to report the pull request merged before it
  releases the next one.
- Existing cancellation, timeout, rejection, and outcome-comment behavior
  stays the single path for terminal outcomes.

## Durable state

The checkpoint must let a restart recover without process memory. It records:

- pending ordered candidates,
- active candidate inputs and the staging identity,
- candidate outcome and phase timestamps,
- base identity or generation,
- a queue sequence or attempt identity that prevents branch collisions,
- any frontier evidence needed to continue reduction.

A restart must resume a candidate whose recorded inputs still match, or
discard it safely and requeue current work. It must never resume a gate result
for an old source head or an old base.

Bolt/bbolt stores one JSON snapshot per queue in the existing queue bucket.
Adding fields to that JSON does not require a Bolt schema migration. If the
checkpoint format changes incompatibly, the loader must reject unsafe
in-flight state explicitly and rederive or requeue it. It must not guess.

## Acceptance criteria

The implementation is ready for a follow-up implementation PR when these
scenarios pass as engine tests:

1. **All pass:** Shunt stages an ordered batch once and lands it in queue
   order.
2. **Single failure:** the engine reduces a failed multi-PR candidate and
   isolates the failing pull request without losing the remaining entries.
3. **Prefix conflict:** a staging conflict bounces the conflicting entry and
   preserves the order of entries after it.
4. **Head update:** a changed source head invalidates its old evidence and
   prevents the old result from releasing it.
5. **Base update:** a base advance invalidates affected evidence and causes
   fresh staging.
6. **Restart:** a checkpoint resumes only matching work. Unsafe or
   incompatible in-flight state never counts as passing.
7. **Ordering:** a later passing candidate cannot release while an earlier
   candidate stays unresolved.
8. **Closure:** the engine deletes closed or merged entries without leaving
   an untracked staging branch.
9. **Persistence:** a Bolt save/load round trip preserves every field needed
   for frontier and active-candidate recovery.
10. **No queue:** an idle queue deletes its checkpoint with the existing
    lifecycle behavior.

## Operational requirements

- Emit structured logging that identifies the queue, candidate, staging
  attempt, and invalidation reason. Do not log credentials or private
  payloads.
- Expose metrics for active candidates, pending candidates, staging attempts,
  reductions, invalidations, bounces, and gate wait time.
- Document configuration only after Shunt implements and tests the behavior.
- Keep the public deployment path dependency-light and compatible with
  `make lint test build`.

## Implementation sequence

1. Land this PRD as the behavior contract.
2. Implement the neutral checkpoint and domain model with Bolt round-trip
   tests.
3. Implement deterministic serial frontier reduction in the engine.
4. Add restart, head-change, base-change, ordering, and failure-isolation
   tests before concurrency work.
5. Measure serial behavior in representative queues.
6. Evaluate optional fan-out as a separate proposal and implementation.
7. Evaluate Postgres persistence and multi-replica coordination separately.
   Test them against the same domain model.

The PRD is complete when an implementation can be reviewed against the
acceptance criteria above. The review must not rely on a historical
implementation or private deployment assumptions.
