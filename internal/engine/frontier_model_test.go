package engine

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// This file contains an exhaustive model of the ordered frontier.
// It checks all Boolean gate outcomes for queues of up to seven candidates.
// The tests check that the model preserves the order of queue decisions.
//
// Candidates use their queue index. An exact key contains the accepted prefix
// and the candidate under test. The key uses comma-separated indexes.

func frontierKeyOf(nums []int) string {
	parts := make([]string, len(nums))
	for i, n := range nums {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ",")
}

// errUnknownFrontierOutcome is raised (as a value) by the model when the oracle
// has no answer for an exact key, so the exhaustive walker can fork both
// answers.
type errUnknownFrontierOutcome struct{ key string }

func (e errUnknownFrontierOutcome) Error() string { return "unknown outcome for key " + e.key }

type frontierEvent struct {
	outcome   string // "accept" or "reject"
	baseline  []int
	candidate []int
	key       string
}

type frontierResult struct {
	accepted []int
	rejected []int
	events   []frontierEvent
	lookups  []string
}

// resolveFrontier resolves one queue with exact keys and left-first recursion.
func resolveFrontier(size int, outcomes map[string]bool) (frontierResult, error) {
	var (
		accepted []int
		rejected []int
		events   []frontierEvent
		lookups  []string
		unknown  *errUnknownFrontierOutcome
	)

	test := func(candidate []int) bool {
		key := frontierKeyOf(append(append([]int(nil), accepted...), candidate...))
		lookups = append(lookups, key)
		out, ok := outcomes[key]
		if !ok {
			unknown = &errUnknownFrontierOutcome{key: key}
			panic(unknown)
		}
		return out
	}

	var resolve func(candidate []int)
	resolve = func(candidate []int) {
		baseline := append([]int(nil), accepted...)
		key := frontierKeyOf(append(append([]int(nil), baseline...), candidate...))
		if test(candidate) {
			events = append(events, frontierEvent{"accept", baseline, append([]int(nil), candidate...), key})
			accepted = append(accepted, candidate...)
			return
		}
		if len(candidate) == 1 {
			events = append(events, frontierEvent{"reject", baseline, append([]int(nil), candidate...), key})
			rejected = append(rejected, candidate...)
			return
		}
		middle := len(candidate) / 2
		resolve(candidate[:middle])
		resolve(candidate[middle:])
	}

	full := make([]int, size)
	for i := range full {
		full[i] = i
	}

	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				if unknown != nil {
					err = *unknown
					return
				}
				panic(r)
			}
		}()
		resolve(full)
		return nil
	}()
	if err != nil {
		return frontierResult{}, err
	}
	return frontierResult{accepted, rejected, events, lookups}, nil
}

// preservedFrontierPrefix returns held decisions before a changed candidate.
func preservedFrontierPrefix(events []frontierEvent, changed int) []frontierEvent {
	var prefix []frontierEvent
	for _, ev := range events {
		stop := false
		for _, n := range ev.candidate {
			if n >= changed {
				stop = true
				break
			}
		}
		if stop {
			break
		}
		prefix = append(prefix, ev)
	}
	return prefix
}

func assertFrontierInvariants(t *testing.T, size int, outcomes map[string]bool, r frontierResult) {
	t.Helper()

	union := append(append([]int(nil), r.accepted...), r.rejected...)
	sort.Ints(union)
	want := make([]int, size)
	for i := range want {
		want[i] = i
	}
	if frontierKeyOf(union) != frontierKeyOf(want) {
		t.Fatalf("accepted+rejected = %v, want every candidate 0..%d exactly once", union, size-1)
	}
	if !sort.IntsAreSorted(r.accepted) {
		t.Fatalf("accepted %v is not an ordered subsequence", r.accepted)
	}
	if !sort.IntsAreSorted(r.rejected) {
		t.Fatalf("rejected %v is not ordered", r.rejected)
	}

	var accumulated []int
	for _, ev := range r.events {
		if frontierKeyOf(ev.baseline) != frontierKeyOf(accumulated) {
			t.Fatalf("event baseline %v != running accumulator %v", ev.baseline, accumulated)
		}
		if ev.key != frontierKeyOf(append(append([]int(nil), ev.baseline...), ev.candidate...)) {
			t.Fatalf("event key %q != baseline+candidate", ev.key)
		}
		if outcomes[ev.key] != (ev.outcome == "accept") {
			t.Fatalf("event %q outcome %q disagrees with oracle %v", ev.key, ev.outcome, outcomes[ev.key])
		}
		if ev.outcome == "accept" {
			accumulated = append(accumulated, ev.candidate...)
		} else if len(ev.candidate) != 1 {
			t.Fatalf("rejected a non-singleton candidate %v", ev.candidate)
		}
	}
	if frontierKeyOf(accumulated) != frontierKeyOf(r.accepted) {
		t.Fatalf("event accumulator %v != accepted %v", accumulated, r.accepted)
	}
	if len(r.accepted) > 0 && !outcomes[frontierKeyOf(r.accepted)] {
		t.Fatalf("final accepted accumulator %v has no passing proof", r.accepted)
	}

	// Successor-root prefix inheritance: whatever crosses into a successor when
	// candidate `changed` is repinned must be whole decisions strictly to its
	// left, and the boundary decision must contain or start after `changed`.
	for changed := 0; changed < size; changed++ {
		inherited := preservedFrontierPrefix(r.events, changed)
		for _, ev := range inherited {
			for _, c := range ev.candidate {
				if c >= changed {
					t.Fatalf("inherited decision %v crosses changed candidate %d", ev.candidate, changed)
				}
			}
		}
		if len(inherited) < len(r.events) {
			boundary := r.events[len(inherited)].candidate
			maxc := boundary[0]
			for _, c := range boundary {
				if c > maxc {
					maxc = c
				}
			}
			if maxc < changed {
				t.Fatalf("boundary decision %v is entirely left of changed %d but was not inherited", boundary, changed)
			}
		}
	}
}

// TestFrontierModelExhaustive walks every deterministic Boolean-oracle path for
// n = 1..7 and checks the invariants plus the CI-run bounds from the design.
func TestFrontierModelExhaustive(t *testing.T) {
	for size := 1; size <= 7; size++ {
		frontier := []map[string]bool{{}}
		var completed []frontierResult

		for len(frontier) > 0 {
			outcomes := frontier[len(frontier)-1]
			frontier = frontier[:len(frontier)-1]

			r, err := resolveFrontier(size, outcomes)
			if err != nil {
				u := err.(errUnknownFrontierOutcome)
				for _, answer := range []bool{false, true} {
					branch := make(map[string]bool, len(outcomes)+1)
					for k, v := range outcomes {
						branch[k] = v
					}
					branch[u.key] = answer
					frontier = append(frontier, branch)
				}
				continue
			}
			assertFrontierInvariants(t, size, outcomes, r)
			completed = append(completed, r)
		}

		acceptedSets := map[string]bool{}
		minRuns, maxRuns := math.MaxInt, 0
		for _, r := range completed {
			acceptedSets[frontierKeyOf(r.accepted)] = true
			distinct := map[string]bool{}
			for _, k := range r.lookups {
				distinct[k] = true
			}
			if len(distinct) < minRuns {
				minRuns = len(distinct)
			}
			if len(distinct) > maxRuns {
				maxRuns = len(distinct)
			}
		}

		if len(completed) != 1<<size {
			t.Fatalf("n=%d: %d completed paths, want %d", size, len(completed), 1<<size)
		}
		if len(acceptedSets) != 1<<size {
			t.Fatalf("n=%d: %d distinct accepted sets, want %d", size, len(acceptedSets), 1<<size)
		}
		if minRuns != 1 {
			t.Fatalf("n=%d: min CI runs = %d, want 1 (root passes)", size, minRuns)
		}
		if maxRuns != 2*size-1 {
			t.Fatalf("n=%d: max CI runs = %d, want %d", size, maxRuns, 2*size-1)
		}
		t.Logf("n=%d: paths=%d, CI runs min=%d max=%d", size, len(completed), minRuns, maxRuns)
	}
}

// TestFrontierModelWorkedExample checks a seven-candidate frontier.
// It checks gate results, exact-key reuse, and the number of gate runs.
func TestFrontierModelWorkedExample(t *testing.T) {
	outcomes := map[string]bool{
		"0,1,2,3,4,5,6": false,
		"0,1,2":         false,
		"0":             true,
		"0,1":           false,
		"0,2":           false,
		"0,3,4,5,6":     true,
	}
	r, err := resolveFrontier(7, outcomes)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if frontierKeyOf(r.accepted) != "0,3,4,5,6" {
		t.Fatalf("accepted = %v, want A,D,E,F,G", r.accepted)
	}
	if frontierKeyOf(r.rejected) != "1,2" {
		t.Fatalf("rejected = %v, want B,C", r.rejected)
	}
	twice := 0
	for _, k := range r.lookups {
		if k == "0,1,2" {
			twice++
		}
	}
	if twice != 2 {
		t.Fatalf("ABC looked up %d times, want 2 (tested then reused)", twice)
	}
	distinct := map[string]bool{}
	for _, k := range r.lookups {
		distinct[k] = true
	}
	if len(distinct) != 6 {
		t.Fatalf("distinct CI runs = %d, want 6", len(distinct))
	}

	// F changes inside the held [D E F G] group: only A, B, C cross into the
	// successor root, and never a fragment of the successful group.
	var pref [][]int
	for _, ev := range preservedFrontierPrefix(r.events, 5) {
		pref = append(pref, ev.candidate)
	}
	if fmt.Sprint(pref) != "[[0] [1] [2]]" {
		t.Fatalf("preserved prefix for changed F = %v, want [[A] [B] [C]]", pref)
	}
}

// TestFrontierModelSparseFailureByPosition reproduces the design's
// single-intrinsic-failure CI-run profile: a lone bad candidate at each queue
// position, candidate-independent, and the resulting distinct-run counts.
func TestFrontierModelSparseFailureByPosition(t *testing.T) {
	const size = 7
	want := []int{5, 6, 5, 6, 5, 5, 4}
	for bad := 0; bad < size; bad++ {
		outcomes := map[string]bool{}
		var build func(prefix []int, start int)
		build = func(prefix []int, start int) {
			if len(prefix) > 0 {
				ok := true
				for _, n := range prefix {
					if n == bad {
						ok = false
					}
				}
				outcomes[frontierKeyOf(prefix)] = ok
			}
			for i := start; i < size; i++ {
				build(append(prefix, i), i+1)
			}
		}
		build(nil, 0)

		r, err := resolveFrontier(size, outcomes)
		if err != nil {
			t.Fatalf("bad=%d resolve: %v", bad, err)
		}
		var wantAccepted []int
		for n := 0; n < size; n++ {
			if n != bad {
				wantAccepted = append(wantAccepted, n)
			}
		}
		if frontierKeyOf(r.accepted) != frontierKeyOf(wantAccepted) {
			t.Fatalf("bad=%d accepted = %v, want %v", bad, r.accepted, wantAccepted)
		}
		if frontierKeyOf(r.rejected) != strconv.Itoa(bad) {
			t.Fatalf("bad=%d rejected = %v, want [%d]", bad, r.rejected, bad)
		}
		distinct := map[string]bool{}
		for _, k := range r.lookups {
			distinct[k] = true
		}
		if len(distinct) != want[bad] {
			t.Fatalf("bad=%d distinct CI runs = %d, want %d", bad, len(distinct), want[bad])
		}
	}
}
