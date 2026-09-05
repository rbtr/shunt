package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbtr/shunt/internal/checkpoint"
)

func TestPostgresSaveQueueUpsertsSnapshot(t *testing.T) {
	linger := time.Date(2026, 6, 22, 21, 30, 0, 0, time.FixedZone("local", -5*60*60))
	db := &fakeDB{}
	store := &Store{db: db, stateTbl: "shunt_queue_state", leaseTbl: "shunt_queue_leases"}
	snapshot := checkpoint.QueueSnapshot{
		Key:     checkpoint.QueueKey{Owner: "octo", Repo: "app", Base: "main"},
		Pending: [][]int{{1, 2}, {3}},
		Active: []checkpoint.ActiveBatchSnapshot{{
			PRs:            []checkpoint.PullRequestSnapshot{{Number: 4, HeadSHA: "abc123"}},
			StagingBranch:  "mq/main/staging-1",
			StagingSHA:     "stage123",
			BaseGeneration: 2,
			Outcome:        "failure",
		}},
		LingerSince:     linger,
		BaseGeneration:  2,
		StagingSequence: 7,
		FormatVersion:   checkpoint.CurrentFormatVersion,
	}

	if err := store.SaveQueue(context.Background(), snapshot); err != nil {
		t.Fatalf("SaveQueue: %v", err)
	}
	if len(db.execs) != 1 {
		t.Fatalf("execs = %d, want 1", len(db.execs))
	}
	exec := db.execs[0]
	if !strings.Contains(exec.query, "INSERT INTO shunt_queue_state") || !strings.Contains(exec.query, "ON CONFLICT") {
		t.Fatalf("query does not upsert queue state:\n%s", exec.query)
	}
	if got := exec.args[:3]; !reflect.DeepEqual(got, []any{"octo", "app", "main"}) {
		t.Fatalf("key args = %#v, want owner/repo/base", got)
	}
	var pending [][]int
	if err := json.Unmarshal([]byte(exec.args[3].(string)), &pending); err != nil {
		t.Fatalf("pending json: %v", err)
	}
	if !reflect.DeepEqual(pending, [][]int{{1, 2}, {3}}) {
		t.Fatalf("pending = %#v", pending)
	}
	var activeJSON []activeBatchJSON
	if err := json.Unmarshal([]byte(exec.args[4].(string)), &activeJSON); err != nil {
		t.Fatalf("active json: %v", err)
	}
	active := activeFromJSON(activeJSON)
	if !reflect.DeepEqual(active, snapshot.Active) {
		t.Fatalf("active = %#v, want %#v", active, snapshot.Active)
	}
	if got, want := exec.args[5].(time.Time), linger.UTC(); !got.Equal(want) {
		t.Fatalf("linger = %v, want %v", got, want)
	}
	if got := exec.args[6:8]; !reflect.DeepEqual(got, []any{2, 7}) {
		t.Fatalf("generation args = %#v", got)
	}
	if got := exec.args[8]; got != checkpoint.CurrentFormatVersion {
		t.Fatalf("format_version arg = %#v, want %d", got, checkpoint.CurrentFormatVersion)
	}
	if !strings.Contains(exec.query, "format_version") {
		t.Fatalf("upsert does not write format_version:\n%s", exec.query)
	}
	// $10 is the whole snapshot, verbatim — the field nothing can drop.
	if !strings.Contains(exec.query, "snapshot") {
		t.Fatalf("upsert does not write the snapshot column:\n%s", exec.query)
	}
	var full checkpoint.QueueSnapshot
	if err := json.Unmarshal([]byte(exec.args[9].(string)), &full); err != nil {
		t.Fatalf("snapshot json: %v", err)
	}
	if !reflect.DeepEqual(full.Pending, snapshot.Pending) ||
		!reflect.DeepEqual(full.Active, snapshot.Active) ||
		full.FormatVersion != snapshot.FormatVersion ||
		full.StagingSequence != snapshot.StagingSequence ||
		!full.LingerSince.Equal(snapshot.LingerSince) {
		t.Fatalf("snapshot blob did not round-trip: %#v", full)
	}
}

func TestPostgresRoundTripsEveryField(t *testing.T) {
	// A snapshot that exercises the fields the column layout never persisted:
	// the bisection Trees (with anchor + results cache + held leaves), the
	// PendingNodes lineage, the TransitionOutbox, and the per-batch RunID /
	// BaseAnchor / ExactKey / LineagePath.
	snapshot := checkpoint.QueueSnapshot{
		Key:     checkpoint.QueueKey{Owner: "octo", Repo: "app", Base: "main"},
		Pending: [][]int{{9}},
		PendingNodes: []checkpoint.PendingNodeSnapshot{
			{PRs: []int{9}, RunID: "run-9", Path: "L"},
		},
		Active: []checkpoint.ActiveBatchSnapshot{{
			PRs:           []checkpoint.PullRequestSnapshot{{Number: 4, HeadSHA: "abc"}},
			StagingBranch: "mq/main/staging-run-7-L",
			StagingSHA:    "stage7",
			RunID:         "run-7",
			LineagePath:   "L",
			ExactKey:      "v2|anchorsha|merge|4",
			BaseAnchor:    "anchorsha",
		}},
		Trees: []checkpoint.BisectionTreeSnapshot{{
			RunID:    "run-7",
			Anchor:   "anchorsha",
			Open:     1,
			Cursor:   0,
			Accepted: []checkpoint.PullRequestSnapshot{{Number: 3, HeadSHA: "cccc"}},
			Results:  map[string]string{"v2|anchorsha|merge|3": "success"},
			Held: []checkpoint.HeldLeafSnapshot{{
				Batch: checkpoint.ActiveBatchSnapshot{
					PRs:           []checkpoint.PullRequestSnapshot{{Number: 5, HeadSHA: "eeee"}},
					StagingBranch: "mq/main/staging-run-7-LR",
					StagingSHA:    "stage7lr",
					RunID:         "run-7",
					LineagePath:   "LR",
				},
				Outcome: "success",
			}},
		}},
		TransitionOutbox: []checkpoint.OutboxTransitionSnapshot{{
			Kind: "landed", PRs: []int{3}, RunID: "run-7", EventID: "run-7|landed|3", Attempts: 2,
		}},
		BaseGeneration:  1,
		StagingSequence: 4,
		FormatVersion:   checkpoint.CurrentFormatVersion,
	}

	db := &fakeDB{}
	store := &Store{db: db, stateTbl: "shunt_queue_state", leaseTbl: "shunt_queue_leases"}
	if err := store.SaveQueue(context.Background(), snapshot); err != nil {
		t.Fatalf("SaveQueue: %v", err)
	}
	blob := db.execs[0].args[9].(string)

	load := &fakeDB{rows: []fakeRow{{scan: func(dest ...any) error {
		*dest[0].(*[]byte) = []byte(`[]`)
		*dest[1].(*[]byte) = []byte(`[]`)
		*dest[2].(*sql.NullTime) = sql.NullTime{}
		*dest[3].(*int) = 0
		*dest[4].(*int) = 0
		*dest[5].(*int) = checkpoint.CurrentFormatVersion
		*dest[6].(*[]byte) = []byte(blob)
		return nil
	}}}}
	loadStore := &Store{db: load, stateTbl: "shunt_queue_state", leaseTbl: "shunt_queue_leases"}
	got, ok, err := loadStore.LoadQueue(context.Background(), snapshot.Key)
	if err != nil || !ok {
		t.Fatalf("LoadQueue: ok=%v err=%v", ok, err)
	}
	if !reflect.DeepEqual(got, snapshot.Clone()) {
		t.Fatalf("round trip lost data:\n got  %#v\n want %#v", got, snapshot.Clone())
	}
}

func TestPostgresLoadQueueDecodesSnapshot(t *testing.T) {
	linger := time.Date(2026, 6, 22, 21, 30, 0, 0, time.UTC)
	db := &fakeDB{rows: []fakeRow{{
		scan: func(dest ...any) error {
			*dest[0].(*[]byte) = []byte(`[[1,2],[3]]`)
			*dest[1].(*[]byte) = []byte(`[{"prs":[{"number":4,"head_sha":"abc123"}],"staging_branch":"mq/main/staging-1","staging_sha":"stage123","base_generation":2,"outcome":"success"}]`)
			*dest[2].(*sql.NullTime) = sql.NullTime{Time: linger, Valid: true}
			*dest[3].(*int) = 2
			*dest[4].(*int) = 7
			*dest[5].(*int) = checkpoint.CurrentFormatVersion
			return nil
		},
	}}}
	store := &Store{db: db, stateTbl: "shunt_queue_state", leaseTbl: "shunt_queue_leases"}

	snapshot, ok, err := store.LoadQueue(context.Background(), checkpoint.QueueKey{Owner: "octo", Repo: "app", Base: "main"})
	if err != nil {
		t.Fatalf("LoadQueue: %v", err)
	}
	if !ok {
		t.Fatal("LoadQueue ok = false, want true")
	}
	if got := snapshot.Pending; !reflect.DeepEqual(got, [][]int{{1, 2}, {3}}) {
		t.Fatalf("pending = %#v", got)
	}
	wantActive := []checkpoint.ActiveBatchSnapshot{{
		PRs:            []checkpoint.PullRequestSnapshot{{Number: 4, HeadSHA: "abc123"}},
		StagingBranch:  "mq/main/staging-1",
		StagingSHA:     "stage123",
		BaseGeneration: 2,
		Outcome:        "success",
	}}
	if !reflect.DeepEqual(snapshot.Active, wantActive) {
		t.Fatalf("active = %#v, want %#v", snapshot.Active, wantActive)
	}
	if !snapshot.LingerSince.Equal(linger) || snapshot.BaseGeneration != 2 || snapshot.StagingSequence != 7 {
		t.Fatalf("snapshot metadata = %#v", snapshot)
	}
	if snapshot.FormatVersion != checkpoint.CurrentFormatVersion {
		t.Fatalf("format version = %d, want %d", snapshot.FormatVersion, checkpoint.CurrentFormatVersion)
	}
	if len(db.queries) != 1 || !strings.Contains(db.queries[0].query, "format_version") {
		t.Fatalf("queries = %#v", db.queries)
	}
}

func TestPostgresLoadQueueMissingReturnsFalse(t *testing.T) {
	db := &fakeDB{rows: []fakeRow{{err: sql.ErrNoRows}}}
	store := &Store{db: db, stateTbl: "shunt_queue_state", leaseTbl: "shunt_queue_leases"}

	_, ok, err := store.LoadQueue(context.Background(), checkpoint.QueueKey{Owner: "octo", Repo: "app", Base: "main"})
	if err != nil {
		t.Fatalf("LoadQueue: %v", err)
	}
	if ok {
		t.Fatal("LoadQueue ok = true, want false")
	}
}

func TestPostgresApplyMigrationsAndDelete(t *testing.T) {
	db := &fakeDB{}
	store := &Store{db: db, stateTbl: "shunt_queue_state", leaseTbl: "shunt_queue_leases"}

	if err := store.ApplyMigrations(context.Background()); err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}
	if err := store.DeleteQueue(context.Background(), checkpoint.QueueKey{Owner: "octo", Repo: "app", Base: "main"}); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	if len(db.execs) != 5 {
		t.Fatalf("execs = %d, want 5", len(db.execs))
	}
	if !strings.Contains(db.execs[0].query, "CREATE TABLE IF NOT EXISTS shunt_queue_state") {
		t.Fatalf("first migration query = %q", db.execs[0].query)
	}
	if !strings.Contains(db.execs[1].query, "CREATE TABLE IF NOT EXISTS shunt_queue_leases") {
		t.Fatalf("second migration query = %q", db.execs[1].query)
	}
	if !strings.Contains(db.execs[2].query, "ADD COLUMN IF NOT EXISTS format_version") {
		t.Fatalf("third migration query = %q", db.execs[2].query)
	}
	if !strings.Contains(db.execs[3].query, "ADD COLUMN IF NOT EXISTS snapshot") {
		t.Fatalf("fourth migration query = %q", db.execs[3].query)
	}
	if !strings.Contains(db.execs[4].query, "DELETE FROM shunt_queue_state") {
		t.Fatalf("delete query = %q", db.execs[4].query)
	}
	if got := db.execs[4].args; !reflect.DeepEqual(got, []any{"octo", "app", "main"}) {
		t.Fatalf("delete args = %#v", got)
	}
}

func TestPostgresAcquireLeaseAcquiresAndRenews(t *testing.T) {
	db := &fakeDB{rows: []fakeRow{leaseRow(), leaseRow()}}
	store := &Store{db: db, stateTbl: "shunt_queue_state", leaseTbl: "shunt_queue_leases"}
	key := checkpoint.QueueKey{Owner: "octo", Repo: "app", Base: "main"}

	for _, holderID := range []string{"holder-a", "holder-a"} {
		acquired, err := store.AcquireLease(context.Background(), key, holderID, 30*time.Second)
		if err != nil {
			t.Fatalf("AcquireLease(%q): %v", holderID, err)
		}
		if !acquired {
			t.Fatalf("AcquireLease(%q) acquired = false, want true", holderID)
		}
	}

	if len(db.queries) != 2 {
		t.Fatalf("queries = %d, want 2", len(db.queries))
	}
	for _, query := range db.queries {
		if !strings.Contains(query.query, "INSERT INTO shunt_queue_leases") ||
			!strings.Contains(query.query, "shunt_queue_leases.holder_id = EXCLUDED.holder_id") {
			t.Fatalf("query does not acquire or renew a queue lease:\n%s", query.query)
		}
		if got := query.args; !reflect.DeepEqual(got, []any{"octo", "app", "main", "holder-a", int64(30_000_000)}) {
			t.Fatalf("lease args = %#v", got)
		}
	}
}

func TestPostgresAcquireLeaseActiveContentionReturnsFalse(t *testing.T) {
	db := &fakeDB{rows: []fakeRow{{err: sql.ErrNoRows}}}
	store := &Store{db: db, stateTbl: "shunt_queue_state", leaseTbl: "shunt_queue_leases"}

	acquired, err := store.AcquireLease(context.Background(), checkpoint.QueueKey{Owner: "octo", Repo: "app", Base: "main"}, "holder-b", time.Minute)
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if acquired {
		t.Fatal("AcquireLease acquired = true, want false for active foreign holder")
	}
}

func TestPostgresAcquireLeaseExpiredHolderCanBeTakenOver(t *testing.T) {
	db := &fakeDB{rows: []fakeRow{leaseRow()}}
	store := &Store{db: db, stateTbl: "shunt_queue_state", leaseTbl: "shunt_queue_leases"}

	acquired, err := store.AcquireLease(context.Background(), checkpoint.QueueKey{Owner: "octo", Repo: "app", Base: "main"}, "holder-b", time.Minute)
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if !acquired {
		t.Fatal("AcquireLease acquired = false, want true after foreign lease expiry")
	}
	if len(db.queries) != 1 || !strings.Contains(db.queries[0].query, "shunt_queue_leases.expires_at <= now()") {
		t.Fatalf("query does not permit expired lease takeover: %#v", db.queries)
	}
}

func leaseRow() fakeRow {
	return fakeRow{scan: func(dest ...any) error {
		*dest[0].(*time.Time) = time.Date(2026, 6, 22, 21, 30, 0, 0, time.UTC)
		return nil
	}}
}

func TestSnapshotValidation(t *testing.T) {
	valid := checkpoint.QueueSnapshot{Key: checkpoint.QueueKey{Owner: "o", Repo: "r", Base: "main"}, Pending: [][]int{{1}}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid snapshot: %v", err)
	}
	invalid := checkpoint.QueueSnapshot{Key: checkpoint.QueueKey{Owner: "o", Repo: "r", Base: "main"}, Active: []checkpoint.ActiveBatchSnapshot{{StagingBranch: "mq/main/staging", StagingSHA: "sha", PRs: []checkpoint.PullRequestSnapshot{{Number: 1}}}}}
	if err := invalid.Validate(); err == nil {
		t.Fatal("invalid active PR without head SHA passed validation")
	}
}

type fakeDB struct {
	execs   []fakeCall
	queries []fakeCall
	rows    []fakeRow
	execErr error
}

type fakeCall struct {
	query string
	args  []any
}

func (f *fakeDB) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	f.execs = append(f.execs, fakeCall{query: query, args: append([]any(nil), args...)})
	return fakeResult(0), f.execErr
}

func (f *fakeDB) QueryRowContext(_ context.Context, query string, args ...any) rowScanner {
	f.queries = append(f.queries, fakeCall{query: query, args: append([]any(nil), args...)})
	if len(f.rows) == 0 {
		return fakeRow{err: sql.ErrNoRows}
	}
	row := f.rows[0]
	f.rows = f.rows[1:]
	return row
}

type fakeRow struct {
	scan func(dest ...any) error
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return r.scan(dest...)
}

type fakeResult int64

func (f fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (f fakeResult) RowsAffected() (int64, error) { return int64(f), nil }
