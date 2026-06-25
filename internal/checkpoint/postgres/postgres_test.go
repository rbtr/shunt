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
	store := &Store{db: db}
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
			return nil
		},
	}}}
	store := &Store{db: db}

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
	if len(db.queries) != 1 || !strings.Contains(db.queries[0].query, "SELECT pending, active") {
		t.Fatalf("queries = %#v", db.queries)
	}
}

func TestPostgresLoadQueueMissingReturnsFalse(t *testing.T) {
	db := &fakeDB{rows: []fakeRow{{err: sql.ErrNoRows}}}
	store := &Store{db: db}

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
	store := &Store{db: db}

	if err := store.ApplyMigrations(context.Background()); err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}
	if err := store.DeleteQueue(context.Background(), checkpoint.QueueKey{Owner: "octo", Repo: "app", Base: "main"}); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	if len(db.execs) != 2 {
		t.Fatalf("execs = %d, want 2", len(db.execs))
	}
	if !strings.Contains(db.execs[0].query, "CREATE TABLE IF NOT EXISTS shunt_queue_state") {
		t.Fatalf("migration query = %q", db.execs[0].query)
	}
	if !strings.Contains(db.execs[1].query, "DELETE FROM shunt_queue_state") {
		t.Fatalf("delete query = %q", db.execs[1].query)
	}
	if got := db.execs[1].args; !reflect.DeepEqual(got, []any{"octo", "app", "main"}) {
		t.Fatalf("delete args = %#v", got)
	}
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
