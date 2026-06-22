package bolt

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/rbtr/shunt/internal/checkpoint"
)

func TestStoreSavesLoadsAndDeletesQueue(t *testing.T) {
	store, err := Open(t.TempDir() + "/shunt.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	snapshot := checkpoint.QueueSnapshot{
		Key: checkpoint.QueueKey{Owner: "o", Repo: "r", Base: "main"},
		Pending: [][]int{
			{1, 2},
			{3},
		},
		Active: []checkpoint.ActiveBatchSnapshot{{
			PRs: []checkpoint.PullRequestSnapshot{
				{Number: 4, HeadSHA: "head-4"},
			},
			StagingBranch:  "mq/main/staging",
			StagingSHA:     "stage-4",
			BaseGeneration: 2,
			Outcome:        "failure",
		}},
		LingerSince:     time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC),
		BaseGeneration:  2,
		StagingSequence: 7,
	}

	if err := store.SaveQueue(context.Background(), snapshot); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := store.LoadQueue(context.Background(), snapshot.Key)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !ok {
		t.Fatal("load ok = false, want true")
	}
	if !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("loaded snapshot = %#v, want %#v", got, snapshot)
	}
	if err := store.DeleteQueue(context.Background(), snapshot.Key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, err := store.LoadQueue(context.Background(), snapshot.Key); err != nil || ok {
		t.Fatalf("load after delete ok/err = %v/%v, want false/nil", ok, err)
	}
}

func TestStoreSeparatesQueueKeys(t *testing.T) {
	store, err := Open(t.TempDir() + "/shunt.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	a := checkpoint.QueueSnapshot{Key: checkpoint.QueueKey{Owner: "o", Repo: "r", Base: "main"}, Pending: [][]int{{1}}}
	b := checkpoint.QueueSnapshot{Key: checkpoint.QueueKey{Owner: "o", Repo: "r", Base: "release"}, Pending: [][]int{{2}}}
	if err := store.SaveQueue(context.Background(), a); err != nil {
		t.Fatalf("save a: %v", err)
	}
	if err := store.SaveQueue(context.Background(), b); err != nil {
		t.Fatalf("save b: %v", err)
	}
	got, ok, err := store.LoadQueue(context.Background(), a.Key)
	if err != nil {
		t.Fatalf("load a: %v", err)
	}
	if !ok || !reflect.DeepEqual(got, a) {
		t.Fatalf("loaded a = %#v ok=%v, want %#v true", got, ok, a)
	}
}

func TestStoreHonorsCanceledContext(t *testing.T) {
	store, err := Open(t.TempDir() + "/shunt.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = store.SaveQueue(ctx, checkpoint.QueueSnapshot{Key: checkpoint.QueueKey{Owner: "o", Repo: "r", Base: "main"}, Pending: [][]int{{1}}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("save canceled err = %v, want context.Canceled", err)
	}
}
