package leader

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestFileLockExcludesOtherHoldersAndReleases(t *testing.T) {
	dir := filepath.Join("testdata", fmt.Sprintf("leader-lock-%d", os.Getpid()))
	path := filepath.Join(dir, "shunt.lock")
	t.Cleanup(func() {
		_ = os.Remove(path)
		_ = os.Remove(dir)
	})

	first, err := NewFileLock(path)
	if err != nil {
		t.Fatalf("NewFileLock: %v", err)
	}
	firstLease, err := first.TryAcquire()
	if err != nil {
		t.Fatalf("first TryAcquire: %v", err)
	}

	second, err := NewFileLock(path)
	if err != nil {
		t.Fatalf("second NewFileLock: %v", err)
	}
	if _, err := second.TryAcquire(); !errors.Is(err, ErrLocked) {
		t.Fatalf("second TryAcquire error = %v, want ErrLocked", err)
	}

	if err := firstLease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	secondLease, err := second.TryAcquire()
	if err != nil {
		t.Fatalf("TryAcquire after release: %v", err)
	}
	if err := secondLease.Release(); err != nil {
		t.Fatalf("second Release: %v", err)
	}
}

func TestNewFileLockRequiresPath(t *testing.T) {
	if _, err := NewFileLock(""); err == nil {
		t.Fatal("NewFileLock should reject an empty path")
	}
}
