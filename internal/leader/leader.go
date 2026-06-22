// Package leader provides optional process leadership primitives.
package leader

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

var ErrLocked = errors.New("leader lock already held")

type Lock interface {
	TryAcquire() (*Lease, error)
}

type FileLock struct {
	path string
}

type Lease struct {
	file *os.File
}

func NewFileLock(path string) (*FileLock, error) {
	if path == "" {
		return nil, errors.New("leader lock path is required")
	}
	return &FileLock{path: path}, nil
}

func (l *FileLock) TryAcquire() (*Lease, error) {
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return nil, fmt.Errorf("create leader lock directory: %w", err)
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open leader lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("acquire leader lock: %w", err)
	}
	return &Lease{file: f}, nil
}

func (l *Lease) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	if closeErr := l.file.Close(); err == nil {
		err = closeErr
	}
	l.file = nil
	return err
}
