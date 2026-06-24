// Package bolt provides a bbolt-backed queue checkpoint store.
package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rbtr/shunt/internal/checkpoint"
	bbolt "go.etcd.io/bbolt"
)

var queueBucket = []byte("queues")

// Store persists queue checkpoints in a local bbolt database.
type Store struct {
	db *bbolt.DB
}

// Open opens or creates a bbolt checkpoint store.
func Open(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) LoadQueue(ctx context.Context, key checkpoint.QueueKey) (checkpoint.QueueSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return checkpoint.QueueSnapshot{}, false, err
	}
	if err := key.Validate(); err != nil {
		return checkpoint.QueueSnapshot{}, false, err
	}
	var snapshot checkpoint.QueueSnapshot
	var found bool
	if err := s.db.View(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		b := tx.Bucket(queueBucket)
		if b == nil {
			return nil
		}
		data := b.Get(queueKey(key))
		if data == nil {
			return nil
		}
		found = true
		if err := json.Unmarshal(data, &snapshot); err != nil {
			return fmt.Errorf("decode queue checkpoint: %w", err)
		}
		return snapshot.Validate()
	}); err != nil {
		return checkpoint.QueueSnapshot{}, false, err
	}
	return snapshot, found, nil
}

func (s *Store) SaveQueue(ctx context.Context, snapshot checkpoint.QueueSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode queue checkpoint: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := tx.CreateBucketIfNotExists(queueBucket)
		if err != nil {
			return err
		}
		return b.Put(queueKey(snapshot.Key), data)
	})
}

func (s *Store) DeleteQueue(ctx context.Context, key checkpoint.QueueKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := key.Validate(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		b := tx.Bucket(queueBucket)
		if b == nil {
			return nil
		}
		return b.Delete(queueKey(key))
	})
}

func queueKey(key checkpoint.QueueKey) []byte {
	return []byte(key.Owner + "\x00" + key.Repo + "\x00" + key.Base)
}
