package engine

import (
	"context"
	"time"

	"github.com/rbtr/shunt/internal/checkpoint"
)

// QueueLease atomically acquires or renews exclusive ownership of one queue.
// Implementations that coordinate multiple replicas must use a durable store.
type QueueLease interface {
	AcquireLease(ctx context.Context, key checkpoint.QueueKey, holderID string, ttl time.Duration) (bool, error)
}

type alwaysHeldLease struct{}

func (alwaysHeldLease) AcquireLease(context.Context, checkpoint.QueueKey, string, time.Duration) (bool, error) {
	return true, nil
}
