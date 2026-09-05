// Package checkpoint defines durable queue snapshot types.
//
// The types (and their methods) live in the public mq/checkpoint package; these
// aliases keep existing internal imports compiling unchanged.
package checkpoint

import (
	pub "github.com/rbtr/shunt/mq/checkpoint"
)

const CurrentFormatVersion = pub.CurrentFormatVersion

type QueueKey = pub.QueueKey
type QueueSnapshot = pub.QueueSnapshot
type ActiveBatchSnapshot = pub.ActiveBatchSnapshot
type PullRequestSnapshot = pub.PullRequestSnapshot
type PendingNodeSnapshot = pub.PendingNodeSnapshot
type BisectionTreeSnapshot = pub.BisectionTreeSnapshot
type HeldLeafSnapshot = pub.HeldLeafSnapshot
type OutboxTransitionSnapshot = pub.OutboxTransitionSnapshot

// Store is the public mq/checkpoint.Store (kept here for internal imports).
type Store = pub.Store
