// Package checkpoint defines durable queue snapshot types.
//
// The types (and their methods) live in the public mq/checkpoint package; these
// aliases keep existing internal imports compiling unchanged.
package checkpoint

import (
	pub "github.com/rbtr/shunt/mq/checkpoint"
)

type QueueKey = pub.QueueKey
type QueueSnapshot = pub.QueueSnapshot
type ActiveBatchSnapshot = pub.ActiveBatchSnapshot
type PullRequestSnapshot = pub.PullRequestSnapshot

// Store is the public mq/checkpoint.Store (kept here for internal imports).
type Store = pub.Store
