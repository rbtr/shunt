package checkpoint

import "testing"

func TestQueueSnapshotFormatVersion(t *testing.T) {
	key := QueueKey{Owner: "o", Repo: "r", Base: "main"}

	for _, version := range []int{0, CurrentFormatVersion} {
		if err := (QueueSnapshot{Key: key, FormatVersion: version}).Validate(); err != nil {
			t.Fatalf("format version %d: %v", version, err)
		}
	}
	if err := (QueueSnapshot{Key: key, FormatVersion: CurrentFormatVersion + 1}).Validate(); err == nil {
		t.Fatal("future format version passed validation")
	}
}
