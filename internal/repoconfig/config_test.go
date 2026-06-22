package repoconfig

import (
	"strings"
	"testing"
	"time"
)

func TestApplyUsesDefaultsForEmptyConfig(t *testing.T) {
	def := Settings{
		Base:         "main",
		StatusCtx:    "merge-queue",
		MergeStyle:   "merge",
		MaxBatch:     0,
		BatchLinger:  time.Second,
		BatchTarget:  2,
		BisectFanout: 1,
	}
	got, err := Apply(nil, def)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got != def {
		t.Fatalf("Apply(nil) = %+v, want %+v", got, def)
	}
}

func TestApplyOverridesAllSupportedFields(t *testing.T) {
	def := Settings{
		Base:         "main",
		StatusCtx:    "merge-queue",
		MergeStyle:   "merge",
		MaxBatch:     0,
		BatchLinger:  0,
		BatchTarget:  0,
		BisectFanout: 1,
	}
	got, err := Apply([]byte(`
base: trunk
status_context: shunt
merge_style: squash
max_batch: 4
batch_linger: 30s
batch_target: 3
bisect_fanout: 2
`), def)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	want := Settings{
		Base:         "trunk",
		StatusCtx:    "shunt",
		MergeStyle:   "squash",
		MaxBatch:     4,
		BatchLinger:  30 * time.Second,
		BatchTarget:  3,
		BisectFanout: 2,
	}
	if got != want {
		t.Fatalf("Apply = %+v, want %+v", got, want)
	}
}

func TestApplyRejectsInvalidConfigWithoutPartialSettings(t *testing.T) {
	def := Settings{Base: "main", StatusCtx: "merge-queue", MergeStyle: "merge", BisectFanout: 1}
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "unknown field", body: "unknown: true\n", want: "field unknown not found"},
		{name: "bad duration", body: "batch_linger: soon\n", want: "batch_linger"},
		{name: "negative batch", body: "max_batch: -1\n", want: "max_batch"},
		{name: "bad fanout", body: "bisect_fanout: 0\n", want: "bisect_fanout"},
		{name: "bad merge", body: "merge_style: fast-forward\n", want: "merge_style"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := Apply([]byte(tc.body), def); err == nil {
				t.Fatalf("Apply succeeded with %+v", got)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Apply error = %q, want substring %q", err, tc.want)
			}
		})
	}
}
