// Package repoconfig parses per-repository shunt configuration files.
package repoconfig

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const FileName = ".shunt.yml"

type Settings struct {
	Base               string
	StatusCtx          string
	MergeStyle         string
	MaxBatch           int
	BatchLinger        time.Duration
	BatchTarget        int
	InitialBatchFanout int
	BisectFanout       int
}

type fileConfig struct {
	Base               *string `yaml:"base"`
	StatusCtx          *string `yaml:"status_context"`
	MergeStyle         *string `yaml:"merge_style"`
	MaxBatch           *int    `yaml:"max_batch"`
	BatchLinger        *string `yaml:"batch_linger"`
	BatchTarget        *int    `yaml:"batch_target"`
	InitialBatchFanout *int    `yaml:"initial_batch_fanout"`
	BisectFanout       *int    `yaml:"bisect_fanout"`
}

func Apply(data []byte, defaults Settings) (Settings, error) {
	var cfg fileConfig
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && err != io.EOF {
		return Settings{}, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return Settings{}, fmt.Errorf("multiple YAML documents are not supported")
		}
		return Settings{}, err
	}

	out := defaults
	if cfg.Base != nil {
		base := strings.TrimSpace(*cfg.Base)
		if base == "" {
			return Settings{}, fmt.Errorf("base must not be empty")
		}
		out.Base = base
	}
	if cfg.StatusCtx != nil {
		statusCtx := strings.TrimSpace(*cfg.StatusCtx)
		if statusCtx == "" {
			return Settings{}, fmt.Errorf("status_context must not be empty")
		}
		out.StatusCtx = statusCtx
	}
	if cfg.MergeStyle != nil {
		style, err := normalizeMergeStyle(*cfg.MergeStyle)
		if err != nil {
			return Settings{}, err
		}
		out.MergeStyle = style
	}
	if cfg.MaxBatch != nil {
		if *cfg.MaxBatch < 0 {
			return Settings{}, fmt.Errorf("max_batch must be a non-negative integer")
		}
		out.MaxBatch = *cfg.MaxBatch
	}
	if cfg.BatchLinger != nil {
		linger, err := time.ParseDuration(strings.TrimSpace(*cfg.BatchLinger))
		if err != nil || linger < 0 {
			return Settings{}, fmt.Errorf("batch_linger must be a non-negative duration")
		}
		out.BatchLinger = linger
	}
	if cfg.BatchTarget != nil {
		if *cfg.BatchTarget < 0 {
			return Settings{}, fmt.Errorf("batch_target must be a non-negative integer")
		}
		out.BatchTarget = *cfg.BatchTarget
	}
	if cfg.InitialBatchFanout != nil {
		if *cfg.InitialBatchFanout < 1 {
			return Settings{}, fmt.Errorf("initial_batch_fanout must be a positive integer")
		}
		out.InitialBatchFanout = *cfg.InitialBatchFanout
	}
	if cfg.BisectFanout != nil {
		if *cfg.BisectFanout < 1 {
			return Settings{}, fmt.Errorf("bisect_fanout must be a positive integer")
		}
		out.BisectFanout = *cfg.BisectFanout
	}
	return out, nil
}

func normalizeMergeStyle(style string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(style)) {
	case "", "merge", "merge-commit", "merge_commit":
		return "merge", nil
	case "squash":
		return "squash", nil
	case "rebase":
		return "rebase", nil
	default:
		return "", fmt.Errorf("unsupported merge_style %q: use merge, squash, or rebase", style)
	}
}
