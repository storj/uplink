// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package stalldetection

import (
	"time"

	"storj.io/eventkit"
)

var evs = eventkit.Package()

// Config contains configuration for stall detection and handling.
// Empty struct implies default values.
// Nil implies stall manager is disabled.
type Config struct {
	// BaseUploads is the number of successful uploads that need to
	// occur before we start considering whether remaining uploads have stalled.
	BaseUploads int

	// Factor determines how much longer than the fastest uploads the remaining
	// uploads are allowed to take. Once BaseUploads successful uploads have occurred,
	// remaining uploads will be considered stalled if they take longer than the
	// BaseUploads'th duration times Factor.
	Factor int

	// MinStallDuration is the minimum amount of time that can be considered a stall.
	MinStallDuration time.Duration

	// DynamicBaseUploads if set to true, will use a dynamically calculated BaseUploads value
	// based on the erasure coding parameters instead of the configured BaseUploads value.
	// The original BaseUploads value will be ignored.
	DynamicBaseUploads bool
}

// DefaultConfig provides default values for stall detection.
var DefaultConfig = Config{
	BaseUploads:      3,
	Factor:           2,
	MinStallDuration: 10 * time.Second,
}

// Setup updates the stall detection config values to their finals.
// Uses defaults when out of range or unassigned.
func (c *Config) Setup(totalNodes int) {
	if c.Factor < 1 {
		c.Factor = DefaultConfig.Factor
	}
	if c.BaseUploads < 1 {
		c.BaseUploads = DefaultConfig.BaseUploads
	}
	if c.DynamicBaseUploads && totalNodes > 0 {
		c.BaseUploads = totalNodes / 2
	}

	if c.MinStallDuration <= 0 {
		c.MinStallDuration = DefaultConfig.MinStallDuration
	}

	evs.Event("stall-detection-config-setup",
		eventkit.Int64("base_uploads", int64(c.BaseUploads)),
		eventkit.Int64("factor", int64(c.Factor)),
		eventkit.Duration("min_stall_duration", c.MinStallDuration),
		eventkit.Bool("dynamic_base_uploads", c.DynamicBaseUploads),
		eventkit.Int64("total_nodes", int64(totalNodes)),
	)
}
