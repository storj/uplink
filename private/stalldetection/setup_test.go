// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package stalldetection

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSetup(t *testing.T) {
	tests := []struct {
		name            string
		total           int
		initialConfig   *Config
		resultingConfig *Config
	}{
		{
			name:            "disabled",
			total:           10,
			initialConfig:   nil,
			resultingConfig: nil,
		},
		{
			name:          "default config",
			total:         0,
			initialConfig: &Config{},
			resultingConfig: &Config{
				BaseUploads:        DefaultConfig.BaseUploads,
				Factor:             DefaultConfig.Factor,
				MinStallDuration:   DefaultConfig.MinStallDuration,
				DynamicBaseUploads: DefaultConfig.DynamicBaseUploads},
		},
		{
			name:          "default config, dynamic min upload enabled",
			total:         9,
			initialConfig: &Config{DynamicBaseUploads: true},
			resultingConfig: &Config{
				BaseUploads:        4,
				Factor:             DefaultConfig.Factor,
				MinStallDuration:   DefaultConfig.MinStallDuration,
				DynamicBaseUploads: true},
		},
		{
			name:            "custom config, dynamic min upload disabled",
			total:           9,
			initialConfig:   &Config{BaseUploads: 5, Factor: 3, MinStallDuration: 15 * time.Millisecond, DynamicBaseUploads: false},
			resultingConfig: &Config{BaseUploads: 5, Factor: 3, MinStallDuration: 15 * time.Millisecond, DynamicBaseUploads: false},
		},
		{
			name:            "custom config, dynamic min upload enabled",
			total:           9,
			initialConfig:   &Config{BaseUploads: 5, Factor: 3, MinStallDuration: 15 * time.Millisecond, DynamicBaseUploads: true},
			resultingConfig: &Config{BaseUploads: 4, Factor: 3, MinStallDuration: 15 * time.Millisecond, DynamicBaseUploads: true},
		},
		{
			name:            "custom config, dynamic min upload enabled, total nodes 0",
			total:           0,
			initialConfig:   &Config{BaseUploads: 5, Factor: 3, MinStallDuration: 15 * time.Millisecond, DynamicBaseUploads: true},
			resultingConfig: &Config{BaseUploads: 5, Factor: 3, MinStallDuration: 15 * time.Millisecond, DynamicBaseUploads: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.initialConfig != nil {
				tt.initialConfig.Setup(tt.total)
			}
			assert.EqualExportedValues(t, tt.initialConfig, tt.resultingConfig)
		})
	}
}
