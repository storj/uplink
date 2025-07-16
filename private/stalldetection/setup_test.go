// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package stalldetection

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigFromEnv(t *testing.T) {
	t.Run("stall detection disabled", func(t *testing.T) {
		config := ConfigFromEnv()
		require.Nil(t, config)
	})

	t.Run("enabled with default values", func(t *testing.T) {
		t.Setenv("STORJ_STALL_DETECTION_ENABLED", "true")

		config := ConfigFromEnv()
		require.NotNil(t, config)
		assert.Equal(t, defaultConfig.BaseUploads, config.BaseUploads)
		assert.Equal(t, defaultConfig.Factor, config.Factor)
		assert.Equal(t, defaultConfig.MinStallDuration, config.MinStallDuration)
		assert.False(t, config.DynamicBaseUploads)
	})

	t.Run("enabled with custom values", func(t *testing.T) {
		t.Setenv("STORJ_STALL_DETECTION_ENABLED", "true")
		t.Setenv("STORJ_STALL_DETECTION_BASE_UPLOADS", "5")
		t.Setenv("STORJ_STALL_DETECTION_FACTOR", "3")
		t.Setenv("STORJ_STALL_DETECTION_MIN_DURATION", "30s")
		t.Setenv("STORJ_STALL_DETECTION_DYNAMIC_BASE", "true")

		config := ConfigFromEnv()
		require.NotNil(t, config)
		assert.Equal(t, 5, config.BaseUploads)
		assert.Equal(t, 3, config.Factor)
		assert.Equal(t, 30*time.Second, config.MinStallDuration)
		assert.True(t, config.DynamicBaseUploads)
	})

	t.Run("enabled with invalid env values", func(t *testing.T) {
		t.Setenv("STORJ_STALL_DETECTION_ENABLED", "true")
		t.Setenv("STORJ_STALL_DETECTION_BASE_UPLOADS", "invalid")
		t.Setenv("STORJ_STALL_DETECTION_FACTOR", "not-a-number")
		t.Setenv("STORJ_STALL_DETECTION_MIN_DURATION", "invalid-duration")
		t.Setenv("STORJ_STALL_DETECTION_DYNAMIC_BASE", "not-true")

		config := ConfigFromEnv()
		require.NotNil(t, config)
		assert.Equal(t, defaultConfig.BaseUploads, config.BaseUploads)
		assert.Equal(t, defaultConfig.Factor, config.Factor)
		assert.Equal(t, defaultConfig.MinStallDuration, config.MinStallDuration)
		assert.False(t, config.DynamicBaseUploads)
	})

	t.Run("case insensitive enabled flag", func(t *testing.T) {
		testCases := []string{"TRUE", "True", "true", "tRuE"}

		for _, enabled := range testCases {
			t.Run("enabled="+enabled, func(t *testing.T) {
				t.Setenv("STORJ_STALL_DETECTION_ENABLED", enabled)
				config := ConfigFromEnv()
				assert.NotNil(t, config)
			})
		}
	})
}

func TestValidateAndUpdate(t *testing.T) {
	t.Run("all valid values", func(t *testing.T) {
		original := &Config{
			BaseUploads:        5,
			Factor:             3,
			MinStallDuration:   15 * time.Second,
			DynamicBaseUploads: false,
		}

		result := original.ValidateAndUpdate(15)

		assert.Equal(t, 5, result.BaseUploads)
		assert.Equal(t, 3, result.Factor)
		assert.Equal(t, 15*time.Second, result.MinStallDuration)
		assert.False(t, result.DynamicBaseUploads)

		// Ensure original is not modified
		assert.Equal(t, 5, original.BaseUploads)
	})

	t.Run("dynamic base uploads enabled", func(t *testing.T) {
		original := &Config{
			BaseUploads:        5,
			Factor:             3,
			MinStallDuration:   15 * time.Second,
			DynamicBaseUploads: true,
		}

		result := original.ValidateAndUpdate(15)

		assert.Equal(t, 7, result.BaseUploads) // int(totalNodes/2) = int(15/2) = 7
		assert.Equal(t, 3, result.Factor)
		assert.Equal(t, 15*time.Second, result.MinStallDuration)
		assert.True(t, result.DynamicBaseUploads)
	})

	t.Run("dynamic base uploads with single node", func(t *testing.T) {
		original := &Config{
			BaseUploads:        5,
			Factor:             3,
			MinStallDuration:   15 * time.Second,
			DynamicBaseUploads: true,
		}

		result := original.ValidateAndUpdate(1)

		assert.Equal(t, 5, result.BaseUploads) // Should not change since totalNodes <= 1
		assert.Equal(t, 3, result.Factor)
		assert.Equal(t, 15*time.Second, result.MinStallDuration)
		assert.True(t, result.DynamicBaseUploads)
	})

	t.Run("invalid factor", func(t *testing.T) {
		original := &Config{
			BaseUploads:        5,
			Factor:             0,
			MinStallDuration:   15 * time.Second,
			DynamicBaseUploads: false,
		}

		result := original.ValidateAndUpdate(15)

		assert.Equal(t, 5, result.BaseUploads)
		assert.Equal(t, defaultConfig.Factor, result.Factor)
		assert.Equal(t, 15*time.Second, result.MinStallDuration)
		assert.False(t, result.DynamicBaseUploads)
	})

	t.Run("invalid base uploads", func(t *testing.T) {
		original := &Config{
			BaseUploads:        0,
			Factor:             3,
			MinStallDuration:   15 * time.Second,
			DynamicBaseUploads: false,
		}

		result := original.ValidateAndUpdate(15)

		assert.Equal(t, defaultConfig.BaseUploads, result.BaseUploads)
		assert.Equal(t, 3, result.Factor)
		assert.Equal(t, 15*time.Second, result.MinStallDuration)
		assert.False(t, result.DynamicBaseUploads)
	})

	t.Run("invalid min stall duration", func(t *testing.T) {
		original := &Config{
			BaseUploads:        5,
			Factor:             3,
			MinStallDuration:   0,
			DynamicBaseUploads: false,
		}

		result := original.ValidateAndUpdate(15)

		assert.Equal(t, 5, result.BaseUploads)
		assert.Equal(t, 3, result.Factor)
		assert.Equal(t, defaultConfig.MinStallDuration, result.MinStallDuration)
		assert.False(t, result.DynamicBaseUploads)
	})

	t.Run("multiple invalid values", func(t *testing.T) {
		original := &Config{
			BaseUploads:        -1,
			Factor:             -5,
			MinStallDuration:   -10 * time.Second,
			DynamicBaseUploads: true,
		}

		result := original.ValidateAndUpdate(20)

		assert.Equal(t, 10, result.BaseUploads) // totalNodes/2 = 20/2 = 10
		assert.Equal(t, defaultConfig.Factor, result.Factor)
		assert.Equal(t, defaultConfig.MinStallDuration, result.MinStallDuration)
		assert.True(t, result.DynamicBaseUploads)
	})
}
