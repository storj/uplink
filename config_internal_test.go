// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSettingMaximumBufferSize(t *testing.T) {
	var config Config
	config_setMaximumBufferSize(&config, 1)

	dialer, err := config.getDialerForPool(t.Context(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, dialer.Pool.Close()) }()

	require.Equal(t, 1, dialer.ConnectionOptions.Manager.Stream.MaximumBufferSize)
}
