// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package expose_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
)

func TestExposed(t *testing.T) {
	config := uplink.Config{}
	expose.ConfigSetConnectionPool(&config, nil)
	dialer, _ := expose.ConfigGetDialer(config, context.Background())
	require.NotNil(t, dialer)

	access, err := uplink.ParseAccess("12edqwjdy4fmoHasYrxLzmu8Ubv8Hsateq1LPYne6Jzd64qCsYgET53eJzhB4L2pWDKBpqMowxt8vqLCbYxu8Qz7BJVH1CvvptRt9omm24k5GAq1R99mgGjtmc6yFLqdEFgdevuQwH5yzXCEEtbuBYYgES8Stb1TnuSiU3sa62bd2G88RRgbTCtwYrB8HZ7CLjYWiWUphw7RNa3NfD1TW6aUJ6E5D1F9AM6sP58X3D4H7tokohs2rqCkwRT")
	require.NoError(t, err)

	require.NotNil(t, expose.AccessGetAPIKey(access))
	require.NotNil(t, expose.AccessGetEncAccess(access))
}
