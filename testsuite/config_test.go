// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func TestRequestAccess(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		satelliteNodeURL := storj.NodeURL{ID: satellite.ID(), Address: satellite.Addr()}.String()
		apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
		uplinkConfig := uplink.Config{
			Whitelist: uplink.InsecureSkipConnectionVerify(),
		}
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, satelliteNodeURL, apiKey.Serialize(), "mypassphrase")
		require.NoError(t, err)

		project, err := uplinkConfig.Open(ctx, access)
		require.NoError(t, err)

		err = project.Close()
		require.NoError(t, err)
	})
}
