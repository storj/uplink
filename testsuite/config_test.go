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
		project := openProject(t, ctx, planet)
		ctx.Check(project.Close)

		t.Run("satellite url without id", func(t *testing.T) {
			// try connecting without a proper satellite url
			satellite := planet.Satellites[0]
			satelliteurl := satellite.URL()
			apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
			satelliteurl.ID = storj.NodeID{}
			_, err := uplink.RequestAccessWithPassphrase(ctx, satelliteurl.String(), apiKey.Serialize(), "mypassphrase")
			require.Error(t, err)
		})
	})
}

func openProject(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) *uplink.Project {
	satellite := planet.Satellites[0]
	apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
	uplinkConfig := uplink.Config{}
	access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, satellite.URL().String(), apiKey.Serialize(), "mypassphrase")
	require.NoError(t, err)

	project, err := uplinkConfig.Open(ctx, access)
	require.NoError(t, err)

	return project
}
