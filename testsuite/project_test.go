// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func TestProject_OpenProjectDialFail(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		projectInfo := planet.Uplinks[0].Projects[0]
		access, err := uplink.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		err = planet.StopPeer(planet.Satellites[0])
		require.NoError(t, err)

		_, err = uplink.OpenProject(ctx, access)
		require.Error(t, err)
	})
}
