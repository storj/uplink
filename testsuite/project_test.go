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

		project, err := uplink.OpenProject(ctx, access)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = project.EnsureBucket(ctx, "bucket")
		require.Error(t, err)
	})
}

func TestProject_OpenProjectMalformedUserAgent(t *testing.T) {
	config := uplink.Config{
		UserAgent: "Invalid (darwin; amd64) invalid/v7.0.6 version/2020-11-17T00:39:14Z",
	}

	ctx := testcontext.New(t)
	defer ctx.Cleanup()
	_, err := config.OpenProject(ctx, &uplink.Access{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "user agent")
}
