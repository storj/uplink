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
			projectInfo := planet.Uplinks[0].Projects[0]
			_, err := uplink.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.Addr(), projectInfo.APIKey, "mypassphrase")
			require.Error(t, err)
		})
	})
}

func openProject(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) *uplink.Project {
	projectInfo := planet.Uplinks[0].Projects[0]

	uplinkConfig := uplink.Config{}
	access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
	require.NoError(t, err)

	project, err := uplinkConfig.OpenProject(ctx, access)
	require.NoError(t, err)

	return project
}
