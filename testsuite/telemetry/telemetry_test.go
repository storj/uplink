// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package telemetry_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	commontelemetry "storj.io/common/telemetry"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
	"storj.io/uplink/telemetry"
)

func TestWithServer(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		// create a telemetry server
		server, err := commontelemetry.Listen("127.0.0.1:0")
		require.NoError(t, err)
		defer ctx.Check(server.Close)

		// create acccess
		projectInfo := planet.Uplinks[0].Projects[0]
		access, err := uplink.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		// open project with telemetry enabled.
		tctx, cancel := telemetry.EnableWith(ctx, &telemetry.Options{
			Endpoint:    server.Addr(),
			Application: "test",
		})
		defer cancel()

		project, err := uplink.OpenProject(tctx, access)
		require.NoError(t, err)

		defer ctx.Check(project.Close)
	})
}
