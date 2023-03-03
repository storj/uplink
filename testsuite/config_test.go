// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
	"storj.io/uplink/private/transport"
)

func TestRequestAccess(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		t.Run("satellite url without id", func(t *testing.T) {
			// try connecting without a proper satellite url
			projectInfo := planet.Uplinks[0].Projects[0]
			_, err := uplink.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.Addr(), projectInfo.APIKey, "mypassphrase")
			require.Error(t, err)
		})
	})
}

func openProject(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) *uplink.Project {
	project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
	require.NoError(t, err)

	return project
}

func TestUserAgent(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 1,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		config := uplink.Config{
			UserAgent: "Zenko",
		}

		satellite, uplink := planet.Satellites[0], planet.Uplinks[0]
		apiKey := uplink.Projects[0].APIKey

		access, err := config.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "mypassphrase")
		require.NoError(t, err)

		project, err := config.OpenProject(ctx, access)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = project.EnsureBucket(ctx, "bucket")
		require.NoError(t, err)

		upload, err := project.UploadObject(ctx, "bucket", "alpha", nil)
		require.NoError(t, err)

		_, err = upload.Write(testrand.Bytes(5 * memory.KiB))
		require.NoError(t, err)
		require.NoError(t, upload.Commit())

		bucketInfo, err := satellite.DB.Buckets().GetBucket(ctx, []byte("bucket"), uplink.Projects[0].ID)
		require.NoError(t, err)
		assert.Contains(t, string(bucketInfo.UserAgent), "Zenko")

		attribution, err := satellite.DB.Attribution().Get(ctx, uplink.Projects[0].ID, []byte("bucket"))
		require.NoError(t, err)
		assert.Contains(t, string(attribution.UserAgent), "Zenko")
	})
}

func TestCustomDialContext(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		{
			config := uplink.Config{
				DialContext: badDialContext,
			}

			project, err := config.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			_, err = project.EnsureBucket(ctx, "bucket")
			require.Error(t, err)
		}

		{
			config := uplink.Config{
				DialContext: (new(net.Dialer)).DialContext,
			}

			project, err := config.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			_, err = project.EnsureBucket(ctx, "bucket")
			require.NoError(t, err)
		}
	})
}

func badDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return nil, errors.New("dial error")
}

func TestSettingMaximumBufferSize(t *testing.T) {
	var config uplink.Config

	transport.SetMaximumBufferSize(&config, 1)

	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	d, err := expose.ConfigGetDialer(config, ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, d.ConnectionOptions.Manager.Stream.MaximumBufferSize)
}
