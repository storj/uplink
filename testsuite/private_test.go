// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink/private/storage/streams"
)

func TestConcurrentUploadToSamePath(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: testplanet.MaxSegmentSize(20 * memory.KiB),
		},
	}, func(t *testing.T, tctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		uplink := planet.Uplinks[0]

		project := openProject(t, tctx, planet)
		defer tctx.Check(project.Close)

		createBucket(t, tctx, project, "testbucket")

		expectedData := testrand.Bytes(50 * memory.KiB)
		N := len(expectedData)

		ctx := streams.DisableDeleteOnCancel(tctx)

		upload1, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)

		upload2, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)

		upload3, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)

		_, err = upload1.Write(expectedData[:N/2])
		require.NoError(t, err)
		_, err = upload2.Write(expectedData[:N/2])
		require.NoError(t, err)
		_, err = upload3.Write(expectedData[:N/2])
		require.NoError(t, err)

		_, err = upload1.Write(expectedData[N/2:])
		require.NoError(t, err)
		_, err = upload2.Write(expectedData[N/2:])
		require.NoError(t, err)
		_, err = upload3.Write(expectedData[N/2:])
		require.NoError(t, err)

		err = upload1.Commit()
		require.NoError(t, err)

		downloaded, err := uplink.Download(ctx, satellite, "testbucket", "test.dat")
		require.NoError(t, err)
		require.Equal(t, expectedData, downloaded)

		err = upload2.Abort()
		require.NoError(t, err)

		downloaded, err = uplink.Download(ctx, satellite, "testbucket", "test.dat")
		require.NoError(t, err)
		require.Equal(t, expectedData, downloaded)

		err = upload3.Commit()
		require.NoError(t, err)

		downloaded, err = uplink.Download(ctx, satellite, "testbucket", "test.dat")
		require.NoError(t, err)
		require.Equal(t, expectedData, downloaded)
	})
}
