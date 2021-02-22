// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite/metainfo/metabase"
)

func TestMultisegmentUploadWithLastInline(t *testing.T) {
	// this is special case were uploaded object has 3 segments (2 remote + 1 inline)
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: testplanet.MaxSegmentSize(20 * memory.KiB),
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		uplink := planet.Uplinks[0]

		expectedData := testrand.Bytes(40 * memory.KiB)

		err := planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "testbucket", "test/path", expectedData)
		require.NoError(t, err)

		downloaded, err := uplink.Download(ctx, satellite, "testbucket", "test/path")
		require.NoError(t, err)
		require.Equal(t, expectedData, downloaded)

		// verify that object has 3 segments, 2 remote + 1 inline
		objects, err := planet.Satellites[0].Metainfo.Metabase.TestingAllCommittedObjects(ctx, planet.Uplinks[0].Projects[0].ID, "testbucket")
		require.NoError(t, err)
		require.Len(t, objects, 1)

		segments, err := planet.Satellites[0].Metainfo.Metabase.TestingAllObjectSegments(ctx, metabase.ObjectLocation{
			ProjectID:  planet.Uplinks[0].Projects[0].ID,
			BucketName: "testbucket",
			ObjectKey:  objects[0].ObjectKey,
		})
		require.NoError(t, err)
		require.Equal(t, 3, len(segments))
		// TODO we should check 2 segments to be remote and last one to be inline
		// but main satellite implementation doens't give such info at the moment
	})
}
