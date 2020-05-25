// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testuplink_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/storage"
	"storj.io/uplink/private/testuplink"
)

func TestWithMaxSegmentSize(t *testing.T) {
	ctx := context.Background()
	_, ok := testuplink.GetMaxSegmentSize(ctx)
	require.False(t, ok)

	newCtx := testuplink.WithMaxSegmentSize(ctx, memory.KiB)
	segmentSize, ok := testuplink.GetMaxSegmentSize(newCtx)
	require.True(t, ok)
	require.EqualValues(t, memory.KiB, segmentSize)

}

func TestWithMaxSegmentSize_Upload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		newCtx := testuplink.WithMaxSegmentSize(ctx, 10*memory.KiB)

		expectedData := testrand.Bytes(19 * memory.KiB)
		err := planet.Uplinks[0].Upload(newCtx, planet.Satellites[0], "super-bucket", "super-object", expectedData)
		require.NoError(t, err)

		data, err := planet.Uplinks[0].Download(newCtx, planet.Satellites[0], "super-bucket", "super-object")
		require.NoError(t, err)
		require.Equal(t, expectedData, data)

		// verify we have two segments instead of one
		keys, err := planet.Satellites[0].Metainfo.Database.List(ctx, storage.Key{}, -1)
		require.NoError(t, err)
		require.Equal(t, 2, len(keys.Strings()))
	})
}
