// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
)

func TestMultisegmentUploadWithLastInline(t *testing.T) {
	// this is special case were uploaded object has 3 segments (2 remote + 1 inline)
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		uplink := planet.Uplinks[0]

		data := testrand.Bytes(50 * memory.KiB)
		config := uplink.GetConfig(satellite)
		config.Client.SegmentSize = 10 * memory.KiB
		err := uplink.UploadWithClientConfig(ctx, satellite, config, "testbucket", "test/path", data)
		require.NoError(t, err)

		downloaded, err := uplink.Download(ctx, satellite, "testbucket", "test/path")
		require.NoError(t, err)
		require.True(t, bytes.Equal(data, downloaded), "upload != download data")
	})
}
