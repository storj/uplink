// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package object_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink/private/object"
	"storj.io/uplink/private/testuplink"
)

func TestDownloadObjectAt(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		testCases := []struct {
			Name       string
			ObjectSize memory.Size
		}{
			{"empty", 0},
			{"inline", memory.KiB},
			{"remote ", 90 * memory.KiB},
			{"remote + empty inline", 100 * memory.KiB},
			{"remote + inline", 101 * memory.KiB},
			{"two remotes", 190 * memory.KiB},
			{"multiple remotes", 1090 * memory.KiB},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.Name, func(t *testing.T) {
				newCtx := testuplink.WithMaxSegmentSize(ctx, 100*memory.KiB)
				expected := testrand.Bytes(tc.ObjectSize)
				err = planet.Uplinks[0].Upload(newCtx, planet.Satellites[0], "foo", tc.Name, expected)
				require.NoError(t, err)

				output := ctx.File(tc.Name)
				file, err := os.Create(output)
				require.NoError(t, err)
				defer ctx.Check(file.Close)

				err = object.DownloadObjectAt(ctx, project, "foo", tc.Name, file, nil)
				require.NoError(t, err)

				data, err := ioutil.ReadAll(file)
				require.NoError(t, err)
				require.Equal(t, len(expected), len(data))
				require.Equal(t, expected, data)
			})
		}
	})
}

// TODO add test for object with more segments than satellite page size
