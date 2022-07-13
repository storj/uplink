// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package bucket_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
	"storj.io/uplink/private/bucket"
)

func TestListBucketsWithAttribution(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		access := planet.Uplinks[0].Access[planet.Satellites[0].ID()]

		// map bucket -> user agent
		testCases := map[string]string{
			"aaa": "foo",
			"bbb": "",
			"ccc": "boo",
			"ddd": "foo",
		}

		for bucket, userAgent := range testCases {
			func() {
				config := uplink.Config{
					UserAgent: userAgent,
				}

				project, err := config.OpenProject(ctx, access)
				require.NoError(t, err)
				defer ctx.Check(project.Close)

				_, err = project.CreateBucket(ctx, bucket)
				require.NoError(t, err)
			}()
		}

		project, err := uplink.OpenProject(ctx, access)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		iterator := bucket.ListBucketsWithAttribution(ctx, project, nil)
		for iterator.Next() {
			item := iterator.Item()
			userAgent, ok := testCases[item.Name]
			require.True(t, ok)
			require.Equal(t, userAgent, item.Attribution)

			delete(testCases, item.Name)
		}
		require.NoError(t, iterator.Err())
		require.Empty(t, testCases)
	})
}
