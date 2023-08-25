// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package bucket_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/buckets"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
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

func TestGetBucketLocation(t *testing.T) {
	placementRules := overlay.NewPlacementRules()
	err := placementRules.AddPlacementFromString(fmt.Sprintf(`40:annotated(annotated(country("PL"),annotation("%s","Poland")),annotation("%s","%s"))`,
		nodeselection.Location, nodeselection.AutoExcludeSubnet, nodeselection.AutoExcludeSubnetOFF))
	require.NoError(t, err)

	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Placement = *placementRules
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// no bucket name
		_, err = bucket.GetBucketLocation(ctx, project, "")
		require.ErrorIs(t, err, uplink.ErrBucketNameInvalid)

		// bucket not exists
		_, err = bucket.GetBucketLocation(ctx, project, "test-bucket")
		require.ErrorIs(t, err, uplink.ErrBucketNotFound)

		err = planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], "test-bucket")
		require.NoError(t, err)

		// bucket without location
		location, err := bucket.GetBucketLocation(ctx, project, "test-bucket")
		require.NoError(t, err)
		require.Empty(t, location)

		_, err = planet.Satellites[0].DB.Buckets().UpdateBucket(ctx, buckets.Bucket{
			ProjectID: planet.Uplinks[0].Projects[0].ID,
			Name:      "test-bucket",
			Placement: storj.PlacementConstraint(40),
		})
		require.NoError(t, err)

		// bucket with location
		location, err = bucket.GetBucketLocation(ctx, project, "test-bucket")
		require.NoError(t, err)
		require.Equal(t, "Poland", location)
	})
}
