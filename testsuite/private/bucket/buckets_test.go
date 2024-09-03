// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package bucket_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/macaroon"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/buckets"
	"storj.io/storj/satellite/console"
	"storj.io/storj/satellite/nodeselection"
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
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Placement = nodeselection.ConfigurablePlacementRule{
					PlacementRules: fmt.Sprintf(`40:annotated(annotated(country("PL"),annotation("%s","Poland")),annotation("%s","%s"))`,
						nodeselection.Location, nodeselection.AutoExcludeSubnet, nodeselection.AutoExcludeSubnetOFF),
				}
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

func TestSetBucketVersioning(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		projectID := planet.Uplinks[0].Projects[0].ID
		satellite := planet.Satellites[0]
		enable := true
		suspend := false

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		for _, tt := range []struct {
			name                     string
			bucketName               string
			initialVersioningState   buckets.Versioning
			versioning               bool
			resultantVersioningState buckets.Versioning
		}{
			{"Enable unsupported bucket fails", "bucket1", buckets.VersioningUnsupported, enable, buckets.VersioningUnsupported},
			{"Suspend unsupported bucket fails", "bucket2", buckets.VersioningUnsupported, suspend, buckets.VersioningUnsupported},
			{"Enable unversioned bucket succeeds", "bucket3", buckets.Unversioned, enable, buckets.VersioningEnabled},
			{"Suspend unversioned bucket fails", "bucket4", buckets.Unversioned, suspend, buckets.Unversioned},
			{"Enable enabled bucket succeeds", "bucket5", buckets.VersioningEnabled, enable, buckets.VersioningEnabled},
			{"Suspend enabled bucket succeeds", "bucket6", buckets.VersioningEnabled, suspend, buckets.VersioningSuspended},
			{"Enable suspended bucket succeeds", "bucket7", buckets.VersioningSuspended, enable, buckets.VersioningEnabled},
			{"Suspend suspended bucket succeeds", "bucket8", buckets.VersioningSuspended, suspend, buckets.VersioningSuspended},
		} {
			t.Run(tt.name, func(t *testing.T) {
				testBucket, err := satellite.API.DB.Buckets().CreateBucket(ctx, buckets.Bucket{
					ProjectID:  projectID,
					Name:       tt.bucketName,
					Versioning: tt.initialVersioningState,
				})
				require.NoError(t, err)
				require.NotNil(t, testBucket)
				err = bucket.SetBucketVersioning(ctx, project, testBucket.Name, tt.versioning)
				// only 3 error state transitions
				if tt.initialVersioningState == buckets.VersioningUnsupported ||
					(tt.initialVersioningState == buckets.Unversioned && tt.versioning == suspend) {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				versioningState, err := bucket.GetBucketVersioning(ctx, project, testBucket.Name)
				require.NoError(t, err)
				require.Equal(t, tt.resultantVersioningState, buckets.Versioning(versioningState))
			})
		}
	})
}

func TestCreateBucketWithObjectLock(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.ObjectLockEnabled = true
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		upl := planet.Uplinks[0]
		projectID := upl.Projects[0].ID
		userID := upl.Projects[0].Owner.ID

		userCtx, err := sat.UserContext(ctx, userID)
		require.NoError(t, err)

		_, key, err := sat.API.Console.Service.CreateAPIKey(userCtx, projectID, "test key", macaroon.APIKeyVersionMin)
		require.NoError(t, err)

		access, err := uplink.RequestAccessWithPassphrase(ctx, sat.URL(), key.Serialize(), "")
		require.NoError(t, err)

		upl.Access[sat.ID()] = access

		project, err := upl.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// permission denied for older API key version
		_, err = bucket.GetBucketObjectLockConfiguration(ctx, project, "test-bucket")
		require.ErrorIs(t, err, uplink.ErrPermissionDenied)

		// permission denied for older API key version
		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              "some-bucket",
			ObjectLockEnabled: true,
		})
		require.ErrorIs(t, err, uplink.ErrPermissionDenied)

		err = project.Close()
		require.NoError(t, err)

		_, key, err = sat.API.Console.Service.CreateAPIKey(userCtx, projectID, "test key2", macaroon.APIKeyVersionObjectLock)
		require.NoError(t, err)

		access, err = uplink.RequestAccessWithPassphrase(ctx, sat.URL(), key.Serialize(), "")
		require.NoError(t, err)

		upl.Access[sat.ID()] = access

		err = sat.API.DB.Console().Projects().UpdateDefaultVersioning(ctx, projectID, console.DefaultVersioning(buckets.VersioningEnabled))
		require.NoError(t, err)

		project, err = upl.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// bucket not exists
		_, err = bucket.GetBucketObjectLockConfiguration(ctx, project, "test-bucket")
		require.ErrorIs(t, err, uplink.ErrBucketNotFound)

		// no bucket name
		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              "",
			ObjectLockEnabled: false,
		})
		require.ErrorIs(t, err, uplink.ErrBucketNameInvalid)

		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              "test-bucket",
			ObjectLockEnabled: false,
		})
		require.NoError(t, err)

		_, err = bucket.GetBucketObjectLockConfiguration(ctx, project, "test-bucket")
		require.ErrorIs(t, err, bucket.ErrBucketNoLock)

		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              "test-bucket",
			ObjectLockEnabled: true,
		})
		require.ErrorIs(t, err, uplink.ErrBucketAlreadyExists)

		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              "test-bucket2",
			ObjectLockEnabled: true,
		})
		require.NoError(t, err)

		enabled, err := bucket.GetBucketObjectLockConfiguration(ctx, project, "test-bucket2")
		require.NoError(t, err)
		require.True(t, enabled)
	})
}
