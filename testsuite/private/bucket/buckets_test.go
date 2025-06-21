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
	privateBucket "storj.io/uplink/private/bucket"
	"storj.io/uplink/private/metaclient"
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

		iterator := privateBucket.ListBucketsWithAttribution(ctx, project, nil)
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
		_, err = privateBucket.GetBucketLocation(ctx, project, "")
		require.ErrorIs(t, err, uplink.ErrBucketNameInvalid)

		// bucket not exists
		_, err = privateBucket.GetBucketLocation(ctx, project, "test-bucket")
		require.ErrorIs(t, err, uplink.ErrBucketNotFound)

		err = planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], "test-bucket")
		require.NoError(t, err)

		// bucket without location
		location, err := privateBucket.GetBucketLocation(ctx, project, "test-bucket")
		require.NoError(t, err)
		require.Empty(t, location)

		_, err = planet.Satellites[0].DB.Buckets().UpdateBucket(ctx, buckets.Bucket{
			ProjectID: planet.Uplinks[0].Projects[0].ID,
			Name:      "test-bucket",
			Placement: storj.PlacementConstraint(40),
		})
		require.NoError(t, err)

		// bucket with location
		location, err = privateBucket.GetBucketLocation(ctx, project, "test-bucket")
		require.NoError(t, err)
		require.Equal(t, "Poland", location)
	})
}

func TestCreateBucketWithLocation(t *testing.T) {
	var (
		placement       = storj.PlacementConstraint(40)
		placementDetail = console.PlacementDetail{
			ID:     40,
			IdName: "Poland",
		}
	)
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Placement = nodeselection.ConfigurablePlacementRule{
					PlacementRules: "buckets_test_placement.yaml",
				}
				config.Console.Placement.SelfServeDetails.SetMap(map[storj.PlacementConstraint]console.PlacementDetail{
					0:         {ID: 0},
					placement: placementDetail,
				})
				config.Console.Placement.SelfServeEnabled = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		projectID := planet.Uplinks[0].Projects[0].ID
		bucketName := "bucket"

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// change the default_placement of the project
		err = planet.Satellites[0].API.DB.Console().Projects().UpdateDefaultPlacement(ctx, projectID, storj.EU)
		require.NoError(t, err)

		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
			Name:      bucketName,
			Placement: "Poland",
		})
		// cannot create bucket with custom placement if there is project default.
		require.True(t, metaclient.ErrConflictingPlacement.Has(err))

		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
			Name: bucketName,
		})
		require.NoError(t, err)

		// check if placement is set to project default
		placement, err := planet.Satellites[0].API.DB.Buckets().GetBucketPlacement(ctx, []byte(bucketName), projectID)
		require.NoError(t, err)
		require.Equal(t, storj.EU, placement)

		// delete the bucket
		err = planet.Satellites[0].API.DB.Buckets().DeleteBucket(ctx, []byte(bucketName), projectID)
		require.NoError(t, err)

		// change the default_placement of the project
		err = planet.Satellites[0].API.DB.Console().Projects().UpdateDefaultPlacement(ctx, projectID, storj.DefaultPlacement)
		require.NoError(t, err)

		// We can't recreate a bucket if the deleted one had a custom placement set,
		// i.e., it was attributed to a specific placement.
		// This is a new caveat related to billing for the self-serve placement feature.
		bucketName1 := "bucket1"

		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
			Name:      bucketName1,
			Placement: "Poland",
		})
		require.NoError(t, err)

		placement, err = planet.Satellites[0].API.DB.Buckets().GetBucketPlacement(ctx, []byte(bucketName1), projectID)
		require.NoError(t, err)
		require.Equal(t, storj.PlacementConstraint(40), placement)

		// delete the bucket
		err = planet.Satellites[0].API.DB.Buckets().DeleteBucket(ctx, []byte(bucketName1), projectID)
		require.NoError(t, err)

		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
			Name:      bucketName1,
			Placement: "EU", // invalid placement
		})
		require.True(t, metaclient.ErrInvalidPlacement.Has(err))

		// disable self-serve placement
		planet.Satellites[0].API.Metainfo.Endpoint.TestSelfServePlacementEnabled(false)

		bucketName2 := "bucket2"

		// passing invalid placement should not fail if self-serve placement is disabled.
		// This is for backward compatibility with integration tests that'll pass placements
		// regardless of self-serve placement being enabled or not.
		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
			Name:      bucketName2,
			Placement: "EU", // invalid placement
		})
		require.NoError(t, err)

		// placement should be set to default event though a placement was passed
		// because self-serve placement is disabled.
		placement, err = planet.Satellites[0].API.DB.Buckets().GetBucketPlacement(ctx, []byte(bucketName2), projectID)
		require.NoError(t, err)
		require.Equal(t, storj.DefaultPlacement, placement)
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
				err = privateBucket.SetBucketVersioning(ctx, project, testBucket.Name, tt.versioning)
				// only 3 error state transitions
				if tt.initialVersioningState == buckets.VersioningUnsupported ||
					(tt.initialVersioningState == buckets.Unversioned && tt.versioning == suspend) {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				versioningState, err := privateBucket.GetBucketVersioning(ctx, project, testBucket.Name)
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
		_, err = privateBucket.GetBucketObjectLockConfiguration(ctx, project, "test-bucket")
		require.ErrorIs(t, err, uplink.ErrPermissionDenied)

		// permission denied for older API key version
		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
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
		_, err = privateBucket.GetBucketObjectLockConfiguration(ctx, project, "test-bucket")
		require.ErrorIs(t, err, uplink.ErrBucketNotFound)

		// no bucket name
		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
			Name:              "",
			ObjectLockEnabled: false,
		})
		require.ErrorIs(t, err, uplink.ErrBucketNameInvalid)

		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
			Name:              "test-bucket",
			ObjectLockEnabled: false,
		})
		require.NoError(t, err)

		_, err = privateBucket.GetBucketObjectLockConfiguration(ctx, project, "test-bucket")
		require.ErrorIs(t, err, privateBucket.ErrBucketNoLock)

		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
			Name:              "test-bucket",
			ObjectLockEnabled: true,
		})
		require.ErrorIs(t, err, uplink.ErrBucketAlreadyExists)

		// force deleting bucket without object lock enabled should be allowed
		_, err = project.DeleteBucketWithObjects(ctx, "test-bucket")
		require.NoError(t, err)

		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
			Name:              "test-bucket2",
			ObjectLockEnabled: true,
		})
		require.NoError(t, err)

		configuration, err := privateBucket.GetBucketObjectLockConfiguration(ctx, project, "test-bucket2")
		require.NoError(t, err)
		require.True(t, configuration.Enabled)
		require.Nil(t, configuration.DefaultRetention)

		// force deleting bucket with object lock enabled should not be allowed
		_, err = project.DeleteBucketWithObjects(ctx, "test-bucket2")
		require.Error(t, err)
	})
}

func TestSetBucketObjectLockConfig(t *testing.T) {
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

		bucketName := "test-bucket"

		// permission denied for older API key version
		err = privateBucket.SetBucketObjectLockConfiguration(ctx, project, bucketName, &metaclient.BucketObjectLockConfiguration{})
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

		config := &metaclient.BucketObjectLockConfiguration{Enabled: true}

		// no bucket name
		err = privateBucket.SetBucketObjectLockConfiguration(ctx, project, "", config)
		require.ErrorIs(t, err, uplink.ErrBucketNameInvalid)

		// bucket not exists
		err = privateBucket.SetBucketObjectLockConfiguration(ctx, project, bucketName, config)
		require.ErrorIs(t, err, uplink.ErrBucketNotFound)

		_, err = privateBucket.CreateBucketWithObjectLock(ctx, project, privateBucket.CreateBucketWithObjectLockParams{
			Name:              bucketName,
			ObjectLockEnabled: false,
		})
		require.NoError(t, err)

		err = privateBucket.SetBucketObjectLockConfiguration(ctx, project, bucketName, config)
		require.NoError(t, err)

		configResp, err := privateBucket.GetBucketObjectLockConfiguration(ctx, project, bucketName)
		require.NoError(t, err)
		require.True(t, configResp.Enabled)

		duration := int32(5)
		config.DefaultRetention = &metaclient.DefaultRetention{
			Mode:  storj.ComplianceMode,
			Years: duration,
			Days:  duration,
		}

		err = privateBucket.SetBucketObjectLockConfiguration(ctx, project, bucketName, config)
		require.ErrorIs(t, err, privateBucket.ErrBucketInvalidObjectLockConfig)

		config.DefaultRetention.Years = 0
		config.DefaultRetention.Mode = storj.NoRetention

		err = privateBucket.SetBucketObjectLockConfiguration(ctx, project, bucketName, config)
		require.ErrorIs(t, err, privateBucket.ErrBucketInvalidObjectLockConfig)

		config.DefaultRetention.Mode = storj.GovernanceMode

		err = privateBucket.SetBucketObjectLockConfiguration(ctx, project, bucketName, config)
		require.NoError(t, err)

		configResp, err = privateBucket.GetBucketObjectLockConfiguration(ctx, project, bucketName)
		require.NoError(t, err)
		require.True(t, configResp.Enabled)
		require.NotNil(t, configResp.DefaultRetention)
		require.Equal(t, config.DefaultRetention.Mode, configResp.DefaultRetention.Mode)
		require.Equal(t, config.DefaultRetention.Days, configResp.DefaultRetention.Days)
		require.Equal(t, config.DefaultRetention.Years, configResp.DefaultRetention.Years)
	})
}

func TestSuspendVersioningObjectLock(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.ObjectLockEnabled = true
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionObjectLock
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		upl := planet.Uplinks[0]
		projectID := upl.Projects[0].ID

		project, err := upl.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = sat.API.DB.Buckets().CreateBucket(ctx, buckets.Bucket{
			ProjectID:  projectID,
			Name:       "test-bucket",
			Versioning: buckets.VersioningEnabled,
			ObjectLock: buckets.ObjectLockSettings{
				Enabled: true,
			},
		})
		require.NoError(t, err)

		require.ErrorIs(t, privateBucket.SetBucketVersioning(ctx, project, "test-bucket", false), privateBucket.ErrBucketInvalidStateObjectLock)
	})
}

func TestGetBucketTagging(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.BucketTaggingEnabled = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		upl := planet.Uplinks[0]
		projectID := upl.Projects[0].ID

		project, err := upl.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		const bucketName = "test-bucket"
		_, err = sat.API.DB.Buckets().CreateBucket(ctx, buckets.Bucket{
			ProjectID: projectID,
			Name:      bucketName,
		})
		require.NoError(t, err)

		t.Run("Nonexistent bucket", func(t *testing.T) {
			_, err := privateBucket.GetBucketTagging(ctx, project, "nonexistent-bucket")
			require.ErrorIs(t, err, uplink.ErrBucketNotFound)
		})

		t.Run("No tags", func(t *testing.T) {
			_, err := privateBucket.GetBucketTagging(ctx, project, bucketName)
			require.ErrorIs(t, err, privateBucket.ErrTagsNotFound)
		})

		t.Run("Basic", func(t *testing.T) {
			expectedTags := []privateBucket.Tag{
				{
					Key:   "abcdeABCDE01234+-./:=@_",
					Value: "_@=:/.-+fghijFGHIJ56789",
				},
				{
					Key:   string([]rune{'Ա', 'א', 'ء', 'ऄ', 'ঀ', '٠', '०', '০'}),
					Value: string([]rune{'ֆ', 'ת', 'ي', 'ह', 'হ', '٩', '९', '৯'}),
				},
				{
					Key:   "\t\n\v\f\r \xc2\x85\xc2\xa0\u1680\u2002",
					Value: "\u3000\u2003\xc2\xa0\xc2\x85 \r\f\v\n\t",
				},
				{
					Key:   "key",
					Value: "",
				},
			}

			var dbTags []buckets.Tag
			for _, tag := range expectedTags {
				dbTags = append(dbTags, buckets.Tag(tag))
			}
			require.NoError(t, sat.DB.Buckets().SetBucketTagging(ctx, []byte(bucketName), projectID, dbTags))

			tags, err := privateBucket.GetBucketTagging(ctx, project, bucketName)
			require.NoError(t, err)
			require.Equal(t, expectedTags, tags)
		})

		t.Run("Missing bucket name", func(t *testing.T) {
			_, err := privateBucket.GetBucketTagging(ctx, project, "")
			require.ErrorIs(t, err, uplink.ErrBucketNameInvalid)
		})
	})
}
