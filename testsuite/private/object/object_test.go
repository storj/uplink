// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package object_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/grant"
	"storj.io/common/macaroon"
	"storj.io/common/memory"
	"storj.io/common/pkcrypto"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/common/uuid"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/buckets"
	"storj.io/storj/satellite/console"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/metabase/metabasetest"
	"storj.io/uplink"
	"storj.io/uplink/private/bucket"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/multipart"
	"storj.io/uplink/private/object"
	"storj.io/uplink/private/testuplink"
)

// TODO(ver) add tests for versioned/unversioned/suspended objects as well as delete markers
// for all methods from 'object' package

func TestStatObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		_, err = object.StatObject(ctx, project, "", "", nil)
		require.ErrorIs(t, err, uplink.ErrBucketNameInvalid)

		_, err = object.StatObject(ctx, project, bucketName, "", nil)
		require.ErrorIs(t, err, uplink.ErrObjectKeyInvalid)

		_, err = object.StatObject(ctx, project, "non-existing-bucket", objectKey, nil)
		require.ErrorIs(t, err, uplink.ErrObjectNotFound)

		_, err = object.StatObject(ctx, project, bucketName, "non-existing-object", nil)
		require.ErrorIs(t, err, uplink.ErrObjectNotFound)

		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		obj, err := object.StatObject(ctx, project, bucketName, objectKey, nil)
		require.NoError(t, err)
		require.Equal(t, objectKey, obj.Key)
		require.NotZero(t, obj.Version)

		// try to stat specific version
		objTwo, err := object.StatObject(ctx, project, bucketName, objectKey, obj.Version)
		require.NoError(t, err)
		require.Equal(t, objectKey, objTwo.Key)
		require.Equal(t, obj.Version, objTwo.Version)

		// try to stat NOT EXISTING version
		nonExistingVersion := make([]byte, 16)
		nonExistingVersion[0] = 1
		_, err = object.StatObject(ctx, project, bucketName, objectKey, nonExistingVersion)
		require.ErrorIs(t, err, uplink.ErrObjectNotFound)
	})
}

func TestCommitUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		uploadInfo, err := project.BeginUpload(ctx, bucketName, objectKey, nil)
		require.NoError(t, err)

		// use custom method which will also return object version
		obj, err := object.CommitUpload(ctx, project, bucketName, objectKey, uploadInfo.UploadID, nil)
		require.NoError(t, err)
		require.NotEmpty(t, obj.Version)

		statObj, err := object.StatObject(ctx, project, bucketName, objectKey, obj.Version)
		require.NoError(t, err)
		require.EqualExportedValues(t, *obj, *statObj)
	})
}

func TestUploadObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		upload, err := object.UploadObject(ctx, project, bucketName, objectKey, nil)
		require.NoError(t, err)

		_, err = upload.Write([]byte("test1"))
		require.NoError(t, err)

		require.NoError(t, upload.Commit())
		require.Empty(t, upload.Info().Version)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		upload, err = object.UploadObject(ctx, project, bucketName, objectKey, nil)
		require.NoError(t, err)

		_, err = upload.Write([]byte("test2"))
		require.NoError(t, err)

		require.NoError(t, upload.Commit())
		require.NotEmpty(t, upload.Info().Version)

		statObj, err := object.StatObject(ctx, project, bucketName, objectKey, upload.Info().Version)
		require.NoError(t, err)

		uploadObject := upload.Info()
		uploadObject.Custom = uplink.CustomMetadata{}
		statObj.Custom = uplink.CustomMetadata{}
		require.EqualExportedValues(t, *uploadObject, *statObj)
	})
}

func TestUploadObjectWithObjectLock(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionObjectLock
			},
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
				config.Metainfo.ObjectLockEnabled = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              bucketName,
			ObjectLockEnabled: true,
		})
		require.NoError(t, err)

		retention := metaclient.Retention{
			Mode:        storj.ComplianceMode,
			RetainUntil: time.Now().Add(time.Hour).Truncate(time.Hour).UTC(),
		}
		govRetention := metaclient.Retention{
			Mode:        storj.GovernanceMode,
			RetainUntil: time.Now().Add(time.Hour).Truncate(time.Hour).UTC(),
		}

		for _, testCase := range []struct {
			name                    string
			expectedRetention       *metaclient.Retention
			legalHold               bool
			expectedDeleteObjectErr error
		}{
			{
				name: "no retention, no legal hold",
			},
			{
				name:                    "retention - compliance, no legal hold",
				expectedRetention:       &retention,
				expectedDeleteObjectErr: object.ErrObjectProtected,
			},
			{
				name:                    "retention - governance, no legal hold",
				expectedRetention:       &govRetention,
				expectedDeleteObjectErr: object.ErrObjectProtected,
			},
			{
				name:                    "no retention, legal hold",
				legalHold:               true,
				expectedDeleteObjectErr: object.ErrObjectProtected,
			},
			{
				name:                    "retention - compliance, legal hold",
				expectedRetention:       &retention,
				legalHold:               true,
				expectedDeleteObjectErr: object.ErrObjectProtected,
			},
			{
				name:                    "retention - governance, legal hold",
				expectedRetention:       &govRetention,
				legalHold:               true,
				expectedDeleteObjectErr: object.ErrObjectProtected,
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				opts := &object.UploadOptions{
					LegalHold: testCase.legalHold,
				}
				if testCase.expectedRetention != nil {
					opts.Retention = *testCase.expectedRetention
				}
				upload, err := object.UploadObject(ctx, project, bucketName, objectKey, opts)
				require.NoError(t, err)

				_, err = upload.Write([]byte("test1"))
				require.NoError(t, err)

				require.NoError(t, upload.Commit())
				require.NotEmpty(t, upload.Info().Version)

				statObj, err := object.StatObject(ctx, project, bucketName, objectKey, upload.Info().Version)
				require.NoError(t, err)
				require.Equal(t, testCase.expectedRetention, statObj.Retention)
				require.Equal(t, &testCase.legalHold, statObj.LegalHold)

				uploadObject := upload.Info()
				uploadObject.Custom = uplink.CustomMetadata{}
				statObj.Custom = uplink.CustomMetadata{}
				statObj.LegalHold = nil
				statObj.Retention = nil
				require.EqualExportedValues(t, *uploadObject, *statObj)

				_, err = object.DeleteObject(ctx, project, bucketName, objectKey, upload.Info().Version, nil)
				require.ErrorIs(t, err, testCase.expectedDeleteObjectErr)
			})
		}
	})
}

func TestUploadObjectWithDefaultRetention(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
				config.Metainfo.ObjectLockEnabled = true
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionObjectLock
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		upl := planet.Uplinks[0]
		projectID := upl.Projects[0].ID

		err := sat.API.DB.Console().Projects().UpdateDefaultVersioning(ctx, projectID, console.DefaultVersioning(buckets.VersioningEnabled))
		require.NoError(t, err)

		project, err := upl.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		type testOpts struct {
			defaultRetention  metaclient.DefaultRetention
			overrideRetention metaclient.Retention
			expectedRetention metaclient.Retention
		}

		test := func(t *testing.T, opts testOpts) {
			bucketName := testrand.BucketName()
			_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
				Name:              bucketName,
				ObjectLockEnabled: true,
			})
			require.NoError(t, err)

			err := bucket.SetBucketObjectLockConfiguration(ctx, project, bucketName, &metaclient.BucketObjectLockConfiguration{
				Enabled:          true,
				DefaultRetention: &opts.defaultRetention,
			})
			require.NoError(t, err)

			objectKey := testrand.Path()

			upload, err := object.UploadObject(ctx, project, bucketName, objectKey, &metaclient.UploadOptions{
				Retention: opts.overrideRetention,
			})
			require.NoError(t, err)
			require.NoError(t, upload.Commit())

			retention, err := object.GetObjectRetention(ctx, project, bucketName, objectKey, nil)
			require.NoError(t, err)

			require.Equal(t, opts.expectedRetention.Mode, retention.Mode)
			require.WithinDuration(t, opts.expectedRetention.RetainUntil, retention.RetainUntil, time.Minute)
		}

		t.Run("Use default retention", func(t *testing.T) {
			t.Run("Days, Compliance mode", func(t *testing.T) {
				test(t, testOpts{
					defaultRetention: metaclient.DefaultRetention{
						Mode: storj.ComplianceMode,
						Days: 3,
					},
					expectedRetention: metaclient.Retention{
						Mode:        storj.ComplianceMode,
						RetainUntil: time.Now().AddDate(0, 0, 3),
					},
				})
			})

			t.Run("Years, Governance mode", func(t *testing.T) {
				test(t, testOpts{
					defaultRetention: metaclient.DefaultRetention{
						Mode:  storj.GovernanceMode,
						Years: 5,
					},
					expectedRetention: metaclient.Retention{
						Mode:        storj.GovernanceMode,
						RetainUntil: time.Now().AddDate(5, 0, 0),
					},
				})
			})

			t.Run("Leap year", func(t *testing.T) {
				// Find the nearest date N years after the current date that lies after a leap day.
				now := time.Now()
				leapYear := now.Year()
				var leapDay time.Time
				for {
					if (leapYear%4 == 0 && leapYear%100 != 0) || (leapYear%400 == 0) {
						leapDay = time.Date(leapYear, time.February, 29, 0, 0, 0, 0, time.UTC)
						if leapDay.After(now) {
							break
						}
					}
					leapYear++
				}
				years := leapYear - now.Year()
				if now.AddDate(years, 0, 0).Before(leapDay) {
					years++
				}

				t.Run("Days", func(t *testing.T) {
					// Expect 1 day to always be considered a 24-hour period, with no adjustments
					// made to accommodate the leap day.
					test(t, testOpts{
						defaultRetention: metaclient.DefaultRetention{
							Mode: storj.ComplianceMode,
							Days: int32(365 * years),
						},
						expectedRetention: metaclient.Retention{
							Mode:        storj.ComplianceMode,
							RetainUntil: time.Now().AddDate(0, 0, 365*years),
						},
					})
				})

				t.Run("Years", func(t *testing.T) {
					// Expect the retention period duration to take the leap day into account.
					test(t, testOpts{
						defaultRetention: metaclient.DefaultRetention{
							Mode:  storj.ComplianceMode,
							Years: int32(years),
						},
						expectedRetention: metaclient.Retention{
							Mode:        storj.ComplianceMode,
							RetainUntil: time.Now().AddDate(0, 0, 365*years+1),
						},
					})
				})
			})
		})

		t.Run("Override default retention", func(t *testing.T) {
			override := metaclient.Retention{
				Mode:        storj.GovernanceMode,
				RetainUntil: time.Now().AddDate(0, 0, 5),
			}

			test(t, testOpts{
				defaultRetention: metaclient.DefaultRetention{
					Mode:  storj.ComplianceMode,
					Years: 3,
				},
				overrideRetention: override,
				expectedRetention: override,
			})
		})

		t.Run("TTL is disallowed", func(t *testing.T) {
			test := func(t *testing.T, project *uplink.Project, objectTTL time.Time, expectedError error) {
				bucketName := testrand.BucketName()
				_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
					Name:              bucketName,
					ObjectLockEnabled: true,
				})
				require.NoError(t, err)

				err := bucket.SetBucketObjectLockConfiguration(ctx, project, bucketName, &metaclient.BucketObjectLockConfiguration{
					Enabled: true,
					DefaultRetention: &metaclient.DefaultRetention{
						Mode:  storj.ComplianceMode,
						Years: 5,
					},
				})
				require.NoError(t, err)

				objectKey := testrand.Path()

				upload, err := object.UploadObject(ctx, project, bucketName, objectKey, &metaclient.UploadOptions{
					Expires: objectTTL,
				})
				require.NoError(t, err)
				require.ErrorIs(t, upload.Commit(), expectedError)
			}

			t.Run("TTL set on object", func(t *testing.T) {
				test(t, project, time.Now().Add(time.Hour), object.ErrObjectLockUploadWithTTLAndDefaultRetention)
			})

			t.Run("TTL set on API key", func(t *testing.T) {
				userCtx, err := sat.UserContext(ctx, upl.Projects[0].Owner.ID)
				require.NoError(t, err)

				_, key, err := sat.API.Console.Service.CreateAPIKey(userCtx, projectID, "test key", macaroon.APIKeyVersionObjectLock)
				require.NoError(t, err)

				// TODO: Use the (*uplink.Access).Share method once it has been updated to support Object Lock permissions.
				dur := time.Hour
				key, err = key.Restrict(macaroon.Caveat{MaxObjectTtl: &dur})
				require.NoError(t, err)

				access, err := uplink.RequestAccessWithPassphrase(ctx, sat.URL(), key.Serialize(), "")
				require.NoError(t, err)

				project, err := uplink.OpenProject(ctx, access)
				require.NoError(t, err)

				test(t, project, time.Time{}, object.ErrObjectLockUploadWithTTLAPIKeyAndDefaultRetention)
			})
		})
	})
}

func TestGetAndSetObjectLegalHold(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
				config.Metainfo.ObjectLockEnabled = true
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionObjectLock
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		upl := planet.Uplinks[0]
		projectID := upl.Projects[0].ID

		err := sat.API.DB.Console().Projects().UpdateDefaultVersioning(ctx, projectID, console.DefaultVersioning(buckets.VersioningEnabled))
		require.NoError(t, err)

		project, err := upl.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		invalidBucket := "invalid-bucket"

		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              invalidBucket,
			ObjectLockEnabled: false,
		})
		require.NoError(t, err)

		objectKey := "test-object"

		upload, err := object.UploadObject(ctx, project, invalidBucket, objectKey, nil)
		require.NoError(t, err)

		_, err = upload.Write([]byte("test1"))
		require.NoError(t, err)

		require.NoError(t, upload.Commit())
		require.NotEmpty(t, upload.Info().Version)

		legalHoldStatus, err := object.GetObjectLegalHold(ctx, project, invalidBucket, objectKey, upload.Info().Version)
		require.ErrorIs(t, err, bucket.ErrBucketNoLock)
		require.False(t, legalHoldStatus)

		err = object.SetObjectLegalHold(ctx, project, invalidBucket, objectKey, upload.Info().Version, true)
		require.ErrorIs(t, err, bucket.ErrBucketNoLock)

		bucketName := "test-bucket"

		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              bucketName,
			ObjectLockEnabled: true,
		})
		require.NoError(t, err)

		upload, err = object.UploadObject(ctx, project, bucketName, objectKey, nil)
		require.NoError(t, err)

		_, err = upload.Write([]byte("test1"))
		require.NoError(t, err)

		require.NoError(t, upload.Commit())
		require.NotEmpty(t, upload.Info().Version)

		wrongBucket := "random-bucket"
		wrongKey := "random-key"

		legalHoldStatus, err = object.GetObjectLegalHold(ctx, project, wrongBucket, objectKey, upload.Info().Version)
		require.True(t, strings.Contains(err.Error(), string(metaclient.ErrBucketNotFound)))
		require.False(t, legalHoldStatus)

		legalHoldStatus, err = object.GetObjectLegalHold(ctx, project, bucketName, wrongKey, upload.Info().Version)
		require.True(t, strings.Contains(err.Error(), string(metaclient.ErrObjectNotFound)))
		require.False(t, legalHoldStatus)

		err = object.SetObjectLegalHold(ctx, project, wrongBucket, objectKey, upload.Info().Version, true)
		require.True(t, strings.Contains(err.Error(), string(metaclient.ErrBucketNotFound)))

		err = object.SetObjectLegalHold(ctx, project, bucketName, wrongKey, upload.Info().Version, true)
		require.True(t, strings.Contains(err.Error(), string(metaclient.ErrObjectNotFound)))

		err = object.SetObjectLegalHold(ctx, project, bucketName, objectKey, upload.Info().Version, true)
		require.NoError(t, err)

		legalHoldStatus, err = object.GetObjectLegalHold(ctx, project, bucketName, objectKey, upload.Info().Version)
		require.NoError(t, err)
		require.True(t, legalHoldStatus)
	})
}

func runRetentionModeTests(t *testing.T, name string, fn func(t *testing.T, mode storj.RetentionMode)) {
	for _, tt := range []struct {
		name string
		mode storj.RetentionMode
	}{
		{name: "Compliance", mode: storj.ComplianceMode},
		{name: "Governance", mode: storj.GovernanceMode},
	} {
		t.Run(fmt.Sprintf("%s (%s)", name, tt.name), func(t *testing.T) {
			fn(t, tt.mode)
		})
	}
}

func TestSetObjectRetention(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
				config.Metainfo.ObjectLockEnabled = true
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionObjectLock
				config.DefaultPathCipher = storj.EncNull
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		up := planet.Uplinks[0]

		project, err := up.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		bucketName := testrand.BucketName()
		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              bucketName,
			ObjectLockEnabled: true,
		})
		require.NoError(t, err)

		createObjectWithRetention := func(t *testing.T, project *uplink.Project, bucketName string, retention metaclient.Retention) string {
			objectKey := testrand.Path()
			upload, err := object.UploadObject(ctx, project, bucketName, objectKey, &metaclient.UploadOptions{
				Retention: retention,
			})
			require.NoError(t, err)
			require.NoError(t, upload.Commit())
			return objectKey
		}

		future := time.Now().Add(time.Hour)
		bypassOpts := &metaclient.SetObjectRetentionOptions{BypassGovernanceRetention: true}

		runRetentionModeTests(t, "Set", func(t *testing.T, mode storj.RetentionMode) {
			objectKey := createObjectWithRetention(t, project, bucketName, metaclient.Retention{})
			require.NoError(t, object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, metaclient.Retention{
				Mode:        mode,
				RetainUntil: future,
			}, nil))
		})

		runRetentionModeTests(t, "Extend", func(t *testing.T, mode storj.RetentionMode) {
			retention := metaclient.Retention{
				Mode:        mode,
				RetainUntil: future,
			}
			objectKey := createObjectWithRetention(t, project, bucketName, retention)
			retention.RetainUntil = retention.RetainUntil.Add(time.Minute)
			require.NoError(t, object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, retention, nil))
		})

		runRetentionModeTests(t, "Shorten", func(t *testing.T, mode storj.RetentionMode) {
			retention := metaclient.Retention{
				Mode:        mode,
				RetainUntil: future,
			}
			objectKey := createObjectWithRetention(t, project, bucketName, retention)

			retention.RetainUntil = retention.RetainUntil.Add(-time.Minute)
			err := object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, retention, nil)
			require.ErrorIs(t, err, object.ErrObjectProtected)

			err = object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, retention, bypassOpts)
			if mode == storj.GovernanceMode {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, object.ErrObjectProtected)
			}
		})

		t.Run("Change mode", func(t *testing.T) {
			retention := metaclient.Retention{
				Mode:        storj.GovernanceMode,
				RetainUntil: future,
			}
			objectKey := createObjectWithRetention(t, project, bucketName, retention)

			retention.Mode = storj.ComplianceMode
			err := object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, retention, nil)
			require.ErrorIs(t, err, object.ErrObjectProtected)

			require.NoError(t, object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, retention, bypassOpts))

			retention.Mode = storj.GovernanceMode
			err = object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, retention, nil)
			require.ErrorIs(t, err, object.ErrObjectProtected)

			err = object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, retention, bypassOpts)
			require.ErrorIs(t, err, object.ErrObjectProtected)
		})

		runRetentionModeTests(t, "Remove active", func(t *testing.T, mode storj.RetentionMode) {
			objectKey := createObjectWithRetention(t, project, bucketName, metaclient.Retention{
				Mode:        mode,
				RetainUntil: future,
			})

			err := object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, metaclient.Retention{}, nil)
			require.ErrorIs(t, err, object.ErrObjectProtected)

			err = object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, metaclient.Retention{}, bypassOpts)
			if mode == storj.GovernanceMode {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, object.ErrObjectProtected)
			}
		})

		runRetentionModeTests(t, "Remove expired", func(t *testing.T, mode storj.RetentionMode) {
			objectKey := testrand.Path()
			objStream := metabase.ObjectStream{
				ProjectID:  up.Projects[0].ID,
				BucketName: metabase.BucketName(bucketName),
				ObjectKey:  metabase.ObjectKey(objectKey),
				Version:    1,
				StreamID:   testrand.UUID(),
			}

			metabasetest.CreateTestObject{
				BeginObjectExactVersion: &metabase.BeginObjectExactVersion{
					ObjectStream: objStream,
					Encryption:   metabasetest.DefaultEncryption,
					Retention: metabase.Retention{
						Mode:        mode,
						RetainUntil: time.Now().Add(-time.Hour),
					},
				},
			}.Run(ctx, t, sat.Metabase.DB, objStream, 0)

			require.NoError(t, object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, metaclient.Retention{}, nil))
		})

		retention := metaclient.Retention{
			Mode:        storj.ComplianceMode,
			RetainUntil: future,
		}

		t.Run("Missing object", func(t *testing.T) {
			err = object.SetObjectRetention(ctx, project, bucketName, testrand.Path(), nil, retention, bypassOpts)
			require.ErrorIs(t, err, uplink.ErrObjectNotFound)
		})

		t.Run("Object Lock disabled for bucket", func(t *testing.T) {
			bucketName := testrand.BucketName()
			_, err := bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
				Name:              bucketName,
				ObjectLockEnabled: false,
			})
			require.NoError(t, err)

			err = object.SetObjectRetention(ctx, project, bucketName, testrand.Path(), nil, retention, bypassOpts)
			require.ErrorIs(t, err, bucket.ErrBucketNoLock)
		})

		t.Run("Invalid object state with expiring object", func(t *testing.T) {
			objectKey := testrand.Path()
			upload, err := object.UploadObject(ctx, project, bucketName, objectKey, &object.UploadOptions{
				Expires: time.Now().Add(time.Minute),
			})
			require.NoError(t, err)
			require.NoError(t, upload.Commit())

			err = object.SetObjectRetention(ctx, project, bucketName, objectKey, nil, retention, bypassOpts)
			require.ErrorIs(t, err, object.ErrObjectLockInvalidObjectState)
		})
	})
}

func TestGetObjectRetention(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
				config.Metainfo.ObjectLockEnabled = true
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionObjectLock
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		up := planet.Uplinks[0]

		project, err := up.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		bucketName := testrand.BucketName()
		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              bucketName,
			ObjectLockEnabled: true,
		})
		require.NoError(t, err)

		runRetentionModeTests(t, "Success", func(t *testing.T, mode storj.RetentionMode) {
			objectKey := testrand.Path()
			retainUntil := time.Now().Add(time.Hour).Truncate(time.Microsecond).UTC()

			upload, err := object.UploadObject(ctx, project, bucketName, objectKey, &metaclient.UploadOptions{
				Retention: metaclient.Retention{
					Mode:        mode,
					RetainUntil: retainUntil,
				},
			})
			require.NoError(t, err)
			require.NoError(t, upload.Commit())

			retention, err := object.GetObjectRetention(ctx, project, bucketName, objectKey, nil)
			require.NoError(t, err)
			require.NotNil(t, retention)
			require.Equal(t, mode, retention.Mode)
			require.Equal(t, retainUntil, retention.RetainUntil)
		})

		t.Run("No retention", func(t *testing.T) {
			objectKey := testrand.Path()
			upload, err := object.UploadObject(ctx, project, bucketName, objectKey, nil)
			require.NoError(t, err)
			require.NoError(t, upload.Commit())

			retention, err := object.GetObjectRetention(ctx, project, bucketName, objectKey, nil)
			require.ErrorIs(t, err, object.ErrRetentionNotFound)
			require.Nil(t, retention)
		})

		t.Run("Missing object", func(t *testing.T) {
			retention, err := object.GetObjectRetention(ctx, project, bucketName, testrand.Path(), nil)
			require.ErrorIs(t, err, uplink.ErrObjectNotFound)
			require.Nil(t, retention)
		})

		t.Run("Object Lock disabled for bucket", func(t *testing.T) {
			bucketName := testrand.BucketName()
			_, err := bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
				Name:              bucketName,
				ObjectLockEnabled: false,
			})
			require.NoError(t, err)

			retention, err := object.GetObjectRetention(ctx, project, bucketName, testrand.Path(), nil)
			require.ErrorIs(t, err, bucket.ErrBucketNoLock)
			require.Nil(t, retention)
		})

		t.Run("Invalid object state with delete marker", func(t *testing.T) {
			objectKey := testrand.Path()
			upload, err := object.UploadObject(ctx, project, bucketName, objectKey, nil)
			require.NoError(t, err)
			require.NoError(t, upload.Commit())

			_, err = object.DeleteObject(ctx, project, bucketName, objectKey, nil, nil)
			require.NoError(t, err)

			_, err = object.GetObjectRetention(ctx, project, bucketName, objectKey, nil)
			require.ErrorIs(t, err, object.ErrObjectLockInvalidObjectState)
		})
	})
}

func TestUploadObject_OldCodePath(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		uploadCtx := testuplink.DisableConcurrentSegmentUploads(ctx)

		project, err := planet.Uplinks[0].OpenProject(uploadCtx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		upload, err := object.UploadObject(uploadCtx, project, bucketName, objectKey, nil)
		require.NoError(t, err)

		_, err = upload.Write([]byte("test"))
		require.NoError(t, err)

		require.NoError(t, upload.Commit())
		require.NotEmpty(t, upload.Info().Version)

		statObj, err := object.StatObject(uploadCtx, project, bucketName, objectKey, upload.Info().Version)
		require.NoError(t, err)

		uploadObject := upload.Info()
		require.EqualExportedValues(t, *uploadObject, *statObj)
	})
}

func TestDownloadObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"

		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		expectedDataA := testrand.Bytes(10 * memory.KiB)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, expectedDataA)
		require.NoError(t, err)

		downloadObject := func(version []byte) ([]byte, *object.VersionedObject) {
			download, err := object.DownloadObject(ctx, project, bucketName, objectKey, version, nil)
			require.NoError(t, err)

			data, err := io.ReadAll(download)
			require.NoError(t, err)
			downloadInfo := download.Info()
			require.NotEmpty(t, downloadInfo.Version)

			require.NoError(t, download.Close())
			return data, download.Info()
		}

		// download latest version
		data, downloadInfo := downloadObject(nil)
		require.Equal(t, expectedDataA, data)

		// download using version returned with previous download
		data, versionedDownloadInfo := downloadObject(nil)
		require.Equal(t, expectedDataA, data)
		require.EqualExportedValues(t, *downloadInfo, *versionedDownloadInfo)

		// try download non existing version
		notExistingVerions := downloadInfo.Version
		notExistingVerions[0] = 99
		_, err = object.DownloadObject(ctx, project, bucketName, objectKey, notExistingVerions, nil)
		require.ErrorIs(t, err, uplink.ErrObjectNotFound)

		// upload second version of the same object
		expectedDataB := testrand.Bytes(9 * memory.KiB)

		upload, err := object.UploadObject(ctx, project, bucketName, objectKey, nil)
		require.NoError(t, err)

		_, err = upload.Write(expectedDataB)
		require.NoError(t, err)

		require.NoError(t, upload.Commit())
		require.NotEmpty(t, upload.Info().Version)

		// download latest version
		data, _ = downloadObject(nil)
		require.Equal(t, expectedDataB, data)

		// download using version returned by upload
		data, _ = downloadObject(upload.Info().Version)
		require.Equal(t, expectedDataB, data)

		// create delete marker
		err = planet.Uplinks[0].DeleteObject(ctx, planet.Satellites[0], bucketName, objectKey)
		require.NoError(t, err)

		// download latest version will return error
		_, err = planet.Uplinks[0].Download(ctx, planet.Satellites[0], bucketName, objectKey)
		require.Error(t, err)

		// download using version returned by upload, previous version of object is still available
		data, _ = downloadObject(upload.Info().Version)
		require.Equal(t, expectedDataB, data)

		// TODO(ver): add test to download delete marker
	})
}

func TestDeleteObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		upload := func(key string) *object.VersionedObject {
			upload, err := object.UploadObject(ctx, project, bucketName, key, nil)
			require.NoError(t, err)

			_, err = upload.Write([]byte("test"))
			require.NoError(t, err)

			require.NoError(t, upload.Commit())
			require.NotEmpty(t, upload.Info().Version)
			return upload.Info()
		}

		_ = upload(objectKey)
		uploadInfoA2 := upload(objectKey)
		uploadInfoB := upload(objectKey + "B")

		objects, err := planet.Satellites[0].Metabase.DB.TestingAllObjects(ctx)
		require.NoError(t, err)
		require.Len(t, objects, 3)

		deleteObj, err := object.DeleteObject(ctx, project, bucketName, uploadInfoA2.Key, uploadInfoA2.Version, nil)
		require.NoError(t, err)
		require.NotEmpty(t, deleteObj.Version)
		// delete was done with specified version so no delete marker should be created
		require.False(t, deleteObj.IsDeleteMarker)

		// delete non existing version of existing object
		nonExistingVersion := slices.Clone(uploadInfoB.Version)
		nonExistingVersion[0]++ // change original version
		deleteObj, err = object.DeleteObject(ctx, project, bucketName, uploadInfoB.Key, nonExistingVersion, nil)
		require.NoError(t, err)
		require.Nil(t, deleteObj)

		// delete latest version with version nil
		deleteObj, err = object.DeleteObject(ctx, project, bucketName, uploadInfoB.Key, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, deleteObj.Version)
		require.True(t, deleteObj.IsDeleteMarker)

		items, _, err := object.ListObjectVersions(ctx, project, bucketName, nil)
		require.NoError(t, err)

		listedDeleteMarkers := 0
		listedObjects := 0
		for _, item := range items {
			if item.IsDeleteMarker {
				listedDeleteMarkers++
			} else {
				listedObjects++
			}
		}
		require.Equal(t, 1, listedDeleteMarkers)
		require.Equal(t, 2, listedObjects)
	})
}

func TestDeleteObject_BypassGovernanceRetention(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
				config.Metainfo.ObjectLockEnabled = true
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionObjectLock
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		const (
			bucketName = "test-bucket"
			objectKey  = "test-object"
		)
		sat := planet.Satellites[0]
		up := planet.Uplinks[0]
		projectID := up.Projects[0].ID
		userID := up.Projects[0].Owner.ID

		project, err := up.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              bucketName,
			ObjectLockEnabled: true,
		})
		require.NoError(t, err)

		upload, err := object.UploadObject(ctx, project, bucketName, objectKey, &metaclient.UploadOptions{
			Retention: metaclient.Retention{
				Mode:        storj.GovernanceMode,
				RetainUntil: time.Now().Add(time.Hour),
			},
		})
		require.NoError(t, err)

		_, err = upload.Write([]byte("test"))
		require.NoError(t, err)

		require.NoError(t, upload.Commit())

		// TODO: Use the up.Access[sat.ID()].Share method once it has been updated to support Object Lock permissions.

		userCtx, err := sat.UserContext(ctx, userID)
		require.NoError(t, err)

		_, restrictedAPIKey, err := sat.API.Console.Service.CreateAPIKey(userCtx, projectID, "test key", macaroon.APIKeyVersionObjectLock)
		require.NoError(t, err)

		restrictedAPIKey, err = restrictedAPIKey.Restrict(macaroon.Caveat{DisallowBypassGovernanceRetention: true})
		require.NoError(t, err)

		restrictedAccess, err := uplink.RequestAccessWithPassphrase(ctx, sat.URL(), restrictedAPIKey.Serialize(), "")
		require.NoError(t, err)

		restrictedProject, err := uplink.OpenProject(ctx, restrictedAccess)
		require.NoError(t, err)

		opts := &metaclient.DeleteObjectOptions{BypassGovernanceRetention: true}
		_, err = object.DeleteObject(ctx, restrictedProject, bucketName, objectKey, upload.Info().Version, opts)
		require.ErrorIs(t, err, uplink.ErrPermissionDenied)

		objects, err := sat.Metabase.DB.TestingAllObjects(ctx)
		require.NoError(t, err)
		require.Len(t, objects, 1)

		_, err = object.DeleteObject(ctx, project, bucketName, objectKey, upload.Info().Version, opts)
		require.NoError(t, err)

		objects, err = sat.Metabase.DB.TestingAllObjects(ctx)
		require.NoError(t, err)
		require.Empty(t, objects)
	})
}

func TestCopyObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		expectedData := testrand.Bytes(5 * memory.KiB)
		// upload first version of object
		obj, err := planet.Uplinks[0].UploadWithOptions(ctx, planet.Satellites[0], bucketName, objectKey, expectedData, nil)
		require.NoError(t, err)

		// upload second version of object
		_, err = planet.Uplinks[0].UploadWithOptions(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(6*memory.KiB), nil)
		require.NoError(t, err)

		copiedObject, err := object.CopyObject(ctx, project, bucketName, objectKey, obj.Version, bucketName, objectKey+"-copy", object.CopyObjectOptions{})
		require.NoError(t, err)
		require.NotEmpty(t, copiedObject.Version)

		data, err := planet.Uplinks[0].Download(ctx, planet.Satellites[0], bucketName, objectKey+"-copy")
		require.NoError(t, err)
		require.Equal(t, expectedData, data)

		nonExistingVersion := slices.Clone(obj.Version)
		nonExistingVersion[0]++ // change original version
		_, err = object.CopyObject(ctx, project, bucketName, objectKey, nonExistingVersion, bucketName, objectKey+"-copy", object.CopyObjectOptions{})
		require.ErrorIs(t, err, uplink.ErrObjectNotFound)
	})
}

func TestCopyObjectWithObjectLock(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
				config.Metainfo.ObjectLockEnabled = true
			},
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.APIKeyVersion = macaroon.APIKeyVersionObjectLock
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		upl := planet.Uplinks[0]
		projectID := upl.Projects[0].ID

		err := sat.API.DB.Console().Projects().UpdateDefaultVersioning(ctx, projectID, console.DefaultVersioning(buckets.VersioningEnabled))
		require.NoError(t, err)

		project, err := upl.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		bucketName := "test-bucket"
		objectKey := "test-object"

		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              bucketName,
			ObjectLockEnabled: true,
		})
		require.NoError(t, err)

		obj, err := planet.Uplinks[0].UploadWithOptions(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(5*memory.KiB), nil)
		require.NoError(t, err)

		retention := metaclient.Retention{
			Mode:        storj.ComplianceMode,
			RetainUntil: time.Now().Add(time.Hour).Truncate(time.Hour).UTC(),
		}

		for _, testCase := range []struct {
			name              string
			expectedRetention *metaclient.Retention
			legalHold         bool
		}{
			{
				name: "no retention, no legal hold",
			},
			{
				name:              "retention, no legal hold",
				expectedRetention: &retention,
			},
			{
				name:      "no retention, legal hold",
				legalHold: true,
			},
			{
				name:              "retention, legal hold",
				expectedRetention: &retention,
				legalHold:         true,
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				options := object.CopyObjectOptions{
					LegalHold: testCase.legalHold,
				}
				if testCase.expectedRetention != nil {
					options.Retention = *testCase.expectedRetention
				}
				copiedObject, err := object.CopyObject(ctx, project, bucketName, objectKey, obj.Version, bucketName, objectKey+"-copy", options)
				require.NoError(t, err)
				require.Equal(t, testCase.expectedRetention, copiedObject.Retention)
				require.NotNil(t, copiedObject.LegalHold)
				require.Equal(t, testCase.legalHold, *copiedObject.LegalHold)

				objectInfo, err := object.StatObject(ctx, project, bucketName, copiedObject.Key, copiedObject.Version)
				require.NoError(t, err)
				require.Equal(t, testCase.expectedRetention, objectInfo.Retention)
				require.NotNil(t, copiedObject.LegalHold)
				require.Equal(t, testCase.legalHold, *objectInfo.LegalHold)
			})
		}

		noLockBucket := "no-lock-bucket"
		_, err = bucket.CreateBucketWithObjectLock(ctx, project, bucket.CreateBucketWithObjectLockParams{
			Name:              noLockBucket,
			ObjectLockEnabled: false,
		})
		require.NoError(t, err)

		// cannot set expectedRetention on object in bucket without object lock
		_, err = object.CopyObject(ctx, project, bucketName, objectKey, obj.Version, noLockBucket, objectKey, object.CopyObjectOptions{
			Retention: retention,
		})
		require.ErrorIs(t, err, object.ErrNoObjectLockConfiguration)

		// cannot set legal hold on object in bucket without object lock
		_, err = object.CopyObject(ctx, project, bucketName, objectKey, obj.Version, noLockBucket, objectKey, object.CopyObjectOptions{
			LegalHold: true,
		})
		require.ErrorIs(t, err, object.ErrNoObjectLockConfiguration)
	})
}

func TestObject_Versioning(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(100))
		require.NoError(t, err)

		err = planet.Uplinks[0].DeleteObject(ctx, planet.Satellites[0], bucketName, objectKey)
		require.NoError(t, err)

		objects, err := planet.Satellites[0].Metabase.DB.TestingAllObjects(ctx)
		require.NoError(t, err)
		require.Len(t, objects, 2)

		version := objects[0].StreamVersionID().Bytes()
		if objects[1].Status.IsDeleteMarker() {
			version = objects[1].StreamVersionID().Bytes()
		}

		_, err = object.StatObject(ctx, project, bucketName, objectKey, version)
		require.ErrorIs(t, err, object.ErrMethodNotAllowed)

		_, err = object.DownloadObject(ctx, project, bucketName, objectKey, version, nil)
		require.ErrorIs(t, err, object.ErrMethodNotAllowed)
	})
}

func TestListObjectVersions_SingleObject_TwoVersions(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		err = bucket.SetBucketVersioning(ctx, project, bucketName, true)
		require.NoError(t, err)

		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(memory.KiB))
		require.NoError(t, err)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		objects, more, err := object.ListObjectVersions(ctx, project, bucketName, nil)
		require.NoError(t, err)
		require.False(t, more)
		require.Len(t, objects, 2)
		require.Equal(t, objectKey, objects[0].Key)
		require.Equal(t, objectKey, objects[1].Key)
		require.NotEqual(t, objects[0].Version, objects[1].Version)
	})
}

func TestListObjects_TwoObjects_TwoVersionsEach_OneDeleteMarker(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKeyA := "test-objectA"
		objectKeyB := "test-objectB"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		err = bucket.SetBucketVersioning(ctx, project, bucketName, true)
		require.NoError(t, err)

		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKeyA, testrand.Bytes(memory.KiB))
		require.NoError(t, err)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKeyA, testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKeyB, testrand.Bytes(memory.KiB))
		require.NoError(t, err)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKeyB, testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		err = planet.Uplinks[0].DeleteObject(ctx, planet.Satellites[0], bucketName, objectKeyB)
		require.NoError(t, err)

		objects, more, err := object.ListObjectVersions(ctx, project, bucketName, nil)
		require.NoError(t, err)
		require.False(t, more)
		require.Len(t, objects, 5)
		require.Equal(t, objectKeyB, objects[0].Key)
		require.Equal(t, objectKeyB, objects[1].Key)
		require.Equal(t, objectKeyB, objects[2].Key)
		require.Equal(t, objectKeyA, objects[3].Key)
		require.Equal(t, objectKeyA, objects[4].Key)
		require.True(t, objects[0].IsDeleteMarker)
		require.NotEqual(t, objects[0].Version, objects[1].Version)
		require.NotEqual(t, objects[2].Version, objects[3].Version)
	})
}

func TestListObjectVersions_SingleObject_TwoVersions_OneDeleteMarker(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		err = bucket.SetBucketVersioning(ctx, project, bucketName, true)
		require.NoError(t, err)

		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(memory.KiB))
		require.NoError(t, err)
		err = planet.Uplinks[0].DeleteObject(ctx, planet.Satellites[0], bucketName, objectKey)
		require.NoError(t, err)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		objects, more, err := object.ListObjectVersions(ctx, project, bucketName, nil)
		require.NoError(t, err)
		require.False(t, more)
		require.Len(t, objects, 3)
		require.Equal(t, objectKey, objects[0].Key)
		require.Equal(t, objectKey, objects[1].Key)
		require.Equal(t, objectKey, objects[2].Key)
		require.True(t, objects[1].IsDeleteMarker)
		require.NotEqual(t, objects[0].Version, objects[1].Version)
		require.NotEqual(t, objects[1].Version, objects[2].Version)
		require.NotEqual(t, objects[0].Version, objects[2].Version)
	})
}

func TestListObjectVersions_Suspended(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// upload unversioned object
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "objectA", testrand.Bytes(100))
		require.NoError(t, err)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		// upload versioned object
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "objectA", testrand.Bytes(100))
		require.NoError(t, err)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, false))
		versioningState, err := bucket.GetBucketVersioning(ctx, project, bucketName)
		require.NoError(t, err)
		require.Equal(t, buckets.VersioningSuspended, buckets.Versioning(versioningState))

		// upload unversioned object in suspended bucket. should overwrite previous unversioned object
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "objectA", testrand.Bytes(100))
		require.NoError(t, err)

		items, _, err := object.ListObjectVersions(ctx, project, bucketName, nil)
		require.NoError(t, err)
		require.Len(t, items, 2)

		var versioned int
		// with listing version should be always set
		for _, item := range items {
			require.NotEmpty(t, item.Version)
			if item.IsVersioned {
				versioned++
			}
		}
		require.Equal(t, 1, versioned)
	})
}

func TestListObjectVersions_ListingLimit(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"

		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(memory.KiB))
		require.NoError(t, err)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(memory.KiB))
		require.NoError(t, err)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(memory.KiB))
		require.NoError(t, err)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, objectKey, testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		items, more, err := object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{Limit: 2})
		require.NoError(t, err)
		require.Len(t, items, 2)
		require.True(t, more)

		items, more, err = object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{Limit: 4})
		require.NoError(t, err)
		require.Len(t, items, 4)
		require.False(t, more)

		items, more, err = object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{Limit: 8})
		require.NoError(t, err)
		require.Len(t, items, 4)
		require.False(t, more)
	})
}

// TODO(ver): add listObjectVersions tests with cursors

func TestObject_Versioned_Unversioned(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		// upload unversioned object
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "objectA", testrand.Bytes(100))
		require.NoError(t, err)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		stat, err := object.StatObject(ctx, project, bucketName, "objectA", nil)
		require.NoError(t, err)
		require.Empty(t, stat.Version)
		require.False(t, stat.IsVersioned)

		// upload versioned object
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "objectA", testrand.Bytes(100))
		require.NoError(t, err)

		stat, err = object.StatObject(ctx, project, bucketName, "objectA", nil)
		require.NoError(t, err)
		require.NotEmpty(t, stat.Version)
		require.True(t, stat.IsVersioned)

		items, _, err := object.ListObjectVersions(ctx, project, bucketName, nil)
		require.NoError(t, err)
		require.Len(t, items, 2)

		var versioned int
		// with listing version should be always set
		for _, item := range items {
			require.NotEmpty(t, item.Version)
			if item.IsVersioned {
				versioned++
			}
		}
		require.Equal(t, 1, versioned)
	})
}

func TestListObjectVersions(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "foo/bar/A", testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "foo/bar/test/B", testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		type testCase struct {
			Prefix   string
			Prefixes []string
			Objects  []string
		}

		for _, tc := range []testCase{
			{
				Prefix:   "",
				Prefixes: []string{"foo/"},
			},
			{
				Prefix:   "foo/",
				Prefixes: []string{"bar/"},
			},
			{
				Prefix:   "foo/bar/",
				Prefixes: []string{"test/"},
				Objects:  []string{"A"},
			},
			{
				Prefix:  "foo/bar/test/",
				Objects: []string{"B"},
			},
		} {
			result, _, err := object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{
				Prefix: tc.Prefix,
			})
			require.NoError(t, err)

			prefixes := []string{}
			objects := []string{}
			for _, item := range result {
				if item.IsPrefix {
					prefixes = append(prefixes, item.Key)
				} else {
					objects = append(objects, item.Key)
				}
			}

			require.ElementsMatch(t, tc.Prefixes, prefixes)
			require.ElementsMatch(t, tc.Objects, objects)
		}
	})
}

func TestListObjectVersionsIsLatest(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
				config.Metainfo.UseListObjectsForListing = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := testrand.BucketName()

		require.NoError(t, planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName))

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		for range 3 {
			require.NoError(t, planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "foo/bar/A", testrand.Bytes(memory.KiB)))
		}
		for range 2 {
			require.NoError(t, planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "B", testrand.Bytes(memory.KiB)))
		}
		// recursive
		objs, more, err := object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{
			Recursive: true,
			System:    true,
			Custom:    true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, objs)
		require.False(t, more)

		var fooBarAObserved, BObserved bool
		for _, obj := range objs {
			switch obj.Key {
			case "foo/bar/A":
				if fooBarAObserved {
					require.False(t, obj.IsLatest)
				} else {
					require.True(t, obj.IsLatest)
					fooBarAObserved = true
				}
			case "B":
				if BObserved {
					require.False(t, obj.IsLatest)
				} else {
					require.True(t, obj.IsLatest)
					BObserved = true
				}
			default:
				require.Fail(t, "unexpected object", obj.Key)
			}
		}
		// recursive (limit check)
		objs, more, err = object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{
			Recursive: true,
			System:    true,
			Custom:    true,
			Limit:     4,
		})
		require.NoError(t, err)
		require.Len(t, objs, 4)
		require.True(t, more)
		// non-recursive (limit check)
		objs, more, err = object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{
			Prefix: "foo/bar/",
			System: true,
			Custom: true,
			Limit:  2,
		})
		require.NoError(t, err)
		require.Len(t, objs, 2)
		require.True(t, more)
		// non-recursive
		objs, more, err = object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{
			Prefix: "foo/bar/",
			System: true,
			Custom: true,
			Limit:  3,
		})
		require.NoError(t, err)
		require.Len(t, objs, 3)
		require.False(t, more)

		fooBarAObserved = false
		for _, obj := range objs {
			switch obj.Key {
			case "A":
				if fooBarAObserved {
					require.False(t, obj.IsLatest)
				} else {
					require.True(t, obj.IsLatest)
					fooBarAObserved = true
				}
			default:
				require.Fail(t, "unexpected object", obj.Key)
			}
		}
		// non-recursive
		objs, more, err = object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{
			System: true,
			Custom: true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, objs)
		require.False(t, more)

		BObserved = false
		for _, obj := range objs {
			switch obj.Key {
			case "B":
				if BObserved {
					require.False(t, obj.IsLatest)
				} else {
					require.True(t, obj.IsLatest)
					BObserved = true
				}
			case "foo/":
				require.True(t, obj.IsPrefix)
			default:
				require.Fail(t, "unexpected object", obj.Key)
			}
		}
		// non-recursive
		objs, more, err = object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{
			System: true,
			Custom: true,
			Limit:  1,
		})
		require.NoError(t, err)
		require.Len(t, objs, 1)
		require.True(t, more)
		nextCursor, nextVersion := objs[0].Key, objs[0].Version
		objs, more, err = object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{
			Cursor:        nextCursor,
			VersionCursor: nextVersion,
			System:        true,
			Custom:        true,
			Limit:         12,
		})
		require.NoError(t, err)
		require.NotEmpty(t, objs)
		require.False(t, more)

		if nextCursor == "B" {
			BObserved = true
		} else {
			BObserved = false
		}

		for _, obj := range objs {
			switch obj.Key {
			case "B":
				if BObserved {
					require.False(t, obj.IsLatest)
				} else {
					require.True(t, obj.IsLatest)
					BObserved = true
				}
			case "foo/":
				require.True(t, obj.IsPrefix)
			default:
				require.Fail(t, "unexpected object", obj.Key)
			}
		}
		// non-recursive
		objs, more, err = object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{
			Cursor: "B",
			System: true,
			Custom: true,
		})
		require.NoError(t, err)
		require.False(t, more)

		for _, obj := range objs {
			require.True(t, obj.IsPrefix)
		}
	})
}

func TestDeleteObjects(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.DeleteObjectsEnabled = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]

		bucketName := "test-bucket"
		err := planet.Uplinks[0].CreateBucket(ctx, sat, bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		require.NoError(t, bucket.SetBucketVersioning(ctx, project, bucketName, true))

		type minimalObject struct {
			key     string
			version []byte
		}

		createPrefixedObject := func(t *testing.T, prefix string) minimalObject {
			objectKey := prefix + testrand.Path()
			upload, err := object.UploadObject(ctx, project, bucketName, objectKey, nil)
			require.NoError(t, err)
			require.NoError(t, upload.Commit())
			return minimalObject{
				key:     objectKey,
				version: upload.Info().Version,
			}
		}

		createObject := func(t *testing.T) minimalObject {
			return createPrefixedObject(t, "")
		}

		getLastCommittedVersion := func(t *testing.T, bucketName string, objectKey string) *object.VersionedObject {
			versions, _, err := object.ListObjectVersions(ctx, project, bucketName, &object.ListObjectVersionsOptions{
				Cursor:        objectKey,
				VersionCursor: metabase.NewStreamVersionID(metabase.MaxVersion+1, uuid.UUID{}).Bytes(),
				Recursive:     true,
				Limit:         1,
			})
			require.NoError(t, err)
			require.NotEmpty(t, versions)
			require.Equal(t, versions[0].Key, objectKey)
			return versions[0]
		}

		t.Run("Basic", func(t *testing.T) {
			obj1 := createObject(t)
			obj2 := createObject(t)

			result, err := object.DeleteObjects(ctx, project, bucketName, []object.DeleteObjectsItem{
				{
					ObjectKey: obj1.key,
					Version:   obj1.version,
				},
				{
					ObjectKey: obj2.key,
				},
			}, nil)
			require.NoError(t, err)

			obj2Marker := getLastCommittedVersion(t, bucketName, obj2.key)
			require.True(t, obj2Marker.IsDeleteMarker)

			require.ElementsMatch(t, []object.DeleteObjectsResultItem{
				{
					ObjectKey:        obj1.key,
					RequestedVersion: obj1.version,
					Removed: &metaclient.DeleteObjectsResultItemRemoved{
						Version:     obj1.version,
						IsCommitted: true,
						IsVersioned: true,
					},
					Status: storj.DeleteObjectsStatusOK,
				},
				{
					ObjectKey: obj2.key,
					Marker: &metaclient.DeleteObjectsResultItemMarker{
						Version:     obj2Marker.Version,
						IsVersioned: true,
					},
					Status: storj.DeleteObjectsStatusOK,
				},
			}, result)
		})

		t.Run("Quiet mode", func(t *testing.T) {
			const prefix = "prefix/"

			access := planet.Uplinks[0].Access[sat.ID()]
			access, err := access.Share(uplink.FullPermission(), uplink.SharePrefix{
				Bucket: bucketName,
				Prefix: prefix,
			})
			require.NoError(t, err)

			project, err := uplink.OpenProject(ctx, access)
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			obj := createPrefixedObject(t, prefix)
			unauthorizedObj := createObject(t)
			notFoundObj := minimalObject{
				key:     prefix + testrand.Path(),
				version: randVersion(),
			}

			result, err := object.DeleteObjects(ctx, project, bucketName, []object.DeleteObjectsItem{
				{
					ObjectKey: obj.key,
					Version:   obj.version,
				},
				{
					ObjectKey: unauthorizedObj.key,
					Version:   unauthorizedObj.version,
				},
				{
					ObjectKey: notFoundObj.key,
					Version:   notFoundObj.version,
				},
			}, &metaclient.DeleteObjectsOptions{
				Quiet: true,
			})
			require.NoError(t, err)

			require.ElementsMatch(t, []object.DeleteObjectsResultItem{
				{
					ObjectKey:        unauthorizedObj.key,
					RequestedVersion: unauthorizedObj.version,
					Status:           storj.DeleteObjectsStatusUnauthorized,
				},
				{
					ObjectKey:        notFoundObj.key,
					RequestedVersion: notFoundObj.version,
					Status:           storj.DeleteObjectsStatusNotFound,
				},
			}, result)
		})

		t.Run("Invalid options", func(t *testing.T) {
			item := object.DeleteObjectsItem{
				ObjectKey: testrand.Path(),
				Version:   randVersion(),
			}

			test := func(t *testing.T, bucketName string, items []object.DeleteObjectsItem, expectedError error) {
				result, err := object.DeleteObjects(ctx, project, bucketName, items, nil)
				require.Empty(t, result)
				require.ErrorIs(t, err, expectedError)
			}

			t.Run("Missing bucket name", func(t *testing.T) {
				test(t, "", []object.DeleteObjectsItem{item}, object.ErrBucketNameMissing)
			})

			t.Run("Invalid bucket name", func(t *testing.T) {
				test(t, string(testrand.RandAlphaNumeric(64)), []object.DeleteObjectsItem{item}, uplink.ErrBucketNameInvalid)
			})

			t.Run("No items", func(t *testing.T) {
				test(t, bucketName, []object.DeleteObjectsItem{}, object.ErrDeleteObjectsNoItems)
			})

			t.Run("Too many items", func(t *testing.T) {
				items := make([]object.DeleteObjectsItem, 0, metabase.DeleteObjectsMaxItems+1)
				for range metabase.DeleteObjectsMaxItems + 1 {
					items = append(items, item)
				}
				test(t, bucketName, items, object.ErrDeleteObjectsTooManyItems)
			})

			t.Run("Missing object key", func(t *testing.T) {
				test(t, bucketName, []object.DeleteObjectsItem{{
					Version: randVersion(),
				}}, object.ErrObjectKeyMissing)
			})

			t.Run("Object key too long", func(t *testing.T) {
				objectKey := string(testrand.RandAlphaNumeric(sat.Config.Metainfo.MaxEncryptedObjectKeyLength + 1))
				test(t, bucketName, []object.DeleteObjectsItem{{
					ObjectKey: objectKey,
				}}, object.ErrObjectKeyTooLong)
			})

			t.Run("Invalid object version", func(t *testing.T) {
				test(t, bucketName, []object.DeleteObjectsItem{{
					ObjectKey: testrand.Path(),
					Version:   randVersion()[:8],
				}}, object.ErrObjectVersionInvalid)
			})
		})
	})
}

func TestDeleteObjectsUnimplemented(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.DeleteObjectsEnabled = false
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]

		project, err := planet.Uplinks[0].OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = object.DeleteObjects(ctx, project, testrand.BucketName(), []object.DeleteObjectsItem{{
			ObjectKey: testrand.Path(),
			Version:   randVersion(),
		}}, nil)

		require.ErrorIs(t, err, object.ErrDeleteObjectsUnimplemented)
	})
}

func TestConditionalWrites(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 1,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.UseBucketLevelObjectVersioning = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		upl := planet.Uplinks[0]

		project, err := planet.Uplinks[0].OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		unversionedBucket, versionedBucket := testrand.BucketName(), testrand.BucketName()
		testData := testrand.Bytes(5 * memory.KiB)
		testDataInline := testrand.Bytes(memory.B)

		require.NoError(t, upl.CreateBucket(ctx, sat, unversionedBucket))

		require.NoError(t, upl.CreateBucket(ctx, sat, versionedBucket))
		require.NoError(t, bucket.SetBucketVersioning(ctx, project, versionedBucket, true))

		runTest := func(name string, fn func(t *testing.T, bucket, key string, data []byte)) {
			for _, tc := range []struct {
				name, bucket string
				data         []byte
			}{
				{name: "unversioned bucket", bucket: unversionedBucket, data: testData},
				{name: "inline unversioned bucket", bucket: unversionedBucket, data: testDataInline},
				{name: "versioned bucket", bucket: versionedBucket, data: testData},
				{name: "inline versioned bucket", bucket: versionedBucket, data: testDataInline},
			} {
				t.Run(fmt.Sprintf("%s %s", name, tc.name), func(t *testing.T) {
					fn(t, tc.bucket, testrand.Path(), tc.data)
				})
			}
		}

		runTest("Unimplemented", func(t *testing.T, bucket, key string, data []byte) {
			opts := metaclient.UploadOptions{IfNoneMatch: []string{"something"}}

			_, err := upl.UploadWithOptions(ctx, sat, bucket, key, data, &opts)
			require.ErrorIs(t, err, object.ErrUnimplemented)
		})

		runTest("Upload", func(t *testing.T, bucket, key string, data []byte) {
			opts := metaclient.UploadOptions{IfNoneMatch: []string{"*"}}

			_, err := upl.UploadWithOptions(ctx, sat, bucket, key, data, &opts)
			require.NoError(t, err)

			_, err = upl.UploadWithOptions(ctx, sat, bucket, key, data, &opts)
			require.ErrorIs(t, err, object.ErrFailedPrecondition)

			require.NoError(t, upl.DeleteObject(ctx, sat, bucket, key))

			_, err = upl.UploadWithOptions(ctx, sat, bucket, key, data, &opts)
			require.NoError(t, err)
		})

		runTest("CopyObject", func(t *testing.T, bucket, key string, data []byte) {
			srcKey, dstKey := key, testrand.Path()
			copyOpts := object.CopyObjectOptions{IfNoneMatch: []string{"*"}}

			obj, err := upl.UploadWithOptions(ctx, sat, bucket, srcKey, testData, &metaclient.UploadOptions{
				IfNoneMatch: []string{"*"},
			})
			require.NoError(t, err)

			_, err = object.CopyObject(ctx, project, bucket, srcKey, obj.Version, bucket, dstKey, copyOpts)
			require.NoError(t, err)

			_, err = object.CopyObject(ctx, project, bucket, srcKey, obj.Version, bucket, dstKey, copyOpts)
			require.ErrorIs(t, err, object.ErrFailedPrecondition)

			require.NoError(t, upl.DeleteObject(ctx, sat, bucket, dstKey))

			_, err = object.CopyObject(ctx, project, bucket, srcKey, obj.Version, bucket, dstKey, copyOpts)
			require.NoError(t, err)
		})

		runTest("CommitUpload", func(t *testing.T, bucket, key string, data []byte) {
			opts := metaclient.CommitUploadOptions{IfNoneMatch: []string{"*"}}

			newUpload := func() uplink.UploadInfo {
				upload, err := multipart.BeginUpload(ctx, project, bucket, key, nil)
				require.NoError(t, err)

				part, err := project.UploadPart(ctx, bucket, key, upload.UploadID, 1)
				require.NoError(t, err)

				_, err = part.Write(data)
				require.NoError(t, err)
				require.NoError(t, part.Commit())

				return upload
			}

			upload := newUpload()

			_, err = object.CommitUpload(ctx, project, bucket, key, upload.UploadID, &opts)
			require.NoError(t, err)

			upload2, err := object.UploadObject(ctx, project, bucket, key, &object.UploadOptions{
				IfNoneMatch: []string{"*"},
			})
			require.NoError(t, err)
			err = upload2.Commit()
			require.ErrorIs(t, err, object.ErrFailedPrecondition)

			require.NoError(t, upl.DeleteObject(ctx, sat, bucket, key))

			upload = newUpload()

			_, err = object.CommitUpload(ctx, project, bucket, key, upload.UploadID, &opts)
			require.NoError(t, err)
		})
	})
}

func TestListObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = storj.EncNull
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		require.NoError(t, planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName))

		uploadKeys := []string{
			"foo/bar/A",
			"foo/bar/B",
			"foo/bar/C",
			"foo/bar/test/D",
			"foo/bar/test/E",
			"foo/bar/test/F",
			"foo/foo/G",
		}

		// shuffle the keys to ensure upload order doesn't affect the listing
		rand.Shuffle(len(uploadKeys), func(i, j int) { uploadKeys[i], uploadKeys[j] = uploadKeys[j], uploadKeys[i] })

		var sortedKeys []string
		for _, key := range uploadKeys {
			sortedKeys = append(sortedKeys, key)
			require.NoError(t, planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, key, testrand.Bytes(128)))
		}
		sort.Strings(sortedKeys)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		type testCase struct {
			Opts     object.ListObjectsOptions
			Prefixes []string
			Objects  []string
			Limit    int
		}

		for _, tc := range []testCase{
			{
				Opts:     object.ListObjectsOptions{Prefix: ""},
				Prefixes: []string{"foo/"},
			},
			{
				Opts:     object.ListObjectsOptions{Prefix: "foo"},
				Prefixes: []string{"/"},
			},
			{
				Opts:     object.ListObjectsOptions{Prefix: "foo/"},
				Prefixes: []string{"bar/", "foo/"},
			},
			{
				Opts:     object.ListObjectsOptions{Prefix: "foo/b"},
				Prefixes: []string{"ar/"},
			},
			{
				Opts:     object.ListObjectsOptions{Prefix: "foo/bar/"},
				Prefixes: []string{"test/"},
				Objects:  []string{"A", "B", "C"},
			},
			{
				Opts:    object.ListObjectsOptions{Prefix: "foo/bar/test/"},
				Objects: []string{"D", "E", "F"},
			},
			{
				Opts:    object.ListObjectsOptions{Prefix: "foo/bar/test/", Limit: 2},
				Objects: []string{"D", "E"},
			},
			{
				Opts:    object.ListObjectsOptions{Prefix: "", Recursive: true, Limit: 4},
				Objects: sortedKeys[:4],
			},
			{
				Opts:    object.ListObjectsOptions{Cursor: "foo/bar/B", Recursive: true, Limit: 4},
				Objects: sortedKeys[2:6],
			},
			{
				Opts:    object.ListObjectsOptions{Prefix: "foo/bar/tes", Recursive: true},
				Objects: []string{"t/D", "t/E", "t/F"},
			},
		} {
			result, _, err := object.ListObjects(ctx, project, bucketName, &tc.Opts)
			require.NoError(t, err)

			var objects, prefixes []string
			for _, item := range result {
				if item.IsPrefix {
					prefixes = append(prefixes, item.Key)
				} else {
					objects = append(objects, item.Key)
				}
			}

			require.ElementsMatch(t, tc.Prefixes, prefixes, "opts: %v", tc.Opts)
			require.ElementsMatch(t, tc.Objects, objects, "opts: %v", tc.Opts)
		}
	})
}

func TestListObjectWithPrefixError(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		require.NoError(t, planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName))

		require.NoError(t, planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "foo/bar/A", testrand.Bytes(128)))

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)
		_, _, err = object.ListObjects(ctx, project, bucketName, &object.ListObjectsOptions{Prefix: "foo"})
		require.Errorf(t, err, "prefix should end with slash")
		_, _, err = object.ListObjects(ctx, project, bucketName, &object.ListObjectsOptions{Prefix: "foo/"})
		require.NoError(t, err)
	})
}

func TestDownloadObject_DownloadSegment_ServerSideCopy(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = storj.EncNull
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		endpoint := planet.Satellites[0].API.Metainfo.Endpoint

		now := time.Now()

		require.NoError(t, planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], "test"))
		err := planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "test", "remote", testrand.Bytes(5*memory.KiB))
		require.NoError(t, err)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "test", "inline", testrand.Bytes(500))
		require.NoError(t, err)

		// we need stable identity to add it to trusted uplinks
		ident := planet.Uplinks[0].Identity

		chainPEM := bytes.NewBuffer([]byte{})
		require.NoError(t, pkcrypto.WriteCertPEM(chainPEM, ident.Chain()...))

		keyPEM := bytes.NewBuffer([]byte{})
		require.NoError(t, pkcrypto.WritePrivateKeyPEM(keyPEM, ident.Key))

		config := uplink.Config{
			ChainPEM: chainPEM.Bytes(),
			KeyPEM:   keyPEM.Bytes(),
		}

		project, err := config.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		t.Run("untrusted uplink", func(t *testing.T) {
			for _, objectKey := range []string{"remote", "inline"} {
				_, err = object.DownloadObject(ctx, project, "test", objectKey, nil, &object.DownloadObjectOptions{
					ServerSideCopy: true,
				})
				require.Error(t, err)
			}
		})
		t.Run("trusted uplink", func(t *testing.T) {
			endpoint.TestingAddTrustedUplink(ident.ID)

			for _, objectKey := range []string{"remote", "inline"} {
				func() {
					download, err := object.DownloadObject(ctx, project, "test", objectKey, nil, &object.DownloadObjectOptions{
						Offset:         0,
						Length:         400,
						ServerSideCopy: true,
					})
					require.NoError(t, err)
					defer ctx.Check(download.Close)

					data, err := io.ReadAll(download)
					require.NoError(t, err)
					require.Len(t, data, 400)
				}()
			}

		})

		for _, sn := range planet.StorageNodes {
			sn.Storage2.Orders.SendOrders(ctx, now.Add(24*time.Hour))
		}
		planet.Satellites[0].Orders.Chore.Loop.TriggerWait()

		usage, err := planet.Satellites[0].DB.ProjectAccounting().GetProjectTotal(ctx, planet.Uplinks[0].Projects[0].ID, now.Add(-time.Hour), now.Add(time.Hour))
		require.NoError(t, err)
		require.Zero(t, usage.Egress)
	})
}

func TestListObjectWithArbitraryPrefixMetadata(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = storj.EncNull
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		require.NoError(t, planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName))

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		objectKeys := []string{
			"prefix1filea.txt",
			"prefix1fileb.txt",
			"prefix2filec.txt",
			"prefix2filed.txt",
			"otherfile.txt",
		}

		metadata := uplink.CustomMetadata{
			"test-key": "test-value",
		}

		// Upload objects with custom metadata
		for _, key := range objectKeys {
			upload, err := project.UploadObject(ctx, bucketName, key, nil)
			require.NoError(t, err)

			err = upload.SetCustomMetadata(ctx, metadata)
			require.NoError(t, err)

			_, err = upload.Write(testrand.Bytes(100))
			require.NoError(t, err)

			err = upload.Commit()
			require.NoError(t, err)
		}

		// Test listing with arbitrary prefix - this should work correctly with EncNull
		// and test the metadata decryption
		result, _, err := object.ListObjects(ctx, project, bucketName, &object.ListObjectsOptions{
			Prefix:    "prefix1",
			Recursive: true,
			Custom:    true,
		})
		require.NoError(t, err)
		require.Len(t, result, 2)

		// Verify that objects can be listed and metadata decryption works
		for _, item := range result {
			require.Contains(t, []string{"filea.txt", "fileb.txt"}, item.Key)
			require.Equal(t, metadata, item.Custom)
			require.False(t, item.IsPrefix)
		}
	})
}
func TestUpdateMetadata(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = project.EnsureBucket(ctx, "testbucket")
		require.NoError(t, err)

		expected := testrand.Bytes(1 * memory.KiB)

		// upload object with no custom metadata
		upload, err := object.UploadObject(ctx, project, "testbucket", "obj", nil)
		require.NoError(t, err)
		_, err = upload.Write(expected)
		require.NoError(t, err)
		require.NoError(t, upload.SetETag(ctx, []byte("etag")))
		require.NoError(t, upload.Commit())

		// check that there is no custom metadata after the upload
		obj, err := object.StatObject(ctx, project, "testbucket", "obj", nil)
		require.NoError(t, err)
		require.Empty(t, obj.Custom)

		newMetadata := uplink.CustomMetadata{
			"key1": "value1",
			"key2": "value2",
		}

		// update the object's metadata
		err = object.UpdateObjectMetadata(ctx, project, "testbucket", "obj", newMetadata, &object.UpdateObjectMetadataOptions{
			ETag: obj.ETag,
		})
		require.NoError(t, err)

		// check that the metadata has been updated as expected
		statObj, err := object.StatObject(ctx, project, "testbucket", "obj", nil)
		require.NoError(t, err)
		require.EqualValues(t, []byte("etag"), statObj.ETag)
		require.Equal(t, newMetadata, statObj.Custom)

		// confirm that the object is still downloadable
		download, err := project.DownloadObject(ctx, "testbucket", "obj", nil)
		require.NoError(t, err)
		downloaded, err := io.ReadAll(download)
		require.NoError(t, err)
		require.NoError(t, download.Close())
		require.Equal(t, expected, downloaded)

		// remove the object's metadata
		err = object.UpdateObjectMetadata(ctx, project, "testbucket", "obj", nil, &object.UpdateObjectMetadataOptions{
			ETag: obj.ETag,
		})
		require.NoError(t, err)

		// check that the metadata has been removed
		statObj, err = object.StatObject(ctx, project, "testbucket", "obj", nil)
		require.NoError(t, err)
		require.EqualValues(t, []byte("etag"), statObj.ETag)
		require.Empty(t, statObj.Custom)

		// confirm that the object is still downloadable
		download, err = project.DownloadObject(ctx, "testbucket", "obj", nil)
		require.NoError(t, err)
		downloaded, err = io.ReadAll(download)
		require.NoError(t, err)
		require.NoError(t, download.Close())
		require.Equal(t, expected, downloaded)
	})
}

func TestETag(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		upload, err := object.UploadObject(ctx, project, bucketName, objectKey, nil)
		require.NoError(t, err)

		_, err = upload.Write([]byte("test1"))
		require.NoError(t, err)

		require.NoError(t, upload.SetETag(ctx, []byte("etag")))

		require.NoError(t, upload.Commit())

		require.Error(t, upload.SetETag(ctx, []byte("duplicate set")))

		statObj, err := object.StatObject(ctx, project, bucketName, objectKey, upload.Info().Version)
		require.NoError(t, err)

		require.EqualValues(t, []byte("etag"), statObj.ETag)

		objectKey2 := "test-object2"
		objectKey3 := "test-object3"

		{
			err = project.MoveObject(ctx, bucketName, objectKey, bucketName, objectKey2, nil)
			require.NoError(t, err)

			stat, err := object.StatObject(ctx, project, bucketName, objectKey2, upload.Info().Version)
			require.NoError(t, err)

			require.EqualValues(t, []byte("etag"), stat.ETag)
		}

		{
			_, err = project.CopyObject(ctx, bucketName, objectKey2, bucketName, objectKey3, nil)
			require.NoError(t, err)

			stat, err := object.StatObject(ctx, project, bucketName, objectKey3, upload.Info().Version)
			require.NoError(t, err)

			require.EqualValues(t, []byte("etag"), stat.ETag)
		}

		entries, _, err := object.ListObjects(ctx, project, bucketName, &object.ListObjectsOptions{
			System:       true,
			Custom:       true,
			ETag:         true,
			ETagOrCustom: false,
		})
		require.NoError(t, err)
		for _, entry := range entries {
			require.EqualValues(t, []byte("etag"), entry.ETag)
		}

		entries, _, err = object.ListObjects(ctx, project, bucketName, &object.ListObjectsOptions{
			System:       false,
			Custom:       false,
			ETag:         true,
			ETagOrCustom: false,
		})
		require.NoError(t, err)
		for _, entry := range entries {
			require.EqualValues(t, []byte("etag"), entry.ETag)
		}

		entries, _, err = object.ListObjects(ctx, project, bucketName, &object.ListObjectsOptions{
			System:       false,
			Custom:       false,
			ETag:         false,
			ETagOrCustom: true,
		})
		require.NoError(t, err)
		for _, entry := range entries {
			require.EqualValues(t, []byte("etag"), entry.ETag)
		}
	})
}

func TestListObjects_ETagOrCustom(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		bucketName := "test-bucket"
		objectKey := "test-object"
		err := planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		upload, err := object.UploadObject(ctx, project, bucketName, objectKey, nil)
		require.NoError(t, err)

		_, err = upload.Write([]byte("test1"))
		require.NoError(t, err)

		custom := uplink.CustomMetadata{
			"etag": "etag",
			"123":  "123",
		}

		err = upload.SetCustomMetadata(ctx, custom)
		require.NoError(t, err)

		require.NoError(t, upload.Commit())

		statObj, err := object.StatObject(ctx, project, bucketName, objectKey, upload.Info().Version)
		require.NoError(t, err)

		require.EqualValues(t, custom, statObj.Custom)

		entries, _, err := object.ListObjects(ctx, project, bucketName, &object.ListObjectsOptions{
			System:       true,
			Custom:       false,
			ETagOrCustom: true,
		})
		require.NoError(t, err)
		for _, entry := range entries {
			require.Nil(t, entry.ETag)
			require.EqualValues(t, custom, entry.Custom)
		}

		entries, _, err = object.ListObjects(ctx, project, bucketName, &object.ListObjectsOptions{
			System:       true,
			Custom:       true,
			ETagOrCustom: true,
		})
		require.NoError(t, err)
		for _, entry := range entries {
			require.Nil(t, entry.ETag)
			require.EqualValues(t, custom, entry.Custom)
		}

		entries, _, err = object.ListObjects(ctx, project, bucketName, &object.ListObjectsOptions{
			System:       false,
			Custom:       true,
			ETagOrCustom: true,
		})
		require.NoError(t, err)
		for _, entry := range entries {
			require.Nil(t, entry.ETag)
			require.EqualValues(t, custom, entry.Custom)
		}

		entries, _, err = object.ListObjects(ctx, project, bucketName, &object.ListObjectsOptions{
			System:       false,
			Custom:       false,
			ETagOrCustom: true,
		})
		require.NoError(t, err)
		for _, entry := range entries {
			require.Nil(t, entry.ETag)
			require.EqualValues(t, custom, entry.Custom)
		}

	})
}

func TestListObjectsDelimiter(t *testing.T) {
	testListObjectsDelimiter(t, func(ctx context.Context, project *uplink.Project, params testListObjectsDelimiterParams) ([]*object.VersionedObject, error) {
		objects, _, err := object.ListObjects(ctx, project, params.bucketName, &object.ListObjectsOptions{
			Prefix:    params.prefix,
			Delimiter: params.delimiter,
			Recursive: params.recursive,
		})
		return objects, err
	})
}

func TestListObjectVersionsDelimiter(t *testing.T) {
	testListObjectsDelimiter(t, func(ctx context.Context, project *uplink.Project, params testListObjectsDelimiterParams) ([]*object.VersionedObject, error) {
		objects, _, err := object.ListObjectVersions(ctx, project, params.bucketName, &object.ListObjectVersionsOptions{
			Prefix:    params.prefix,
			Delimiter: params.delimiter,
			Recursive: params.recursive,
		})
		return objects, err
	})
}

type testListObjectsDelimiterParams struct {
	bucketName string
	prefix     string
	delimiter  string
	recursive  bool
}

func testListObjectsDelimiter(t *testing.T, fn func(ctx context.Context, project *uplink.Project, params testListObjectsDelimiterParams) ([]*object.VersionedObject, error)) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Uplink: func(log *zap.Logger, index int, config *testplanet.UplinkConfig) {
				config.DefaultPathCipher = storj.EncNull
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		const (
			delimiter        = "###"
			defaultDelimiter = "/"
		)

		sat := planet.Satellites[0]
		up := planet.Uplinks[0]
		bucketName := testrand.BucketName()

		require.NoError(t, up.CreateBucket(ctx, planet.Satellites[0], bucketName))

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		objects := make(map[string]*object.VersionedObject)

		for _, objectKey := range []string{
			"abc" + delimiter,
			"abc" + delimiter + "def",
			"abc" + delimiter + "def" + delimiter + "ghi",
			"abc" + defaultDelimiter + "def",
			"xyz" + delimiter + "uvw",
		} {
			obj := metabasetest.CreateObject(ctx, t, sat.Metabase.DB, metabase.ObjectStream{
				ProjectID:  up.Projects[0].ID,
				BucketName: metabase.BucketName(bucketName),
				ObjectKey:  metabase.ObjectKey(objectKey),
				Version:    1,
				StreamID:   testrand.UUID(),
			}, 0)

			objects[objectKey] = &object.VersionedObject{
				Object: uplink.Object{
					Key:    string(obj.ObjectKey),
					System: uplink.SystemMetadata{},
					Custom: uplink.CustomMetadata{},
				},
				Version:  obj.StreamVersionID().Bytes(),
				IsLatest: true,
			}
		}

		prefixEntry := func(objectKey string) *object.VersionedObject {
			return &object.VersionedObject{
				Object: uplink.Object{
					Key:      objectKey,
					System:   uplink.SystemMetadata{},
					Custom:   uplink.CustomMetadata{},
					IsPrefix: true,
				},
				Version: make([]byte, 16),
			}
		}

		withoutPrefix := func(prefix string, obj *object.VersionedObject) *object.VersionedObject {
			newObj := *obj
			newObj.Object.Key = newObj.Object.Key[len(prefix):]
			return &newObj
		}

		t.Run("Default delimiter", func(t *testing.T) {
			objectList, err := fn(ctx, project, testListObjectsDelimiterParams{
				bucketName: bucketName,
				prefix:     "",
				delimiter:  "",
			})
			require.NoError(t, err)

			require.Equal(t, []*object.VersionedObject{
				objects["abc"+delimiter],
				objects["abc"+delimiter+"def"],
				objects["abc"+delimiter+"def"+delimiter+"ghi"],
				prefixEntry("abc" + defaultDelimiter),
				objects["xyz"+delimiter+"uvw"],
			}, objectList)
		})

		t.Run("Root", func(t *testing.T) {
			objectList, err := fn(ctx, project, testListObjectsDelimiterParams{
				bucketName: bucketName,
				prefix:     "",
				delimiter:  delimiter,
			})
			require.NoError(t, err)

			require.Equal(t, []*object.VersionedObject{
				prefixEntry("abc" + delimiter),
				objects["abc"+defaultDelimiter+"def"],
				prefixEntry("xyz" + delimiter),
			}, objectList)
		})

		t.Run("1 level deep", func(t *testing.T) {
			objectList, err := fn(ctx, project, testListObjectsDelimiterParams{
				bucketName: bucketName,
				prefix:     "abc" + delimiter,
				delimiter:  delimiter,
			})
			require.NoError(t, err)

			require.Equal(t, []*object.VersionedObject{
				withoutPrefix("abc"+delimiter, objects["abc"+delimiter]),
				withoutPrefix("abc"+delimiter, objects["abc"+delimiter+"def"]),
				prefixEntry("def" + delimiter),
			}, objectList)
		})

		t.Run("2 levels deep", func(t *testing.T) {
			objectList, err := fn(ctx, project, testListObjectsDelimiterParams{
				bucketName: bucketName,
				prefix:     "abc" + delimiter + "def" + delimiter,
				delimiter:  delimiter,
			})
			require.NoError(t, err)

			require.Equal(t, []*object.VersionedObject{
				withoutPrefix(
					"abc"+delimiter+"def"+delimiter,
					objects["abc"+delimiter+"def"+delimiter+"ghi"],
				),
			}, objectList)
		})

		t.Run("Prefix suffixed with partial delimiter", func(t *testing.T) {
			partialDelimiter := delimiter[:len(delimiter)-1]
			remainingDelimiter := delimiter[len(delimiter)-1:]

			objectList, err := fn(ctx, project, testListObjectsDelimiterParams{
				bucketName: bucketName,
				prefix:     "abc" + partialDelimiter,
				delimiter:  delimiter,
			})
			require.NoError(t, err)

			require.Equal(t, []*object.VersionedObject{
				withoutPrefix("abc"+partialDelimiter, objects["abc"+delimiter]),
				withoutPrefix("abc"+partialDelimiter, objects["abc"+delimiter+"def"]),
				prefixEntry(remainingDelimiter + "def" + delimiter),
			}, objectList)
		})

		t.Run("Recursive with delimiter", func(t *testing.T) {
			// Ensure that the delimiter has no effect if recursive listing was requested.
			objectList, err := fn(ctx, project, testListObjectsDelimiterParams{
				bucketName: bucketName,
				prefix:     "",
				delimiter:  delimiter,
				recursive:  true,
			})
			require.NoError(t, err)

			require.Equal(t, []*object.VersionedObject{
				objects["abc"+delimiter],
				objects["abc"+delimiter+"def"],
				objects["abc"+delimiter+"def"+delimiter+"ghi"],
				objects["abc"+defaultDelimiter+"def"],
				objects["xyz"+delimiter+"uvw"],
			}, objectList)
		})

		t.Run("Unsupported cipher", func(t *testing.T) {
			encAccess := grant.NewEncryptionAccessWithDefaultKey(&storj.Key{})
			encAccess.SetDefaultPathCipher(storj.EncAESGCM)

			grantAccess := grant.Access{
				SatelliteAddress: sat.URL(),
				APIKey:           up.APIKey[sat.ID()],
				EncAccess:        encAccess,
			}

			serializedAccess, err := grantAccess.Serialize()
			require.NoError(t, err)

			access, err := uplink.ParseAccess(serializedAccess)
			require.NoError(t, err)

			project, err := uplink.OpenProject(ctx, access)
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			_, err = fn(ctx, project, testListObjectsDelimiterParams{
				bucketName: bucketName,
				prefix:     "",
				delimiter:  delimiter,
			})
			require.ErrorIs(t, err, object.ErrUnsupportedDelimiter)
		})
	})
}

func randVersion() []byte {
	randVersionID := metabase.Version(testrand.Int63n(int64(metabase.MaxVersion-1)) + 1)
	return metabase.NewStreamVersionID(randVersionID, testrand.UUID()).Bytes()
}
