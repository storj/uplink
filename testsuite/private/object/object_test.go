// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package object_test

import (
	"io"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/buckets"
	"storj.io/uplink"
	"storj.io/uplink/private/bucket"
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

		deleteObj, err := object.DeleteObject(ctx, project, bucketName, uploadInfoA2.Key, uploadInfoA2.Version)
		require.NoError(t, err)
		require.NotEmpty(t, deleteObj.Version)
		// delete was done with specified version so no delete marker should be created
		require.False(t, deleteObj.IsDeleteMarker)

		// delete non existing version of existing object
		nonExistingVersion := slices.Clone(uploadInfoB.Version)
		nonExistingVersion[0]++ // change oriinal version
		deleteObj, err = object.DeleteObject(ctx, project, bucketName, uploadInfoB.Key, nonExistingVersion)
		require.NoError(t, err)
		require.Nil(t, deleteObj)

		// delete latest version with version nil
		deleteObj, err = object.DeleteObject(ctx, project, bucketName, uploadInfoB.Key, nil)
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
		versionignState, err := bucket.GetBucketVersioning(ctx, project, bucketName)
		require.NoError(t, err)
		require.Equal(t, buckets.VersioningSuspended, buckets.Versioning(versionignState))

		// upload unversioned object in suspended bucket. should overwright previous unversioned object
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "objectA", testrand.Bytes(100))
		require.NoError(t, err)

		items, _, err := object.ListObjectVersions(ctx, project, bucketName, nil)
		require.NoError(t, err)
		require.Len(t, items, 2)

		// with listing version should be always set
		for _, item := range items {
			require.NotEmpty(t, item.Version)
		}
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

		// upload versioned object
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], bucketName, "objectA", testrand.Bytes(100))
		require.NoError(t, err)

		stat, err = object.StatObject(ctx, project, bucketName, "objectA", nil)
		require.NoError(t, err)
		require.NotEmpty(t, stat.Version)

		items, _, err := object.ListObjectVersions(ctx, project, bucketName, nil)
		require.NoError(t, err)
		require.Len(t, items, 2)

		// with listing version should be always set
		for _, item := range items {
			require.NotEmpty(t, item.Version)
		}
	})
}
