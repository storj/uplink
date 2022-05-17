// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
	"storj.io/uplink/private/testuplink"
)

func TestCopyObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		newCtx := testuplink.WithMaxSegmentSize(ctx, 10*memory.KiB)
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")
		createBucket(t, ctx, project, "new-testbucket")

		testCases := []struct {
			Bucket     string
			Key        string
			NewBucket  string
			NewKey     string
			ObjectSize memory.Size
		}{
			// the same bucket
			{"testbucket", "empty", "testbucket", "new-empty", 0},
			{"testbucket", "inline", "testbucket", "new-inline", memory.KiB},
			{"testbucket", "remote", "testbucket", "new-remote", 9 * memory.KiB},
			// one remote segment and one inline segment
			{"testbucket", "remote-segment-size", "testbucket", "new-remote-segment-size", 10 * memory.KiB},
			{"testbucket", "remote+inline", "testbucket", "new-remote+inline", 11 * memory.KiB},
			//// 3 remote segment
			{"testbucket", "multiple-remote-segments", "testbucket", "new-multiple-remote-segments", 29 * memory.KiB},
			{"testbucket", "remote-with-prefix", "testbucket", "a/prefix/remote-with-prefix", 9 * memory.KiB},
			//
			//// different bucket
			{"testbucket", "empty", "new-testbucket", "new-empty", 0},
			{"testbucket", "inline", "new-testbucket", "new-inline", memory.KiB},
			{"testbucket", "remote", "new-testbucket", "new-remote", 9 * memory.KiB},
			//// one remote segment and one inline segment
			{"testbucket", "remote-segment-size", "new-testbucket", "new-remote-segment-size", 10 * memory.KiB},
			{"testbucket", "remote+inline", "new-testbucket", "new-remote+inline", 11 * memory.KiB},
			// 3 remote segment
			{"testbucket", "multiple-remote-segments", "new-testbucket", "new-multiple-remote-segments", 29 * memory.KiB},
			{"testbucket", "remote-with-prefix", "new-testbucket", "a/prefix/remote-with-prefix", 9 * memory.KiB},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.Key, func(t *testing.T) {
				expectedData := testrand.Bytes(tc.ObjectSize)
				err := planet.Uplinks[0].Upload(newCtx, planet.Satellites[0], tc.Bucket, tc.Key, expectedData)
				require.NoError(t, err)

				obj, err := project.StatObject(ctx, tc.Bucket, tc.Key)
				require.NoError(t, err)
				assertObject(t, obj, tc.Key)

				copyObject, err := project.CopyObject(ctx, tc.Bucket, tc.Key, tc.NewBucket, tc.NewKey, nil)
				require.NoError(t, err)

				data, err := planet.Uplinks[0].Download(newCtx, planet.Satellites[0], tc.NewBucket, tc.NewKey)
				require.NoError(t, err)
				require.Equal(t, expectedData, data)

				statCopyObject, err := project.StatObject(ctx, tc.NewBucket, tc.NewKey)
				require.NoError(t, err)

				// for easy compare
				{
					obj.Key = tc.NewKey
					obj.System.Created = time.Time{}
					copyObject.System.Created = time.Time{}
					statCopyObject.System.Created = time.Time{}
				}

				// compare original object with copied, result from copy operation
				require.Equal(t, obj, statCopyObject)

				// compare original object with copied, stat request
				require.Equal(t, obj, copyObject)

				// verify that original object still exists
				_, err = project.StatObject(ctx, tc.Bucket, tc.Key)
				require.NoError(t, err)
			})
		}
	})
}

func TestDeleteCopiedObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		newCtx := testuplink.WithMaxSegmentSize(ctx, 10*memory.KiB)
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "ancestor-bucket")
		createBucket(t, ctx, project, "copy-bucket")

		testCases := []struct {
			Key              string
			CopyKey          string
			CopyOfCopyKey    string
			BucketToDelete   string
			KeyToDelete      string
			BucketToDownload string
			KeyToDownload    string
			ObjectSize       memory.Size
		}{
			{"inline", "copy-inline", "copy2-inline", "copy-bucket", "copy-inline", "ancestor-bucket", "inline", memory.KiB},
			{"remote", "copy-remote", "copy2-remote", "copy-bucket", "copy-remote", "ancestor-bucket", "remote", 9 * memory.KiB},
			{"inline2", "copy-inline2", "copy2-inline2", "ancestor-bucket", "inline2", "copy-bucket", "copy-inline2", memory.KiB},
			{"remote2", "copy-remote2", "copy2-remote2", "ancestor-bucket", "remote2", "copy-bucket", "copy-remote2", 9 * memory.KiB},
			{"inline3", "copy-inline3", "copy2-inline3", "copy-bucket", "copy-inline3", "copy-bucket", "copy2-inline3", memory.KiB},
			{"remote3", "copy-remote3", "copy2-remote3", "copy-bucket", "copy-remote3", "copy-bucket", "copy2-remote3", 9 * memory.KiB},
			{"inline4", "copy-inline4", "copy2-inline4", "ancestor-bucket", "inline4", "copy-bucket", "copy2-inline4", memory.KiB},
			{"remote4", "copy-remote4", "copy2-remote4", "ancestor-bucket", "remote4", "copy-bucket", "copy2-remote4", 9 * memory.KiB},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run("delete "+tc.KeyToDelete+", download"+tc.KeyToDownload, func(t *testing.T) {
				testData := testrand.Bytes(10 * memory.KiB)
				err := planet.Uplinks[0].Upload(newCtx, planet.Satellites[0], "ancestor-bucket", tc.Key, testData)
				require.NoError(t, err)

				obj, err := project.StatObject(ctx, "ancestor-bucket", tc.Key)
				require.NoError(t, err)
				assertObject(t, obj, tc.Key)

				_, err = project.CopyObject(ctx, "ancestor-bucket", tc.Key, "copy-bucket", tc.CopyKey, nil)
				require.NoError(t, err)

				copiedObj, err := project.StatObject(ctx, "copy-bucket", tc.CopyKey)
				require.NoError(t, err)
				assertObject(t, copiedObj, tc.CopyKey)

				_, err = project.CopyObject(ctx, "copy-bucket", tc.CopyKey, "copy-bucket", tc.CopyOfCopyKey, nil)
				require.NoError(t, err)

				secondCopiedBbj, err := project.StatObject(ctx, "copy-bucket", tc.CopyOfCopyKey)
				require.NoError(t, err)
				assertObject(t, secondCopiedBbj, tc.CopyOfCopyKey)

				_, err = project.DeleteObject(ctx, tc.BucketToDelete, tc.KeyToDelete)
				require.NoError(t, err)

				data, err := planet.Uplinks[0].Download(newCtx, planet.Satellites[0], tc.BucketToDownload, tc.KeyToDownload)
				require.NoError(t, err)
				require.Equal(t, testData, data)
			})
		}
	})
}

func TestCopyObject_Errors(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1,
		UplinkCount:    1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		_, err := project.CopyObject(ctx, "", "", "", "", nil)
		require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

		_, err = project.CopyObject(ctx, "test", "test", "", "", nil)
		require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

		_, err = project.CopyObject(ctx, "test", "", "", "", nil)
		require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

		_, err = project.CopyObject(ctx, "test", "key", "test", "", nil)
		require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

		_, err = project.CopyObject(ctx, "invalid_!?", "test", "invalid_!?", "new-test", nil)
		require.Error(t, err) // check how we can return uplink.ErrBucketNameInvalid here

		createBucket(t, ctx, project, "testbucket")

		_, err = project.CopyObject(ctx, "testbucket", "key", "testbucket", "new-key", nil)
		require.True(t, errors.Is(err, uplink.ErrObjectNotFound))

		// move to non existing bucket
		_, err = project.CopyObject(ctx, "testbucket", "key", "non-existing-bucket", "key", nil)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))
		require.Contains(t, err.Error(), "(\"non-existing-bucket\")")

		_, err = project.CopyObject(ctx, "testbucket", "prefix/", "testbucket", "new-key", nil)
		require.Error(t, err)

		_, err = project.CopyObject(ctx, "testbucket", "key", "testbucket", "prefix/", nil)
		require.Error(t, err)

		// moving not committed objects is not allowed, object will be not found
		_, err = project.BeginUpload(ctx, "testbucket", "multipart", nil)
		require.NoError(t, err)

		_, err = project.CopyObject(ctx, "testbucket", "multipart", "testbucket", "new-multipart", nil)
		require.True(t, errors.Is(err, uplink.ErrObjectNotFound))

		// moving object to key where different object exists should end with error
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "testbucket", "objectA", testrand.Bytes(memory.KiB))
		require.NoError(t, err)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "testbucket", "objectB", testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		// overwriting of existing object is possible
		_, err = project.CopyObject(ctx, "testbucket", "objectA", "testbucket", "objectB", nil)
		require.NoError(t, err)

		// TODO add test cases for lack of access to target location
	})
}

func TestCopyObject_Metadata(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1,
		UplinkCount:    1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		_, err := project.CreateBucket(ctx, "bucket")
		require.NoError(t, err)

		expectedMetadata := map[string]uplink.CustomMetadata{
			"empty": {},
			"not empty": {
				"key": "value",
			},
		}

		for name, metadata := range expectedMetadata {
			t.Run("metadata "+name+"/standard upload", func(t *testing.T) {
				upload, err := project.UploadObject(ctx, "bucket", "non-mpu-"+name, nil)
				require.NoError(t, err)

				_, err = upload.Write(testrand.Bytes(256))
				require.NoError(t, err)
				require.NoError(t, upload.SetCustomMetadata(ctx, metadata))
				require.NoError(t, upload.Commit())

				copiedObject, err := project.CopyObject(ctx, "bucket", "non-mpu-"+name, "bucket", "copy-non-mpu-"+name, nil)
				require.NoError(t, err)
				require.Equal(t, metadata, copiedObject.Custom)

				copiedObject, err = project.StatObject(ctx, "bucket", "copy-non-mpu-"+name)
				require.NoError(t, err)
				require.Equal(t, metadata, copiedObject.Custom)
			})
			t.Run("metadata "+name+"/MPU upload", func(t *testing.T) {
				upload, err := project.BeginUpload(ctx, "bucket", "mpu-"+name, nil)
				require.NoError(t, err)

				uploadPart, err := project.UploadPart(ctx, "bucket", "mpu-"+name, upload.UploadID, 1)
				require.NoError(t, err)

				_, err = uploadPart.Write(testrand.Bytes(256))
				require.NoError(t, err)
				require.NoError(t, uploadPart.Commit())

				_, err = project.CommitUpload(ctx, "bucket", "mpu-"+name, upload.UploadID, &uplink.CommitUploadOptions{
					CustomMetadata: metadata,
				})
				require.NoError(t, err)

				copiedObject, err := project.CopyObject(ctx, "bucket", "mpu-"+name, "bucket", "copy-mpu-"+name, nil)
				require.NoError(t, err)
				require.Equal(t, metadata, copiedObject.Custom)

				copiedObject, err = project.StatObject(ctx, "bucket", "copy-mpu-"+name)
				require.NoError(t, err)
				require.Equal(t, metadata, copiedObject.Custom)
			})
		}

		// verify that if metadata is set then key and nonce are also set
		objects, err := planet.Satellites[0].Metabase.DB.TestingAllObjects(ctx)
		require.NoError(t, err)
		require.Len(t, objects, len(expectedMetadata)*4) // two cases, each two copies

		for _, object := range objects {
			if len(object.EncryptedMetadata) == 0 {
				require.Empty(t, object.EncryptedMetadataEncryptedKey)
				require.Empty(t, object.EncryptedMetadataNonce)
			} else {
				require.NotEmpty(t, object.EncryptedMetadataEncryptedKey)
				require.NotEmpty(t, object.EncryptedMetadataNonce)
			}
		}
	})
}

func TestCopyObjectAndMoveObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		objects := map[string]memory.Size{
			"inline": 1 * memory.KiB,
			"remote": 10 * memory.KiB,
		}

		for name, size := range objects {
			expectedData := testrand.Bytes(size)

			t.Run(name+"/move copy", func(t *testing.T) {
				_, _ = project.DeleteBucketWithObjects(ctx, "bucket") // ignore error, its just cleanup

				expectedData := testrand.Bytes(size)
				err := planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "bucket", "ancestor"+name, expectedData)
				require.NoError(t, err)

				_, err = project.CopyObject(ctx, "bucket", "ancestor"+name, "bucket", "copy"+name, nil)
				require.NoError(t, err)

				err = project.MoveObject(ctx, "bucket", "copy"+name, "bucket", "moved copy"+name, nil)
				require.NoError(t, err)

				data, err := planet.Uplinks[0].Download(ctx, planet.Satellites[0], "bucket", "moved copy"+name)
				require.NoError(t, err)
				require.Equal(t, expectedData, data)
			})

			t.Run(name+"/move ancestor", func(t *testing.T) {
				_, _ = project.DeleteBucketWithObjects(ctx, "bucket") // ignore error, its just cleanup

				expectedData := testrand.Bytes(size)
				err := planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "bucket", "ancestor", expectedData)
				require.NoError(t, err)

				_, err = project.CopyObject(ctx, "bucket", "ancestor", "bucket", "copy", nil)
				require.NoError(t, err)

				err = project.MoveObject(ctx, "bucket", "ancestor", "bucket", "moved ancestor", nil)
				require.NoError(t, err)

				data, err := planet.Uplinks[0].Download(ctx, planet.Satellites[0], "bucket", "copy")
				require.NoError(t, err)
				require.Equal(t, expectedData, data)

				data, err = planet.Uplinks[0].Download(ctx, planet.Satellites[0], "bucket", "moved ancestor")
				require.NoError(t, err)
				require.Equal(t, expectedData, data)
			})

			t.Run(name+"/copy moved object", func(t *testing.T) {
				_, _ = project.DeleteBucketWithObjects(ctx, "bucket") // ignore error, its just cleanup

				err := planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "bucket", "ancestor", expectedData)
				require.NoError(t, err)

				err = project.MoveObject(ctx, "bucket", "ancestor", "bucket", "moved ancestor", nil)
				require.NoError(t, err)

				_, err = project.CopyObject(ctx, "bucket", "moved ancestor", "bucket", "moved ancestor copy", nil)
				require.NoError(t, err)

				data, err := planet.Uplinks[0].Download(ctx, planet.Satellites[0], "bucket", "moved ancestor copy")
				require.NoError(t, err)
				require.Equal(t, expectedData, data)

				data, err = planet.Uplinks[0].Download(ctx, planet.Satellites[0], "bucket", "moved ancestor")
				require.NoError(t, err)
				require.Equal(t, expectedData, data)
			})
		}
	})
}

func TestDeleteCopiesBetweenBuckets(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		_, err := project.EnsureBucket(ctx, "source-bucket")
		require.NoError(t, err)
		_, err = project.EnsureBucket(ctx, "destination-bucket-01")
		require.NoError(t, err)
		_, err = project.EnsureBucket(ctx, "destination-bucket-02")
		require.NoError(t, err)

		assertDownload := func(bucket, key string, expectedData []byte) {
			data, err := planet.Uplinks[0].Download(ctx, planet.Satellites[0], bucket, key)
			require.NoError(t, err)
			require.Equal(t, expectedData, data)
		}

		expectedData := testrand.Bytes(50 * memory.KiB)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "source-bucket", "test", expectedData)
		require.NoError(t, err)

		_, err = project.CopyObject(ctx, "source-bucket", "test", "source-bucket", "test-copy", nil)
		require.NoError(t, err)

		_, err = project.CopyObject(ctx, "source-bucket", "test", "destination-bucket-01", "test", nil)
		require.NoError(t, err)

		_, err = project.CopyObject(ctx, "source-bucket", "test", "destination-bucket-02", "test", nil)
		require.NoError(t, err)

		// copy of copy
		_, err = project.CopyObject(ctx, "destination-bucket-02", "test", "destination-bucket-02", "test-copy", nil)
		require.NoError(t, err)

		// we have now:
		//    source-bucket/test
		//    source-bucket/test-copy
		//    destination-bucket-01/test
		//    destination-bucket-02/test
		//    destination-bucket-02/test-copy

		assertDownload("source-bucket", "test-copy", expectedData)
		assertDownload("destination-bucket-01", "test", expectedData)
		assertDownload("destination-bucket-02", "test", expectedData)
		assertDownload("destination-bucket-02", "test-copy", expectedData)

		_, err = project.DeleteBucketWithObjects(ctx, "source-bucket")
		require.NoError(t, err)

		assertDownload("destination-bucket-01", "test", expectedData)
		assertDownload("destination-bucket-02", "test", expectedData)
		assertDownload("destination-bucket-02", "test-copy", expectedData)

		_, err = project.DeleteBucketWithObjects(ctx, "destination-bucket-01")
		require.NoError(t, err)

		assertDownload("destination-bucket-02", "test", expectedData)
		assertDownload("destination-bucket-02", "test-copy", expectedData)

		_, err = project.DeleteBucketWithObjects(ctx, "destination-bucket-02")
		require.NoError(t, err)

		segments, err := planet.Satellites[0].Metabase.DB.TestingAllSegments(ctx)
		require.NoError(t, err)
		require.Empty(t, segments)
	})
}

func TestOverwriteExistingCopyDestination(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		_, err := project.EnsureBucket(ctx, "source-bucket")
		require.NoError(t, err)

		sizes := map[string]memory.Size{
			"inline": 2 * memory.KiB,
			"remote": 11 * memory.KiB,
		}

		// TODO add more cases like, like copies between buckets, copies of copies

		for name, size := range sizes {
			t.Run(name, func(t *testing.T) {
				expectedData := testrand.Bytes(size)
				err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "source-bucket", name, expectedData)
				require.NoError(t, err)

				// input data should be different from ancestor to be sure we replaced it with copy
				err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "source-bucket", name+"-existing", testrand.Bytes(size))
				require.NoError(t, err)

				_, err = project.CopyObject(ctx, "source-bucket", name, "source-bucket", name+"-existing", nil)
				require.NoError(t, err)

				data, err := planet.Uplinks[0].Download(ctx, planet.Satellites[0], "source-bucket", name)
				require.NoError(t, err)
				require.Equal(t, expectedData, data)

				data, err = planet.Uplinks[0].Download(ctx, planet.Satellites[0], "source-bucket", name+"-existing")
				require.NoError(t, err)
				require.Equal(t, expectedData, data)
			})
		}
	})
}

func TestOverwriteCopyToSource(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		_, err := project.EnsureBucket(ctx, "source-bucket")
		require.NoError(t, err)

		sizes := map[string]memory.Size{
			"empty":  0,
			"inline": 2 * memory.KiB,
			"remote": 11 * memory.KiB,
		}

		for name, size := range sizes {
			t.Run(name, func(t *testing.T) {
				expectedData := testrand.Bytes(size)
				err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "source-bucket", name, expectedData)
				require.NoError(t, err)

				_, err = project.CopyObject(ctx, "source-bucket", name, "source-bucket", name+"-copy", nil)
				require.NoError(t, err)

				_, err = project.CopyObject(ctx, "source-bucket", name+"-copy", "source-bucket", name, nil)
				require.NoError(t, err)

				data, err := planet.Uplinks[0].Download(ctx, planet.Satellites[0], "source-bucket", name)
				require.NoError(t, err)
				require.Equal(t, expectedData, data)

				data, err = planet.Uplinks[0].Download(ctx, planet.Satellites[0], "source-bucket", name+"-copy")
				require.NoError(t, err)
				require.Equal(t, expectedData, data)
			})
		}
	})
}
