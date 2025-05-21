// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
	"storj.io/uplink/private/testuplink"
)

func TestMoveObject(t *testing.T) {
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

		type testCase struct {
			Bucket     string
			Key        string
			NewBucket  string
			NewKey     string
			ObjectSize memory.Size
		}

		testCases := []testCase{
			// the same bucket
			{"testbucket", "empty", "testbucket", "new-empty", 0},
			{"testbucket", "inline", "testbucket", "new-inline", memory.KiB},
			{"testbucket", "remote", "testbucket", "new-remote", 9 * memory.KiB},
			// one remote segment and one inline segment
			{"testbucket", "remote-segment-size", "testbucket", "new-remote-segment-size", 10 * memory.KiB},
			{"testbucket", "remote+inline", "testbucket", "new-remote+inline", 11 * memory.KiB},
			// 3 remote segment
			{"testbucket", "multiple-remote-segments", "testbucket", "new-multiple-remote-segments", 29 * memory.KiB},
			{"testbucket", "remote-with-delimiters", "testbucket", "a/prefix/remote-with-delimiters", 9 * memory.KiB},

			// different bucket
			{"testbucket", "empty", "new-testbucket", "new-empty", 0},
			{"testbucket", "inline", "new-testbucket", "new-inline", memory.KiB},
			{"testbucket", "remote", "new-testbucket", "new-remote", 9 * memory.KiB},
			// one remote segment and one inline segment
			{"testbucket", "remote-segment-size", "new-testbucket", "new-remote-segment-size", 10 * memory.KiB},
			{"testbucket", "remote+inline", "new-testbucket", "new-remote+inline", 11 * memory.KiB},
			// 3 remote segment
			{"testbucket", "multiple-remote-segments", "new-testbucket", "new-multiple-remote-segments", 29 * memory.KiB},
			{"testbucket", "remote-with-delimiters", "new-testbucket", "a/prefix/remote-with-delimiters", 9 * memory.KiB},

			// TODO write tests to move to existing key
		}

		for _, destBucket := range []string{"testbucket", "new-testbucket"} {
			for _, objectSize := range []memory.Size{1 * memory.KiB, 9 * memory.KiB, 21 * memory.KiB} {
				for _, sourceKey := range []string{"an/object/key", "an/object/key/"} {
					for _, destTrailingSlash := range []bool{true, false} {
						for _, destKey := range []string{
							"an/object/key2",
							"an/object/key/2",
							"an/object2",
							"an/object",
							"another/object",
						} {
							if destTrailingSlash {
								destKey += "/"
							}
							testCases = append(testCases, testCase{
								"testbucket", sourceKey, destBucket, destKey, objectSize,
							})
						}
					}
				}
			}
		}

		for _, tc := range testCases {
			t.Run(tc.Bucket+"/"+tc.Key+" to "+tc.NewBucket+"/"+tc.NewKey+" "+tc.ObjectSize.Base2String(), func(t *testing.T) {
				expectedData := testrand.Bytes(tc.ObjectSize)
				err := planet.Uplinks[0].Upload(newCtx, planet.Satellites[0], tc.Bucket, tc.Key, expectedData)
				require.NoError(t, err)

				obj, err := project.StatObject(ctx, tc.Bucket, tc.Key)
				require.NoError(t, err)
				assertObject(t, obj, tc.Key)

				err = project.MoveObject(ctx, tc.Bucket, tc.Key, tc.NewBucket, tc.NewKey, nil)
				require.NoError(t, err)

				data, err := planet.Uplinks[0].Download(newCtx, planet.Satellites[0], tc.NewBucket, tc.NewKey)
				require.NoError(t, err)
				require.Equal(t, expectedData, data)

				movedObj, err := project.StatObject(ctx, tc.NewBucket, tc.NewKey)
				require.NoError(t, err)
				obj.Key = tc.NewKey // for easy compare
				require.Equal(t, obj, movedObj)

				_, err = project.StatObject(ctx, tc.Bucket, tc.Key)
				require.True(t, errors.Is(err, uplink.ErrObjectNotFound))

				// cleanup
				_, err = project.DeleteObject(ctx, tc.NewBucket, tc.NewKey)
				require.NoError(t, err)
			})
		}
	})
}

func TestMoveObject_Errors(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		err := project.MoveObject(ctx, "", "", "", "", nil)
		require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

		err = project.MoveObject(ctx, "test", "test", "", "", nil)
		require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

		err = project.MoveObject(ctx, "test", "", "", "", nil)
		require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

		err = project.MoveObject(ctx, "test", "key", "test", "", nil)
		require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

		err = project.MoveObject(ctx, "invalid_!?", "test", "invalid_!?", "new-test", nil)
		require.Error(t, err) // check how we can return uplink.ErrBucketNameInvalid here

		createBucket(t, ctx, project, "testbucket")

		err = project.MoveObject(ctx, "testbucket", "key", "testbucket", "new-key", nil)
		require.True(t, errors.Is(err, uplink.ErrObjectNotFound))

		// move to non existing bucket
		err = project.MoveObject(ctx, "testbucket", "key", "non-existing-bucket", "key", nil)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))
		require.Contains(t, err.Error(), "(\"non-existing-bucket\")")

		err = project.MoveObject(ctx, "testbucket", "prefix/", "testbucket", "new-key", nil)
		require.True(t, errors.Is(err, uplink.ErrObjectNotFound))

		err = project.MoveObject(ctx, "testbucket", "key", "testbucket", "prefix/", nil)
		require.Error(t, err)

		// moving not committed objects is not allowed, object will be not found
		_, err = project.BeginUpload(ctx, "testbucket", "multipart", nil)
		require.NoError(t, err)

		err = project.MoveObject(ctx, "testbucket", "multipart", "testbucket", "new-multipart", nil)
		require.True(t, errors.Is(err, uplink.ErrObjectNotFound))

		// TODO add test cases for lack of access to target location
	})
}

func TestMoveObject_ETAG(t *testing.T) {
	// verify that segment ETAG can be decrypted after moving object
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		upload, err := project.BeginUpload(ctx, "testbucket", "key", nil)
		require.NoError(t, err)

		partUpload, err := project.UploadPart(ctx, "testbucket", "key", upload.UploadID, 1)
		require.NoError(t, err)

		err = partUpload.SetETag([]byte("my ETAG"))
		require.NoError(t, err)

		_, err = partUpload.Write(testrand.Bytes(10 * memory.KiB))
		require.NoError(t, err)

		err = partUpload.Commit()
		require.NoError(t, err)

		_, err = project.CommitUpload(ctx, "testbucket", "key", upload.UploadID, nil)
		require.NoError(t, err)

		err = project.MoveObject(ctx, "testbucket", "key", "testbucket", "new-key", nil)
		require.NoError(t, err)

		// for now ListUploadParts allow to get parts for committed objects
		iterator := project.ListUploadParts(ctx, "testbucket", "new-key", upload.UploadID, nil)

		require.True(t, iterator.Next())
		require.Equal(t, []byte("my ETAG"), iterator.Item().ETag)
	})
}

func TestMoveObject_Metadata(t *testing.T) {
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

				err = project.MoveObject(ctx, "bucket", "non-mpu-"+name, "bucket", "move-non-mpu-"+name, nil)
				require.NoError(t, err)

				copiedObject, err := project.StatObject(ctx, "bucket", "move-non-mpu-"+name)
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

				err = project.MoveObject(ctx, "bucket", "mpu-"+name, "bucket", "move-mpu-"+name, nil)
				require.NoError(t, err)

				copiedObject, err := project.StatObject(ctx, "bucket", "move-mpu-"+name)
				require.NoError(t, err)
				require.Equal(t, metadata, copiedObject.Custom)
			})
		}

		// verify that if metadata is set then key and nonce are also set
		objects, err := planet.Satellites[0].Metabase.DB.TestingAllObjects(ctx)
		require.NoError(t, err)
		require.Len(t, objects, len(expectedMetadata)*2)

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

func TestMoveObject_Overwrite(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1,
		UplinkCount:    1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		type testCase struct {
			Bucket     string
			Key        string
			NewBucket  string
			NewKey     string
			ObjectSize memory.Size
		}

		tc := testCase{"testbucket", "an/object/key", "testbucket", "an/object/key2", memory.KiB}

		for range 10 {
			uploadObject(t, ctx, project, tc.Bucket, tc.Key, tc.ObjectSize)
			require.NoError(t, project.MoveObject(ctx, tc.Bucket, tc.Key, tc.NewBucket, tc.NewKey, nil))
		}
	})
}
