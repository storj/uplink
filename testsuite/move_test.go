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

		testCases := []struct {
			Bucket     string
			Key        string
			NewBucket  string
			NewKey     string
			ObjectSize memory.Size
		}{
			{"testbucket", "empty", "testbucket", "new-empty", 0},
			{"testbucket", "inline", "testbucket", "new-inline", memory.KiB},
			{"testbucket", "remote", "testbucket", "new-remote", 9 * memory.KiB},
			// one remote segment and one inline segment
			{"testbucket", "remote-segment-size", "testbucket", "new-remote-segment-size", 10 * memory.KiB},
			{"testbucket", "remote+inline", "testbucket", "new-remote+inline", 11 * memory.KiB},
			// 3 remote segment
			{"testbucket", "multiple-remote-segments", "testbucket", "new-multiple-remote-segments", 29 * memory.KiB},
			{"testbucket", "remote-with-prefix", "testbucket", "a/prefix/remote-with-prefix", 9 * memory.KiB},

			// TODO write tests to move to existing key
			// TODO write tests moving between buckets
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

		err = project.MoveObject(ctx, "testbucket", "prefix/", "testbucket", "new-key", nil)
		require.Error(t, err)

		err = project.MoveObject(ctx, "testbucket", "key", "testbucket", "prefix/", nil)
		require.Error(t, err)

		// moving not committed objects is not allowed, object will be not found
		_, err = project.BeginUpload(ctx, "testbucket", "multipart", nil)
		require.NoError(t, err)

		err = project.MoveObject(ctx, "testbucket", "multipart", "testbucket", "new-multipart", nil)
		require.True(t, errors.Is(err, uplink.ErrObjectNotFound))

		// moving object to key where different object exists should end with error
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "testbucket", "objectA", testrand.Bytes(memory.KiB))
		require.NoError(t, err)
		err = planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "testbucket", "objectB", testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		err = project.MoveObject(ctx, "testbucket", "objectA", "testbucket", "objectB", nil)
		require.Error(t, err) // TODO make this error more human friendly

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
