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

		_, err = project.CopyObject(ctx, "testbucket", "objectA", "testbucket", "objectB", nil)
		require.Error(t, err)

		// TODO add test cases for lack of access to target location
	})
}
