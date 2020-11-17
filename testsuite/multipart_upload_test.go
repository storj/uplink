// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func TestProject_NewMultipartUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		_, err := project.NewMultipartUpload(ctx, "not-existing-testbucket", "multipart-object", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		createBucket(t, ctx, project, "testbucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		info, err := project.NewMultipartUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		// we allow to start several multipart uploads for the same key
		// TODO check why its not possible anymore
		// _, err = project.NewMultipartUpload(ctx, "testbucket", "multipart-object", nil)
		// require.NoError(t, err)
		// require.NotNil(t, info.StreamID)

		info, err = project.NewMultipartUpload(ctx, "testbucket", "multipart-object-1", &uplink.MultipartUploadOptions{
			Expires: time.Now().Add(time.Hour),
		})
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)
	})
}

func TestProject_CompleteMultipartUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		{
			_, err := project.CompleteMultipartUpload(ctx, "", "", "", nil)
			require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

			_, err = project.CompleteMultipartUpload(ctx, "testbucket", "", "", nil)
			require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

			_, err = project.CompleteMultipartUpload(ctx, "testbucket", "multipart-object", "", nil)
			require.Error(t, err) // TODO should we create an error like ErrInvalidArgument
		}

		{
			info, err := project.NewMultipartUpload(ctx, "testbucket", "multipart-object", nil)
			require.NoError(t, err)
			require.NotNil(t, info.StreamID)

			_, err = project.CompleteMultipartUpload(ctx, "testbucket", "multipart-object", info.StreamID, nil)
			require.NoError(t, err)

			_, err = project.StatObject(ctx, "testbucket", "multipart-object")
			require.NoError(t, err)

			// object is already committed
			_, err = project.CompleteMultipartUpload(ctx, "testbucket", "multipart-object", info.StreamID, nil)
			require.Error(t, err)
		}

		{
			info, err := project.NewMultipartUpload(ctx, "testbucket", "multipart-object-metadata", nil)
			require.NoError(t, err)
			require.NotNil(t, info.StreamID)

			expectedMetadata := uplink.CustomMetadata{
				"TestField1": "TestFieldValue1",
				"TestField2": "TestFieldValue2",
			}
			_, err = project.CompleteMultipartUpload(ctx, "testbucket", "multipart-object-metadata", info.StreamID, &uplink.MultipartObjectOptions{
				CustomMetadata: expectedMetadata,
			})
			require.NoError(t, err)

			object, err := project.StatObject(ctx, "testbucket", "multipart-object-metadata")
			require.NoError(t, err)
			require.Equal(t, expectedMetadata, object.Custom)
		}

		// TODO add more tests
	})
}
