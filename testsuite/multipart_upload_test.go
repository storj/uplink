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
		_, err = project.NewMultipartUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		info, err = project.NewMultipartUpload(ctx, "testbucket", "multipart-object-1", &uplink.MultipartUploadOptions{
			Expires: time.Now().Add(time.Hour),
		})
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)
	})
}
