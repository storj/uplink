// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func TestBucket(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")
		err := project.DeleteBucket(ctx, "testbucket")
		require.NoError(t, err)
	})
}

func createBucket(t *testing.T, ctx *testcontext.Context, project *uplink.Project, bucketName string) *uplink.Bucket {
	bucket, err := project.EnsureBucket(ctx, bucketName)
	require.NoError(t, err)
	require.NotNil(t, bucket)
	require.Equal(t, bucketName, bucket.Name)
	return bucket
}
