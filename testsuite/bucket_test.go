// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
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

		bucket := createBucket(t, ctx, project, "testbucket")

		{
			// statting a bucket
			statBucket, err := project.StatBucket(ctx, "testbucket")
			require.NoError(t, err)
			require.Equal(t, bucket.Name, statBucket.Name)
			require.Equal(t, bucket.Created, statBucket.Created)
		}

		{
			// creating existing bucket
			existing, err := project.CreateBucket(ctx, bucket.Name)
			require.Error(t, err)
			require.NotNil(t, existing)
			require.Equal(t, bucket.Name, existing.Name)
			require.Equal(t, bucket.Created, existing.Created)
		}

		{
			// ensuring existing bucket
			existing, err := project.EnsureBucket(ctx, bucket.Name)
			require.NoError(t, err)
			require.NotNil(t, existing)
			require.Equal(t, bucket.Name, existing.Name)
			require.Equal(t, bucket.Created, existing.Created)
		}

		{ // deleting a bucket
			deleted, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
			require.NotNil(t, deleted)
			require.Equal(t, "testbucket", deleted.Name)
		}

		{
			// statting a missing bucket
			statBucket, err := project.StatBucket(ctx, "testbucket")
			require.True(t, errors.Is(err, uplink.ErrBucketNotFound))
			require.Nil(t, statBucket)
		}

		{ // deleting a missing bucket
			deleted, err := project.DeleteBucket(ctx, "missing")
			require.True(t, errors.Is(err, uplink.ErrBucketNotFound))
			require.Nil(t, deleted)
		}
	})
}

func TestBucket_DeleteWithObjects(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		testbucket := "testbucket"
		bucket := createBucket(t, ctx, project, testbucket)

		// making sure the bucket exists
		statBucket, err := project.StatBucket(ctx, testbucket)
		require.NoError(t, err)
		require.Equal(t, bucket.Name, statBucket.Name)
		require.Equal(t, bucket.Created, statBucket.Created)

		// uploading an object to the bucket
		uploadObject(t, ctx, project, testbucket, "testobject", 1*memory.KiB)

		// try to delete the bucket
		_, err = project.DeleteBucket(ctx, testbucket)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotEmpty))

		// delete bucket with objects
		bucket, err = project.DeleteBucketWithObjects(ctx, testbucket)
		require.NoError(t, err)
		require.Equal(t, testbucket, bucket.Name)

		// making sure the bucket doesn't exist anymore
		statBucket, err = project.StatBucket(ctx, testbucket)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))
		require.Nil(t, statBucket)
	})
}

func createBucket(t *testing.T, ctx *testcontext.Context, project *uplink.Project, bucketName string) *uplink.Bucket {
	bucket, err := project.EnsureBucket(ctx, bucketName)
	require.NoError(t, err)
	require.NotNil(t, bucket)
	require.Equal(t, bucketName, bucket.Name)
	require.WithinDuration(t, time.Now(), bucket.Created, 10*time.Second)
	return bucket
}
