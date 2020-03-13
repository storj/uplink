// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func TestListObjects_NonExistingBucket(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		// TODO: Should this return BucketNotFound error?
		list := project.ListObjects(ctx, "testbucket", nil)
		require.NoError(t, list.Err())
		require.Nil(t, list.Item())

		assertNoNextObject(t, list)
	})
}

func TestListObjects_EmptyBucket(t *testing.T) {
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

		list := listObjects(t, ctx, project, "testbucket", nil)

		assertNoNextObject(t, list)
	})
}

func TestListObjects_SingleObject(t *testing.T) {
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

		uploadObject(t, ctx, project, "testbucket", "test.dat", 1*memory.KiB)
		defer func() {
			_, err := project.DeleteObject(ctx, "testbucket", "test.dat")
			require.NoError(t, err)
		}()

		list := listObjects(t, ctx, project, "testbucket", nil)

		assert.True(t, list.Next())
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		require.Equal(t, "test.dat", list.Item().Key)

		assertNoNextObject(t, list)
	})
}

func TestListObjects_TwoObjects(t *testing.T) {
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

		expectedObjects := map[string]bool{
			"test1.dat": true,
			"test2.dat": true,
		}

		for object := range expectedObjects {
			object := object
			uploadObject(t, ctx, project, "testbucket", object, 1*memory.KiB)
			defer func() {
				_, err := project.DeleteObject(ctx, "testbucket", object)
				require.NoError(t, err)
			}()
		}

		list := listObjects(t, ctx, project, "testbucket", nil)

		more := list.Next()
		require.True(t, more)
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		delete(expectedObjects, list.Item().Key)

		more = list.Next()
		require.True(t, more)
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		delete(expectedObjects, list.Item().Key)

		require.Empty(t, expectedObjects)
		assertNoNextObject(t, list)
	})
}

func TestListObjects_PrefixRecursive(t *testing.T) {
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

		uploadObject(t, ctx, project, "testbucket", "a/b/c/test.dat", 1*memory.KiB)
		defer func() {
			_, err := project.DeleteObject(ctx, "testbucket", "a/b/c/test.dat")
			require.NoError(t, err)
		}()

		list := listObjects(t, ctx, project, "testbucket", &uplink.ListObjectsOptions{
			Prefix:    "a/b/",
			Recursive: true,
		})

		assert.True(t, list.Next())
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		require.Equal(t, "a/b/c/test.dat", list.Item().Key)

		assertNoNextObject(t, list)
	})
}

func TestListObjects_PrefixNonRecursive(t *testing.T) {
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

		uploadObject(t, ctx, project, "testbucket", "a/b/c/test.dat", 1*memory.KiB)
		defer func() {
			_, err := project.DeleteObject(ctx, "testbucket", "a/b/c/test.dat")
			require.NoError(t, err)
		}()

		list := listObjects(t, ctx, project, "testbucket", &uplink.ListObjectsOptions{Prefix: "a/b/"})

		assert.True(t, list.Next())
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.True(t, list.Item().IsPrefix)
		require.Equal(t, "a/b/c/", list.Item().Key)

		assertNoNextObject(t, list)
	})
}

func TestListObjects_Cursor(t *testing.T) {
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

		expectedObjects := map[string]bool{
			"test1.dat": true,
			"test2.dat": true,
		}

		for object := range expectedObjects {
			object := object
			uploadObject(t, ctx, project, "testbucket", object, 1*memory.KiB)
			defer func() {
				_, err := project.DeleteObject(ctx, "testbucket", object)
				require.NoError(t, err)
			}()
		}

		list := listObjects(t, ctx, project, "testbucket", nil)

		// get the first list item and make it a cursor for the next list request
		more := list.Next()
		require.True(t, more)
		require.NoError(t, list.Err())
		delete(expectedObjects, list.Item().Key)
		cursor := list.Item().Key

		// list again with cursor set to the first item from previous list request
		list = listObjects(t, ctx, project, "testbucket", &uplink.ListObjectsOptions{Cursor: cursor})

		// expect the second item as the first item in this new list request
		more = list.Next()
		require.True(t, more)
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		delete(expectedObjects, list.Item().Key)

		require.Empty(t, expectedObjects)
		assertNoNextObject(t, list)
	})
}

func listObjects(t *testing.T, ctx context.Context, project *uplink.Project, bucket string, options *uplink.ListObjectsOptions) *uplink.ObjectIterator {
	list := project.ListObjects(ctx, bucket, options)
	require.NoError(t, list.Err())
	require.Nil(t, list.Item())
	return list
}

func assertNoNextObject(t *testing.T, list *uplink.ObjectIterator) {
	require.False(t, list.Next())
	require.NoError(t, list.Err())
	require.Nil(t, list.Item())
}
