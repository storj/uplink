// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
	"storj.io/uplink/private/testuplink"
)

func TestListObjects_NonExistingBucket(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		list := project.ListObjects(ctx, "non-existing-bucket", nil)
		require.NoError(t, list.Err())
		require.Nil(t, list.Item())

		require.False(t, list.Next())
		require.Error(t, list.Err())
		require.True(t, errors.Is(list.Err(), uplink.ErrBucketNotFound))
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

		list := listObjects(ctx, t, project, "testbucket", nil)

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

		list := listObjects(ctx, t, project, "testbucket", nil)

		assert.True(t, list.Next())
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		require.Equal(t, "test.dat", list.Item().Key)

		assertNoNextObject(t, list)
	})
}

func TestListObjects_IncludeMetadata(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		expectedMetadata := uplink.CustomMetadata{
			"testtag": "testtag",
		}
		uploadObjectWithMetadata(t, ctx, project, "testbucket", "test.dat", 1*memory.KiB, expectedMetadata)

		list := listObjects(ctx, t, project, "testbucket", &uplink.ListObjectsOptions{
			Custom: true, // include metadata
		})
		assert.True(t, list.Next())
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.Equal(t, expectedMetadata, list.Item().Custom)

		list = listObjects(ctx, t, project, "testbucket", &uplink.ListObjectsOptions{
			Custom: false, // don't include metadata
		})
		assert.True(t, list.Next())
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.Equal(t, uplink.CustomMetadata(nil), list.Item().Custom)

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

		list := listObjects(ctx, t, project, "testbucket", nil)

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

		list := listObjects(ctx, t, project, "testbucket", &uplink.ListObjectsOptions{
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

		list := listObjects(ctx, t, project, "testbucket", &uplink.ListObjectsOptions{Prefix: "a/b/"})

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

		list := listObjects(ctx, t, project, "testbucket", nil)

		// get the first list item and make it a cursor for the next list request
		more := list.Next()
		require.True(t, more)
		require.NoError(t, list.Err())
		delete(expectedObjects, list.Item().Key)
		cursor := list.Item().Key

		// list again with cursor set to the first item from previous list request
		list = listObjects(ctx, t, project, "testbucket", &uplink.ListObjectsOptions{Cursor: cursor})

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

func TestListObjects_Paging(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		totalObjects := 17
		expectedObjects := map[string]bool{}
		expectedContentSize := memory.Size(3)
		expectedMetadata := uplink.CustomMetadata{
			"metadata-key": "metadata-value",
		}

		now := time.Now()

		for i := 0; i < totalObjects; i++ {
			key := fmt.Sprintf("%d/%d.dat", i, i)
			expectedObjects[key] = true
			uploadObjectWithMetadata(t, ctx, project, "testbucket", key, expectedContentSize, expectedMetadata)
		}

		newCtx := testuplink.WithListLimit(ctx, 3)

		t.Run("without system and custom metadata", func(t *testing.T) {
			list := listObjects(newCtx, t, project, "testbucket", &uplink.ListObjectsOptions{
				Recursive: true,
			})

			numberOfObjects := 0
			for list.Next() {
				object := list.Item()

				_, ok := expectedObjects[object.Key]
				require.True(t, ok)

				numberOfObjects++
			}

			require.NoError(t, list.Err())
			require.Equal(t, numberOfObjects, len(expectedObjects))
		})

		t.Run("with system metadata", func(t *testing.T) {
			list := listObjects(newCtx, t, project, "testbucket", &uplink.ListObjectsOptions{
				Recursive: true,
				System:    true,
			})

			numberOfObjects := 0
			for list.Next() {
				object := list.Item()

				_, ok := expectedObjects[object.Key]
				require.True(t, ok)

				require.Equal(t, expectedContentSize.Int64(), object.System.ContentLength)
				require.WithinDuration(t, now, object.System.Created, 10*time.Second)

				numberOfObjects++
			}

			require.NoError(t, list.Err())
			require.Equal(t, numberOfObjects, len(expectedObjects))
		})

		t.Run("with custom metadata", func(t *testing.T) {
			list := listObjects(newCtx, t, project, "testbucket", &uplink.ListObjectsOptions{
				Recursive: true,
				Custom:    true,
			})

			numberOfObjects := 0
			for list.Next() {
				object := list.Item()

				_, ok := expectedObjects[object.Key]
				require.True(t, ok)

				require.Equal(t, expectedMetadata, object.Custom)

				numberOfObjects++
			}

			require.NoError(t, list.Err())
			require.Equal(t, numberOfObjects, len(expectedObjects))
		})

		t.Run("with system and custom metadata", func(t *testing.T) {
			list := listObjects(newCtx, t, project, "testbucket", &uplink.ListObjectsOptions{
				Recursive: true,
				System:    true,
				Custom:    true,
			})

			numberOfObjects := 0
			for list.Next() {
				object := list.Item()

				_, ok := expectedObjects[object.Key]
				require.True(t, ok)

				require.Equal(t, expectedContentSize.Int64(), object.System.ContentLength)
				require.WithinDuration(t, now, object.System.Created, 10*time.Second)
				require.Equal(t, expectedMetadata, object.Custom)

				numberOfObjects++
			}

			require.NoError(t, list.Err())
			require.Equal(t, numberOfObjects, len(expectedObjects))
		})
	})
}

func TestListObjects_TwoObjectsWithDiffPassphrase(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		apiKey := planet.Uplinks[0].Projects[0].APIKey

		bucket := "test-bucket"
		items := []string{"first-object", "second-object"}
		accesses := make([]*uplink.Access, 2)
		var err error

		// upload two objects with different encryption passphrases
		for i, item := range items {
			accesses[i], err = uplink.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, item)
			require.NoError(t, err)

			project, err := uplink.OpenProject(ctx, accesses[i])
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			createBucket(t, ctx, project, bucket)

			uploadObject(t, ctx, project, bucket, item, 1)
		}

		// listing should return one object per access
		for i, access := range accesses {
			project, err := uplink.OpenProject(ctx, access)
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			objects := project.ListObjects(ctx, bucket, nil)

			require.True(t, objects.Next())
			require.NoError(t, objects.Err())
			require.Equal(t, items[i], objects.Item().Key)
			require.False(t, objects.Next())
		}
	})
}

func TestListObjects_DifficultNames(t *testing.T) {
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

		uploadObject(t, ctx, project, "testbucket", "/", 1*memory.KiB)
		defer func() {
			_, err := project.DeleteObject(ctx, "testbucket", "/")
			require.NoError(t, err)
		}()

		uploadObject(t, ctx, project, "testbucket", "//", 1*memory.KiB)
		defer func() {
			_, err := project.DeleteObject(ctx, "testbucket", "//")
			require.NoError(t, err)
		}()

		uploadObject(t, ctx, project, "testbucket", "///", 1*memory.KiB)
		defer func() {
			_, err := project.DeleteObject(ctx, "testbucket", "///")
			require.NoError(t, err)
		}()

		{
			list := listObjects(ctx, t, project, "testbucket", nil)

			require.True(t, list.Next())
			require.True(t, list.Item().IsPrefix)
			require.Equal(t, "/", list.Item().Key)

			require.NoError(t, list.Err())
			assertNoNextObject(t, list)
		}

		{
			list := listObjects(ctx, t, project, "testbucket", &uplink.ListObjectsOptions{
				Prefix: "/",
			})

			require.True(t, list.Next())
			require.False(t, list.Item().IsPrefix)
			require.Equal(t, "/", list.Item().Key)

			require.True(t, list.Next())
			require.True(t, list.Item().IsPrefix)
			require.Equal(t, "//", list.Item().Key)

			require.NoError(t, list.Err())
			assertNoNextObject(t, list)
		}

		{
			list := listObjects(ctx, t, project, "testbucket", &uplink.ListObjectsOptions{
				Prefix: "//",
			})

			require.True(t, list.Next())
			require.False(t, list.Item().IsPrefix)
			require.Equal(t, "//", list.Item().Key)

			require.True(t, list.Next())
			require.True(t, list.Item().IsPrefix)
			require.Equal(t, "///", list.Item().Key)

			require.NoError(t, list.Err())
			assertNoNextObject(t, list)
		}

		{
			list := listObjects(ctx, t, project, "testbucket", &uplink.ListObjectsOptions{
				Prefix: "///",
			})

			require.True(t, list.Next())
			require.False(t, list.Item().IsPrefix)
			require.Equal(t, "///", list.Item().Key)

			require.NoError(t, list.Err())
			assertNoNextObject(t, list)
		}
	})
}

func listObjects(ctx context.Context, t *testing.T, project *uplink.Project, bucket string, options *uplink.ListObjectsOptions) *uplink.ObjectIterator {
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
