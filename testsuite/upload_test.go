// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func TestSetMetadata(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		ctx.Check(project.Close)

		bucket := createBucket(t, ctx, project, "test-bucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "test-bucket")
			require.NoError(t, err)
		}()

		key := "object-with-metadata"
		upload, err := project.UploadObject(ctx, bucket.Name, key, nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), key)

		expectedCustomMetadata := uplink.CustomMetadata{}
		for i := 0; i < 10; i++ {
			// TODO figure out why its failing with
			// expectedCustomMetadata[string(testrand.BytesInt(10))] = string(testrand.BytesInt(100))
			expectedCustomMetadata["key"+strconv.Itoa(i)] = "value" + strconv.Itoa(i)
		}

		err = upload.SetCustomMetadata(ctx, expectedCustomMetadata)
		require.NoError(t, err)

		// don't allow invalid metadata
		err = upload.SetCustomMetadata(ctx, uplink.CustomMetadata{
			"\x00": "alpha",
		})
		require.Error(t, err)

		randData := testrand.Bytes(1 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), key)

		err = upload.Commit()
		require.NoError(t, err)
		assertObject(t, upload.Info(), key)

		defer func() {
			_, err := project.DeleteObject(ctx, "test-bucket", key)
			require.NoError(t, err)
		}()

		{ // test metadata from Stat
			obj, err := project.StatObject(ctx, bucket.Name, key)
			require.NoError(t, err)

			require.Equal(t, memory.KiB.Int64(), obj.System.ContentLength)
			require.Equal(t, expectedCustomMetadata, obj.Custom)
		}
		{ // test metadata from ListObjects
			objects := project.ListObjects(ctx, bucket.Name, &uplink.ListObjectsOptions{
				System: true,
				Custom: true,
			})
			require.NoError(t, objects.Err())

			found := objects.Next()
			require.NoError(t, objects.Err())
			require.True(t, found)

			listObject := objects.Item()
			require.Equal(t, memory.KiB.Int64(), listObject.System.ContentLength)
			require.Equal(t, expectedCustomMetadata, listObject.Custom)
		}
		{ // test metadata from ListObjects and disabled standard and custom metadata
			objects := project.ListObjects(ctx, bucket.Name, &uplink.ListObjectsOptions{
				System: false,
				Custom: false,
			})
			require.NoError(t, objects.Err())

			found := objects.Next()
			require.NoError(t, objects.Err())
			require.True(t, found)

			listObject := objects.Item()
			require.Equal(t, int64(0), listObject.System.ContentLength)
			require.Equal(t, uplink.CustomMetadata(nil), listObject.Custom)
		}
	})
}

func TestSetMetadataAfterCommit(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		ctx.Check(project.Close)

		bucket := createBucket(t, ctx, project, "test-bucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "test-bucket")
			require.NoError(t, err)
		}()

		key := "object-with-metadata"
		upload, err := project.UploadObject(ctx, bucket.Name, key, nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), key)

		randData := testrand.Bytes(1 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), key)

		err = upload.Commit()
		require.NoError(t, err)
		assertObject(t, upload.Info(), key)

		defer func() {
			_, err := project.DeleteObject(ctx, "test-bucket", key)
			require.NoError(t, err)
		}()

		err = upload.SetCustomMetadata(ctx, uplink.CustomMetadata{})
		require.Error(t, err)
		require.True(t, uplink.ErrUploadDone.Has(err))
	})
}

func TestSetMetadataAfterAbort(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		ctx.Check(project.Close)

		bucket := createBucket(t, ctx, project, "test-bucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "test-bucket")
			require.NoError(t, err)
		}()

		key := "object-with-metadata"
		upload, err := project.UploadObject(ctx, bucket.Name, key, nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), key)

		randData := testrand.Bytes(1 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), key)

		err = upload.Abort()
		require.NoError(t, err)

		err = upload.Commit()
		require.Error(t, err)

		err = upload.SetCustomMetadata(ctx, uplink.CustomMetadata{})
		require.Error(t, err)
		require.True(t, uplink.ErrUploadDone.Has(err))
	})
}
