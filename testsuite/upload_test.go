// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"io"
	"strconv"
	"testing"
	"time"

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
			err := project.DeleteBucket(ctx, "test-bucket")
			require.NoError(t, err)
		}()

		key := "object-with-metadata"
		upload, err := project.UploadObject(ctx, bucket.Name, key)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), key)

		expectedStdMetadata := &uplink.StandardMetadata{
			ContentLength: testrand.Int63n(200000),
			ContentType:   "aplication/json",

			FileCreated:     time.Now(),
			FileModified:    time.Now().Add(1 * time.Hour),
			FilePermissions: 666,

			// https://protogen.marcgravell.com/decode 78-96-01
			Unknown: []byte{120, 150, 01},
		}

		expectedCustomMetadata := uplink.CustomMetadata{}
		for i := 0; i < 10; i++ {
			// TODO figure out why its failing with
			// expectedCustomMetadata[string(testrand.BytesInt(10))] = string(testrand.BytesInt(100))
			expectedCustomMetadata["key"+strconv.Itoa(i)] = "value" + strconv.Itoa(i)
		}
		err = upload.SetMetadata(ctx, expectedStdMetadata, expectedCustomMetadata)
		require.NoError(t, err)

		randData := testrand.Bytes(1 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), key)

		err = upload.Commit()
		require.NoError(t, err)
		assertObject(t, upload.Info(), key)

		// time is unserialized to UTC
		expectedStdMetadata.FileCreated = expectedStdMetadata.FileCreated.UTC()
		expectedStdMetadata.FileModified = expectedStdMetadata.FileModified.UTC()

		{ // test metadata from Stat
			obj, err := project.StatObject(ctx, bucket.Name, key)
			require.NoError(t, err)

			require.Equal(t, *expectedStdMetadata, obj.Standard)
			require.Equal(t, expectedCustomMetadata, obj.Custom)
		}
		{ // test metadata from ListObjects
			objects := project.ListObjects(ctx, bucket.Name, &uplink.ObjectsOptions{
				Standard: true,
				Custom:   true,
			})
			require.NoError(t, objects.Err())

			found := objects.Next()
			require.NoError(t, objects.Err())
			require.True(t, found)

			listObject := objects.Item()
			require.Equal(t, *expectedStdMetadata, listObject.Object.Standard)
			require.Equal(t, expectedCustomMetadata, listObject.Object.Custom)
		}
		{ // test metadata from ListObjects and disabled standard and custom metadata
			objects := project.ListObjects(ctx, bucket.Name, &uplink.ObjectsOptions{
				Standard: false,
				Custom:   false,
			})
			require.NoError(t, objects.Err())

			found := objects.Next()
			require.NoError(t, objects.Err())
			require.True(t, found)

			listObject := objects.Item()
			require.Equal(t, uplink.StandardMetadata{}, listObject.Object.Standard)
			require.Equal(t, uplink.CustomMetadata(nil), listObject.Object.Custom)
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
			err := project.DeleteBucket(ctx, "test-bucket")
			require.NoError(t, err)
		}()

		key := "object-with-metadata"
		upload, err := project.UploadObject(ctx, bucket.Name, key)
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

		err = upload.SetMetadata(ctx, &uplink.StandardMetadata{}, uplink.CustomMetadata{})
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
			err := project.DeleteBucket(ctx, "test-bucket")
			require.NoError(t, err)
		}()

		key := "object-with-metadata"
		upload, err := project.UploadObject(ctx, bucket.Name, key)
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

		err = upload.SetMetadata(ctx, &uplink.StandardMetadata{}, uplink.CustomMetadata{})
		require.Error(t, err)
		require.True(t, uplink.ErrUploadDone.Has(err))
	})
}
