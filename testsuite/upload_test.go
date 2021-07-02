// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
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
		defer ctx.Check(project.Close)

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

func TestSetNilMetadata(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		bucket := createBucket(t, ctx, project, "test-bucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "test-bucket")
			require.NoError(t, err)
		}()

		key := "object-with-metadata"
		upload, err := project.UploadObject(ctx, bucket.Name, key, nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), key)

		// set nil to be sure we are not breaking anything internally
		err = upload.SetCustomMetadata(ctx, nil)
		require.NoError(t, err)

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
			require.Equal(t, uplink.CustomMetadata{}, obj.Custom)
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
		defer ctx.Check(project.Close)

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
		require.True(t, errors.Is(err, uplink.ErrUploadDone))
	})
}

func TestSetMetadataAfterAbort(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

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
		require.True(t, errors.Is(err, uplink.ErrUploadDone))
	})
}

func TestUpdateMetadata(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		_, err := project.EnsureBucket(ctx, "testbucket")
		require.NoError(t, err)

		expected := testrand.Bytes(1 * memory.KiB)

		// upload object with no custom metadata
		upload, err := project.UploadObject(ctx, "testbucket", "obj", nil)
		require.NoError(t, err)
		_, err = upload.Write(expected)
		require.NoError(t, err)
		require.NoError(t, upload.Commit())

		// check that there is no custom metadata after the upload
		object, err := project.StatObject(ctx, "testbucket", "obj")
		require.NoError(t, err)
		require.Empty(t, object.Custom)

		newMetadata := uplink.CustomMetadata{
			"key1": "value1",
			"key2": "value2",
		}

		// update the object's metadata
		err = project.UpdateObjectMetadata(ctx, "testbucket", "obj", newMetadata, nil)
		require.NoError(t, err)

		// check that the metadata has been updated as expected
		object, err = project.StatObject(ctx, "testbucket", "obj")
		require.NoError(t, err)
		require.Equal(t, newMetadata, object.Custom)

		// confirm that the object is still downloadable
		download, err := project.DownloadObject(ctx, "testbucket", "obj", nil)
		require.NoError(t, err)
		downloaded, err := ioutil.ReadAll(download)
		require.NoError(t, err)
		require.NoError(t, download.Close())
		require.Equal(t, expected, downloaded)

		// remove the object's metadata
		err = project.UpdateObjectMetadata(ctx, "testbucket", "obj", nil, nil)
		require.NoError(t, err)

		// check that the metadata has been removed
		object, err = project.StatObject(ctx, "testbucket", "obj")
		require.NoError(t, err)
		require.Empty(t, object.Custom)

		// confirm that the object is still downloadable
		download, err = project.DownloadObject(ctx, "testbucket", "obj", nil)
		require.NoError(t, err)
		downloaded, err = ioutil.ReadAll(download)
		require.NoError(t, err)
		require.NoError(t, download.Close())
		require.Equal(t, expected, downloaded)
	})
}

func TestConcurrentUploadToSamePath(t *testing.T) {
	t.Skip("Currently, we cannot override object on upload with metabase implementation")

	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: testplanet.MaxSegmentSize(20 * memory.KiB),
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		uplink := planet.Uplinks[0]

		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		expectedData := testrand.Bytes(50 * memory.KiB)
		N := len(expectedData)

		upload1, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)

		upload2, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)

		upload3, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)

		_, err = upload1.Write(expectedData[:N/2])
		require.NoError(t, err)
		_, err = upload2.Write(expectedData[:N/2])
		require.NoError(t, err)
		_, err = upload3.Write(expectedData[:N/2])
		require.NoError(t, err)

		_, err = upload1.Write(expectedData[N/2:])
		require.NoError(t, err)
		_, err = upload2.Write(expectedData[N/2:])
		require.NoError(t, err)
		_, err = upload3.Write(expectedData[N/2:])
		require.NoError(t, err)

		err = upload1.Commit()
		require.NoError(t, err)

		downloaded, err := uplink.Download(ctx, satellite, "testbucket", "test.dat")
		require.NoError(t, err)
		require.Equal(t, expectedData, downloaded)

		err = upload2.Abort()
		require.NoError(t, err)

		downloaded, err = uplink.Download(ctx, satellite, "testbucket", "test.dat")
		require.NoError(t, err)
		require.Equal(t, expectedData, downloaded)

		err = upload3.Commit()
		require.NoError(t, err)

		downloaded, err = uplink.Download(ctx, satellite, "testbucket", "test.dat")
		require.NoError(t, err)
		require.Equal(t, expectedData, downloaded)
	})
}
