// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/storagenode"
	"storj.io/uplink"
)

func TestObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		upload, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		randData := testrand.Bytes(10 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		err = upload.Commit()
		require.NoError(t, err)
		assertObject(t, upload.Info(), "test.dat")

		obj, err := project.StatObject(ctx, "testbucket", "test.dat")
		require.NoError(t, err)
		assertObject(t, obj, "test.dat")

		err = upload.Commit()
		require.True(t, errors.Is(err, uplink.ErrUploadDone))

		uploadInfo := upload.Info()
		assertObject(t, uploadInfo, "test.dat")
		require.True(t, uploadInfo.System.Expires.IsZero())
		require.False(t, uploadInfo.IsPrefix)
		require.EqualValues(t, len(randData), uploadInfo.System.ContentLength)

		download, err := project.DownloadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObject(t, download.Info(), "test.dat")

		var downloaded bytes.Buffer
		_, err = io.Copy(&downloaded, download)
		require.NoError(t, err)
		assert.Equal(t, randData, downloaded.Bytes())

		err = download.Close()
		require.NoError(t, err)

		download, err = project.DownloadObject(ctx, "testbucket", "test.dat",
			&uplink.DownloadOptions{
				Offset: 100,
				Length: 500,
			})
		require.NoError(t, err)
		assertObject(t, download.Info(), "test.dat")

		var downloadedRange bytes.Buffer
		_, err = io.Copy(&downloadedRange, download)
		require.NoError(t, err)
		assert.Equal(t, randData[100:600], downloadedRange.Bytes())

		err = download.Close()
		require.NoError(t, err)

		deleted, err := project.DeleteObject(ctx, "testbucket", "test.dat")
		require.NoError(t, err)
		require.NotNil(t, deleted)
		require.Equal(t, "test.dat", deleted.Key)

		obj, err = project.StatObject(ctx, "testbucket", "test.dat")
		require.True(t, errors.Is(err, uplink.ErrObjectNotFound))
		require.Nil(t, obj)

		// delete missing object
		deleted, err = project.DeleteObject(ctx, "testbucket", "test.dat")
		assert.Nil(t, err)
		require.Nil(t, deleted)
	})
}

func TestAbortUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		upload, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		randData := testrand.Bytes(10 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		err = upload.Abort()
		require.NoError(t, err)

		err = upload.Commit()
		require.Error(t, err)

		_, err = project.StatObject(ctx, "testbucket", "test.dat")
		require.True(t, errors.Is(err, uplink.ErrObjectNotFound))

		err = upload.Abort()
		require.True(t, errors.Is(err, uplink.ErrUploadDone))
	})
}

func TestUploadError(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		upload, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		randData := testrand.Bytes(1 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		require.NoError(t, planet.StopPeer(planet.Satellites[0]))

		err = upload.Commit()
		require.Error(t, err)
	})
}

func TestVeryLongDownload(t *testing.T) {
	const (
		segmentSize           = 100 * memory.KiB
		orderLimitGracePeriod = 5 * time.Second
	)

	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: testplanet.MaxSegmentSize(segmentSize),
			StorageNode: func(index int, config *storagenode.Config) {
				config.Storage2.OrderLimitGracePeriod = orderLimitGracePeriod
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		data := testrand.Bytes(segmentSize * 3 / 2) // 2 segments
		err := planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "testbucket", "test.dat", data)
		require.NoError(t, err)

		download, cleanup, err := planet.Uplinks[0].DownloadStream(ctx, planet.Satellites[0], "testbucket", "test.dat")
		require.NoError(t, err)
		defer ctx.Check(download.Close)
		defer ctx.Check(cleanup)

		// read the first half of the first segment
		buf := make([]byte, segmentSize.Int()/2)
		n, err := io.ReadFull(download, buf)
		require.NoError(t, err)
		assert.Equal(t, segmentSize.Int()/2, n)
		assert.Equal(t, data[:segmentSize.Int()/2], buf)

		// sleep to expire already created orders
		time.Sleep(orderLimitGracePeriod + 1*time.Second)

		// read the rest: the remainder of the first segment, and the second (last) segment
		buf = make([]byte, segmentSize.Int())
		n, err = io.ReadFull(download, buf)
		require.NoError(t, err)
		assert.Equal(t, segmentSize.Int(), n)
		assert.Equal(t, data[segmentSize.Int()/2:segmentSize.Int()*3/2], buf)
	})
}

func assertObject(t *testing.T, obj *uplink.Object, expectedKey string) {
	assert.Equal(t, expectedKey, obj.Key)
	assert.WithinDuration(t, time.Now(), obj.System.Created, 10*time.Second)
}

func assertObjectEmptyCreated(t *testing.T, obj *uplink.Object, expectedKey string) {
	assert.Equal(t, expectedKey, obj.Key)
	assert.Empty(t, obj.System.Created)
}

func uploadObject(t *testing.T, ctx *testcontext.Context, project *uplink.Project, bucket, key string, objectSize memory.Size) *uplink.Object {
	upload, err := project.UploadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	assertObjectEmptyCreated(t, upload.Info(), key)

	randData := testrand.Bytes(objectSize)
	source := bytes.NewBuffer(randData)
	_, err = io.Copy(upload, source)
	require.NoError(t, err)
	assertObjectEmptyCreated(t, upload.Info(), key)

	err = upload.Commit()
	require.NoError(t, err)
	assertObject(t, upload.Info(), key)

	return upload.Info()
}
