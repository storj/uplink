// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func TestObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		satelliteNodeURL := storj.NodeURL{ID: satellite.ID(), Address: satellite.Addr()}.String()
		apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
		uplinkConfig := uplink.Config{
			Whitelist: uplink.InsecureSkipConnectionVerify(),
		}
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, satelliteNodeURL, apiKey.Serialize(), "mypassphrase")
		require.NoError(t, err)

		project, err := uplinkConfig.Open(ctx, access)
		require.NoError(t, err)

		defer ctx.Check(project.Close)

		bucket, err := project.EnsureBucket(ctx, "testbucket")
		require.NoError(t, err)
		require.NotNil(t, bucket)
		require.Equal(t, "testbucket", bucket.Name)

		defer func() {
			err = project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		upload, err := project.UploadObject(ctx, "testbucket", "test.dat")
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info())

		randData := testrand.Bytes(10 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info())

		err = upload.Commit(nil)
		require.NoError(t, err)
		assertObject(t, upload.Info())

		obj, err := project.Stat(ctx, "testbucket", "test.dat")
		require.NoError(t, err)
		assertObject(t, obj)

		err = upload.Commit(nil)
		require.True(t, uplink.ErrUploadDone.Has(err))

		download, err := project.DownloadObject(ctx, "testbucket", "test.dat")
		require.NoError(t, err)
		assertObject(t, download.Info())

		var downloaded bytes.Buffer
		_, err = io.Copy(&downloaded, download)
		require.NoError(t, err)
		assert.Equal(t, randData, downloaded.Bytes())

		err = download.Close()
		require.NoError(t, err)

		downloadReq := uplink.DownloadRequest{
			Bucket: "testbucket",
			Key:    "test.dat",
			Offset: 100,
			Length: 500,
		}
		download, err = downloadReq.Do(ctx, project)
		require.NoError(t, err)
		assertObject(t, download.Info())

		var downloadedRange bytes.Buffer
		_, err = io.Copy(&downloadedRange, download)
		require.NoError(t, err)
		assert.Equal(t, randData[100:600], downloadedRange.Bytes())

		err = download.Close()
		require.NoError(t, err)
	})
}

func TestAbortUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		satelliteNodeURL := storj.NodeURL{ID: satellite.ID(), Address: satellite.Addr()}.String()
		apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
		uplinkConfig := uplink.Config{
			Whitelist: uplink.InsecureSkipConnectionVerify(),
		}
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, satelliteNodeURL, apiKey.Serialize(), "mypassphrase")
		require.NoError(t, err)

		project, err := uplinkConfig.Open(ctx, access)
		require.NoError(t, err)

		defer ctx.Check(project.Close)

		bucket, err := project.EnsureBucket(ctx, "testbucket")
		require.NoError(t, err)
		require.NotNil(t, bucket)
		require.Equal(t, "testbucket", bucket.Name)

		defer func() {
			err = project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		upload, err := project.UploadObject(ctx, "testbucket", "test.dat")
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info())

		randData := testrand.Bytes(10 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info())

		err = upload.Abort()
		require.NoError(t, err)

		err = upload.Commit(nil)
		require.Error(t, err)

		_, err = project.Stat(ctx, "testbucket", "test.dat")
		require.True(t, storj.ErrObjectNotFound.Has(err))

		err = upload.Abort()
		require.True(t, uplink.ErrUploadDone.Has(err))
	})
}

func assertObject(t *testing.T, obj *uplink.Object) {
	assert.Equal(t, "test.dat", obj.Key)
	assert.Condition(t, func() bool {
		return time.Since(obj.Created) < 10*time.Second
	})
}

func assertObjectEmptyCreated(t *testing.T, obj *uplink.Object) {
	assert.Equal(t, "test.dat", obj.Key)
	assert.Empty(t, obj.Created)
}
