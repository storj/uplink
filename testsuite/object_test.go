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

		testCases := []struct {
			Name       string
			ObjectSize memory.Size
		}{
			{"empty", 0},
			{"inline", memory.KiB},
			{"remote", 10 * memory.KiB},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.Name, func(t *testing.T) {
				upload, err := project.UploadObject(ctx, "testbucket", tc.Name, nil)
				require.NoError(t, err)
				assertObjectEmptyCreated(t, upload.Info(), tc.Name)

				randData := testrand.Bytes(tc.ObjectSize)
				source := bytes.NewBuffer(randData)
				_, err = io.Copy(upload, source)
				require.NoError(t, err)
				assertObjectEmptyCreated(t, upload.Info(), tc.Name)

				err = upload.Commit()
				require.NoError(t, err)
				assertObject(t, upload.Info(), tc.Name)

				obj, err := project.StatObject(ctx, "testbucket", tc.Name)
				require.NoError(t, err)
				assertObject(t, obj, tc.Name)

				err = upload.Commit()
				require.True(t, errors.Is(err, uplink.ErrUploadDone))

				uploadInfo := upload.Info()
				assertObject(t, uploadInfo, tc.Name)
				require.True(t, uploadInfo.System.Expires.IsZero())
				require.False(t, uploadInfo.IsPrefix)
				require.EqualValues(t, len(randData), uploadInfo.System.ContentLength)

				download, err := project.DownloadObject(ctx, "testbucket", tc.Name, nil)
				require.NoError(t, err)
				assertObject(t, download.Info(), tc.Name)

				var downloaded bytes.Buffer
				_, err = io.Copy(&downloaded, download)
				require.NoError(t, err)
				assert.Equal(t, randData, downloaded.Bytes())

				err = download.Close()
				require.NoError(t, err)

				if tc.ObjectSize > 600 {
					download, err = project.DownloadObject(ctx, "testbucket", tc.Name,
						&uplink.DownloadOptions{
							Offset: 100,
							Length: 500,
						})
					require.NoError(t, err)
					assertObject(t, download.Info(), tc.Name)

					var downloadedRange bytes.Buffer
					_, err = io.Copy(&downloadedRange, download)
					require.NoError(t, err)
					assert.Equal(t, randData[100:600], downloadedRange.Bytes())

					err = download.Close()
					require.NoError(t, err)
				}

				deleted, err := project.DeleteObject(ctx, "testbucket", tc.Name)
				require.NoError(t, err)
				require.NotNil(t, deleted)
				require.Equal(t, tc.Name, deleted.Key)

				obj, err = project.StatObject(ctx, "testbucket", tc.Name)
				require.True(t, errors.Is(err, uplink.ErrObjectNotFound))
				require.Nil(t, obj)

				// delete missing object
				deleted, err = project.DeleteObject(ctx, "testbucket", tc.Name)
				assert.Nil(t, err)
				require.Nil(t, deleted)
			})
		}

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
