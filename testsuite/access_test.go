// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func TestSharePermisions(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
		uplinkConfig := uplink.Config{}
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, satellite.URL().String(), apiKey.Serialize(), "mypassphrase")
		require.NoError(t, err)

		items := []struct {
			AllowRead   bool
			AllowWrite  bool
			AllowList   bool
			AllowDelete bool
		}{
			{false, false, false, false},
			{true, true, true, true},

			{true, false, false, false},
			{false, true, false, false},
			{false, false, true, false},
			{false, false, false, true},

			// TODO generate all combinations automatically
		}

		expectedData := testrand.Bytes(10 * memory.KiB)
		{
			project := openProject(t, ctx, planet)
			require.NoError(t, err)

			// prepare bucket and object for all test cases
			for i := range items {
				bucketName := "testbucket" + strconv.Itoa(i)
				bucket, err := project.EnsureBucket(ctx, bucketName)
				require.NoError(t, err)
				require.NotNil(t, bucket)
				require.Equal(t, bucketName, bucket.Name)

				upload, err := project.UploadObject(ctx, bucketName, "test.dat", nil)
				require.NoError(t, err)

				source := bytes.NewBuffer(expectedData)
				_, err = io.Copy(upload, source)
				require.NoError(t, err)

				err = upload.Commit()
				require.NoError(t, err)
			}

			ctx.Check(project.Close)
		}

		for i, item := range items {
			i := i
			item := item

			name := func() string {
				result := make([]string, 0, 4)
				if item.AllowRead {
					result = append(result, "AllowRead")
				}
				if item.AllowWrite {
					result = append(result, "AllowWrite")
				}
				if item.AllowDelete {
					result = append(result, "AllowDelete")
				}
				if item.AllowList {
					result = append(result, "AllowList")
				}
				return strings.Join(result, "_")
			}

			t.Run(name(), func(t *testing.T) {
				permission := uplink.Permission{
					AllowRead:   item.AllowRead,
					AllowWrite:  item.AllowWrite,
					AllowDelete: item.AllowDelete,
					AllowList:   item.AllowList,
				}
				sharedAccess, err := access.Share(permission)
				if permission == (uplink.Permission{}) {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				project, err := uplinkConfig.OpenProject(ctx, sharedAccess)
				require.NoError(t, err)

				defer ctx.Check(project.Close)

				bucketName := "testbucket" + strconv.Itoa(i)
				{ // reading
					download, err := project.DownloadObject(ctx, bucketName, "test.dat", nil)
					if item.AllowRead {
						require.NoError(t, err)

						var downloaded bytes.Buffer
						_, err = io.Copy(&downloaded, download)

						require.NoError(t, err)
						require.Equal(t, expectedData, downloaded.Bytes())

						err = download.Close()
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
				}
				{ // writing
					upload, err := project.UploadObject(ctx, bucketName, "new-test.dat", nil)
					require.NoError(t, err)

					source := bytes.NewBuffer(expectedData)
					_, err = io.Copy(upload, source)
					if item.AllowWrite {
						require.NoError(t, err)

						err = upload.Commit()
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
				}
				{ // deleting
					// TODO test removing object
					// _, err := project.DeleteBucket(ctx, bucketName)
					// if item.AllowDelete {
					// 	require.NoError(t, err)
					// } else {
					// 	require.Error(t, err)
					// }
				}

				// TODO test listing buckets and objects

			})
		}
	})
}

func TestAccessSerialization(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		apiKey := planet.Uplinks[0].APIKey[satellite.ID()]

		access, err := uplink.RequestAccessWithPassphrase(ctx, satellite.URL().String(), apiKey.Serialize(), "mypassphrase")
		require.NoError(t, err)

		// try to serialize and deserialize access and use it for upload/download
		serializedAccess, err := access.Serialize()
		require.NoError(t, err)

		access, err = uplink.ParseAccess(serializedAccess)
		require.NoError(t, err)

		project, err := uplink.OpenProject(ctx, access)
		require.NoError(t, err)

		defer ctx.Check(project.Close)

		bucket, err := project.EnsureBucket(ctx, "test-bucket")
		require.NoError(t, err)
		require.NotNil(t, bucket)
		require.Equal(t, "test-bucket", bucket.Name)

		upload, err := project.UploadObject(ctx, "test-bucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		randData := testrand.Bytes(1 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		err = upload.Commit()
		require.NoError(t, err)
		assertObject(t, upload.Info(), "test.dat")

		err = upload.Commit()
		require.True(t, uplink.ErrUploadDone.Has(err))

		download, err := project.DownloadObject(ctx, "test-bucket", "test.dat", nil)
		require.NoError(t, err)
		assertObject(t, download.Info(), "test.dat")
	})
}

func TestUploadNotAllowedPath(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
		access, err := uplink.RequestAccessWithPassphrase(ctx, satellite.URL().String(), apiKey.Serialize(), "mypassphrase")
		require.NoError(t, err)

		err = planet.Uplinks[0].CreateBucket(ctx, satellite, "testbucket")
		require.NoError(t, err)

		sharedAccess, err := access.Share(uplink.FullPermission(), uplink.SharePrefix{
			Bucket: "testbucket",
			Prefix: "videos",
		})
		require.NoError(t, err)

		project, err := uplink.OpenProject(ctx, sharedAccess)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		testData := bytes.NewBuffer(testrand.Bytes(1 * memory.KiB))

		upload, err := project.UploadObject(ctx, "testbucket", "first-level-object", nil)
		require.NoError(t, err)

		_, err = io.Copy(upload, testData)
		require.Error(t, err)

		err = upload.Abort()
		require.NoError(t, err)

		upload, err = project.UploadObject(ctx, "testbucket", "videos/second-level-object", nil)
		require.NoError(t, err)

		_, err = io.Copy(upload, testData)
		require.NoError(t, err)

		err = upload.Commit()
		require.NoError(t, err)
	})
}
