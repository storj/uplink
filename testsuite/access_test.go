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
	"storj.io/common/storj"
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
		satelliteNodeURL := storj.NodeURL{ID: satellite.ID(), Address: satellite.Addr()}.String()
		apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
		uplinkConfig := uplink.Config{
			Whitelist: uplink.InsecureSkipConnectionVerify(),
		}
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, satelliteNodeURL, apiKey.Serialize(), "mypassphrase")
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

				upload, err := project.UploadObject(ctx, bucketName, "test.dat")
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
				sharedAccess, err := access.Share(uplink.Permission{
					AllowRead:   item.AllowRead,
					AllowWrite:  item.AllowWrite,
					AllowDelete: item.AllowDelete,
					AllowList:   item.AllowList,
				})
				require.NoError(t, err)

				project, err := uplinkConfig.Open(ctx, sharedAccess)
				require.NoError(t, err)

				defer ctx.Check(project.Close)

				bucketName := "testbucket" + strconv.Itoa(i)
				{ // reading
					download, err := project.DownloadObject(ctx, bucketName, "test.dat")
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
					upload, err := project.UploadObject(ctx, bucketName, "new-test.dat")
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
					err := project.DeleteBucket(ctx, bucketName)
					if item.AllowDelete {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
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
		satelliteNodeURL := storj.NodeURL{ID: satellite.ID(), Address: satellite.Addr()}.String()
		apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
		uplinkConfig := uplink.Config{
			Whitelist: uplink.InsecureSkipConnectionVerify(),
		}
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, satelliteNodeURL, apiKey.Serialize(), "mypassphrase")
		require.NoError(t, err)

		// try to serialize and deserialize access and use it for upload/download
		serializedAccess, err := access.Serialize()
		require.NoError(t, err)

		access = uplink.ParseAccess(serializedAccess)
		require.NoError(t, access.IsValid())

		project, err := uplinkConfig.Open(ctx, access)
		require.NoError(t, err)

		defer ctx.Check(project.Close)

		bucket, err := project.EnsureBucket(ctx, "test-bucket")
		require.NoError(t, err)
		require.NotNil(t, bucket)
		require.Equal(t, "test-bucket", bucket.Name)

		upload, err := project.UploadObject(ctx, "test-bucket", "test.dat")
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

		download, err := project.DownloadObject(ctx, "test-bucket", "test.dat")
		require.NoError(t, err)
		assertObject(t, download.Info(), "test.dat")
	})
}
