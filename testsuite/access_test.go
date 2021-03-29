// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/paths"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
	privateAccess "storj.io/uplink/private/access"
)

func TestAccessSatelliteAddress(t *testing.T) {
	t.Run("existing access", func(t *testing.T) {
		access, err := uplink.ParseAccess("12edqwjdy4fmoHasYrxLzmu8Ubv8Hsateq1LPYne6Jzd64qCsYgET53eJzhB4L2pWDKBpqMowxt8vqLCbYxu8Qz7BJVH1CvvptRt9omm24k5GAq1R99mgGjtmc6yFLqdEFgdevuQwH5yzXCEEtbuBYYgES8Stb1TnuSiU3sa62bd2G88RRgbTCtwYrB8HZ7CLjYWiWUphw7RNa3NfD1TW6aUJ6E5D1F9AM6sP58X3D4H7tokohs2rqCkwRT")
		require.NoError(t, err)

		require.Equal(t, "1ds2WEsr2Frv8AFcPkdHfCdUjDrnGpb6y4rpADpX32TXzxg57k@127.0.0.1:44819", access.SatelliteAddress())
	})

	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		t.Run("new access", func(t *testing.T) {
			uplinkConfig := uplink.Config{}

			projectInfo := planet.Uplinks[0].Projects[0]
			access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
			require.NoError(t, err)

			require.Equal(t, projectInfo.Satellite.NodeURL().String(), access.SatelliteAddress())
		})
	})
}

func TestSharePermissions(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		uplinkConfig := uplink.Config{}

		projectInfo := planet.Uplinks[0].Projects[0]
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		items := []struct {
			AllowDownload bool
			AllowUpload   bool
			AllowList     bool
			AllowDelete   bool
		}{
			{false, false, false, false},
			{true, true, true, true},

			{true, false, false, false},
			{false, true, false, false},
			{false, false, true, false},
			{false, false, false, true},

			// TODO generate all combinations automatically
		}

		expectedData := testrand.Bytes(1 * memory.KiB)
		{
			project, err := uplinkConfig.OpenProject(ctx, access)
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
				if item.AllowDownload {
					result = append(result, "AllowDownload")
				}
				if item.AllowUpload {
					result = append(result, "AllowUpload")
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
					AllowDownload: item.AllowDownload,
					AllowUpload:   item.AllowUpload,
					AllowDelete:   item.AllowDelete,
					AllowList:     item.AllowList,
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
					if item.AllowDownload {
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
					require.NoError(t, err)

					err = upload.Commit()
					if item.AllowUpload {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
				}
				{ // deleting
					deletedObject, err := project.DeleteObject(ctx, bucketName, "test.dat")
					if item.AllowDelete {
						require.NoError(t, err)
						if item.AllowDownload || item.AllowList {
							require.NotNil(t, deletedObject)
							require.Equal(t, "test.dat", deletedObject.Key)
						} else {
							require.Nil(t, deletedObject)
						}
					} else {
						require.Error(t, err)
					}

					if item.AllowUpload {
						deletedObject, err = project.DeleteObject(ctx, bucketName, "new-test.dat")
						if item.AllowDelete {
							require.NoError(t, err)
							if item.AllowDownload || item.AllowList {
								require.NotNil(t, deletedObject)
								require.Equal(t, "new-test.dat", deletedObject.Key)
							} else {
								require.Nil(t, deletedObject)
							}
						} else {
							require.Error(t, err)
						}
					}

					deletedBucket, err := project.DeleteBucket(ctx, bucketName)
					if item.AllowDelete {
						require.NoError(t, err)
						// TODO: The commented logic does not work because of
						// issue with checking permissions for buckets -
						// buckets are currently automatically granted with Read permission.

						// if item.AllowDownload || item.AllowList {
						require.NotNil(t, deletedBucket)
						require.Equal(t, bucketName, deletedBucket.Name)
						// } else {
						// 	require.Nil(t, deletedBucket)
						// }
					} else {
						require.Error(t, err)
					}
				}

				// TODO test listing buckets and objects

			})
		}
	})
}

func TestSharePermisionsNotAfterNotBefore(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		uplinkConfig := uplink.Config{}

		projectInfo := planet.Uplinks[0].Projects[0]
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		{ // error when Before is earlier then After
			permission := uplink.FullPermission()
			permission.NotBefore = time.Now()
			permission.NotAfter = permission.NotBefore.Add(-1 * time.Hour)
			_, err := access.Share(permission)
			require.Error(t, err)
		}
		{ // don't permit operations until one hour from now
			permission := uplink.FullPermission()
			permission.NotBefore = time.Now().Add(time.Hour)
			sharedAccess, err := access.Share(permission)
			require.NoError(t, err)

			project, err := uplink.OpenProject(ctx, sharedAccess)
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			bucket, err := project.EnsureBucket(ctx, "test-bucket")
			require.Error(t, err)
			require.Nil(t, bucket)
		}
	})
}

func TestSharePrefix_List(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		uplinkConfig := uplink.Config{}

		projectInfo := planet.Uplinks[0].Projects[0]
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		expectedData := testrand.Bytes(1 * memory.KiB)
		{
			project, err := uplinkConfig.OpenProject(ctx, access)
			require.NoError(t, err)

			bucket, err := project.EnsureBucket(ctx, "testbucket")
			require.NoError(t, err)
			require.NotNil(t, bucket)
			require.Equal(t, "testbucket", bucket.Name)

			upload, err := project.UploadObject(ctx, "testbucket", "a/b/c/test.dat", nil)
			require.NoError(t, err)

			source := bytes.NewBuffer(expectedData)
			_, err = io.Copy(upload, source)
			require.NoError(t, err)

			err = upload.Commit()
			require.NoError(t, err)

			ctx.Check(project.Close)
		}

		for _, tt := range []struct {
			sharePrefix, listPrefix string
			deniedListPrefixes      []string
		}{
			{sharePrefix: "", listPrefix: ""},
			{sharePrefix: "", listPrefix: "a/"},
			{sharePrefix: "", listPrefix: "a/b/"},
			{sharePrefix: "a", listPrefix: "a/", deniedListPrefixes: []string{""}},
			{sharePrefix: "a", listPrefix: "a/b/", deniedListPrefixes: []string{""}},
			{sharePrefix: "a", listPrefix: "a/b/c/", deniedListPrefixes: []string{""}},
			{sharePrefix: "a/", listPrefix: "a/", deniedListPrefixes: []string{""}},
			{sharePrefix: "a/", listPrefix: "a/b/", deniedListPrefixes: []string{""}},
			{sharePrefix: "a/", listPrefix: "a/b/c/", deniedListPrefixes: []string{""}},
			{sharePrefix: "a/b", listPrefix: "a/b/c/", deniedListPrefixes: []string{"", "a/"}},
			{sharePrefix: "a/b", listPrefix: "a/b/", deniedListPrefixes: []string{"", "a/"}},
			{sharePrefix: "a/b", listPrefix: "a/b/c/", deniedListPrefixes: []string{"", "a/"}},
			{sharePrefix: "a/b/", listPrefix: "a/b/", deniedListPrefixes: []string{"", "a/"}},
			{sharePrefix: "a/b/", listPrefix: "a/b/c/", deniedListPrefixes: []string{"", "a/"}},
			{sharePrefix: "a/b/c", listPrefix: "a/b/c/", deniedListPrefixes: []string{"", "a/"}},
			{sharePrefix: "a/b/c/", listPrefix: "a/b/c/", deniedListPrefixes: []string{"", "a/"}},
		} {
			tt := tt
			t.Run("sharePrefix: "+tt.sharePrefix+", listPrefix: "+tt.listPrefix, func(t *testing.T) {
				sharedAccess, err := access.Share(uplink.FullPermission(), uplink.SharePrefix{
					Bucket: "testbucket",
					Prefix: tt.sharePrefix,
				})
				require.NoError(t, err)

				project, err := uplinkConfig.OpenProject(ctx, sharedAccess)
				require.NoError(t, err)
				defer ctx.Check(project.Close)

				list := project.ListObjects(ctx, "testbucket", &uplink.ListObjectsOptions{
					Prefix:    tt.listPrefix,
					Recursive: true,
				})
				assert.True(t, list.Next())
				require.NoError(t, list.Err())
				require.NotNil(t, list.Item())
				require.False(t, list.Item().IsPrefix)
				require.Equal(t, "a/b/c/test.dat", list.Item().Key)

				for _, listPrefix := range tt.deniedListPrefixes {
					list := project.ListObjects(ctx, "testbucket", &uplink.ListObjectsOptions{
						Prefix:    listPrefix,
						Recursive: true,
					})
					assert.False(t, list.Next())
					require.True(t, errors.Is(list.Err(), uplink.ErrPermissionDenied))
				}
			})
		}
	})
}

func TestSharePrefix_Download(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		uplinkConfig := uplink.Config{}

		projectInfo := planet.Uplinks[0].Projects[0]
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		expectedData := testrand.Bytes(1 * memory.KiB)
		{
			project, err := uplinkConfig.OpenProject(ctx, access)
			require.NoError(t, err)

			bucket, err := project.EnsureBucket(ctx, "testbucket")
			require.NoError(t, err)
			require.NotNil(t, bucket)
			require.Equal(t, "testbucket", bucket.Name)

			upload, err := project.UploadObject(ctx, "testbucket", "a/b/c/test.dat", nil)
			require.NoError(t, err)

			source := bytes.NewBuffer(expectedData)
			_, err = io.Copy(upload, source)
			require.NoError(t, err)

			err = upload.Commit()
			require.NoError(t, err)

			ctx.Check(project.Close)
		}

		for _, prefix := range []string{
			"",
			"a",
			"a/",
			"a/b",
			"a/b/",
			"a/b/c",
			"a/b/c/",
			"a/b/c/test.dat",
		} {
			prefix := prefix
			t.Run("prefix: "+prefix, func(t *testing.T) {
				sharedAccess, err := access.Share(uplink.FullPermission(), uplink.SharePrefix{
					Bucket: "testbucket",
					Prefix: prefix,
				})
				require.NoError(t, err)

				project, err := uplinkConfig.OpenProject(ctx, sharedAccess)
				require.NoError(t, err)
				defer ctx.Check(project.Close)

				download, err := project.DownloadObject(ctx, "testbucket", "a/b/c/test.dat", nil)
				require.NoError(t, err)

				downloaded, err := ioutil.ReadAll(download)
				require.NoError(t, err)
				require.Equal(t, expectedData, downloaded)

				err = download.Close()
				require.NoError(t, err)
			})
		}
	})
}

func TestSharePrefix_UploadDownload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		uplinkConfig := uplink.Config{}

		projectInfo := planet.Uplinks[0].Projects[0]
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		expectedData := testrand.Bytes(1 * memory.KiB)
		{
			sharedAccess, err := access.Share(
				uplink.Permission{
					AllowUpload: true,
				},
				uplink.SharePrefix{
					Bucket: "testbucket",
				})
			require.NoError(t, err)

			project, err := uplink.OpenProject(ctx, sharedAccess)
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			_, err = project.CreateBucket(ctx, "testbucket")
			require.NoError(t, err)

			upload, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
			require.NoError(t, err)

			source := bytes.NewBuffer(expectedData)
			_, err = io.Copy(upload, source)
			require.NoError(t, err)

			err = upload.Commit()
			require.NoError(t, err)
		}

		{
			sharedAccess, err := access.Share(
				uplink.Permission{
					AllowDownload: true,
				},
				uplink.SharePrefix{
					Bucket: "testbucket",
					Prefix: "test.dat",
				})
			require.NoError(t, err)

			project, err := uplink.OpenProject(ctx, sharedAccess)
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			download, err := project.DownloadObject(ctx, "testbucket", "test.dat", nil)
			require.NoError(t, err)

			downloaded, err := ioutil.ReadAll(download)
			require.NoError(t, err)

			err = download.Close()
			require.NoError(t, err)
			require.Equal(t, expectedData, downloaded)
		}
	})
}

// TestShareUnique asserts sharing from the same root grant with the same
// permissions should result in different access grants.
func TestShareUnique(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		uplinkConfig := uplink.Config{}

		projectInfo := planet.Uplinks[0].Projects[0]
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		permission := uplink.FullPermission()
		permission.NotBefore = time.Now()
		permission.NotAfter = permission.NotBefore.Add(1 * time.Hour)

		prefix := uplink.SharePrefix{
			Bucket: "testbucket",
			Prefix: "test.dat",
		}

		key1, err := access.Share(permission, prefix)
		require.NoError(t, err)

		key2, err := access.Share(permission, prefix)
		require.NoError(t, err)

		key1s, err := key1.Serialize()
		require.NoError(t, err)

		key2s, err := key2.Serialize()
		require.NoError(t, err)

		require.NotEqual(t, key1s, key2s)
	})
}

func TestAccessSerialization(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		apiKey := planet.Uplinks[0].Projects[0].APIKey

		access, err := uplink.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "mypassphrase")
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
		require.True(t, errors.Is(err, uplink.ErrUploadDone))

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

		apiKey := planet.Uplinks[0].Projects[0].APIKey
		access, err := uplink.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "mypassphrase")
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

func TestListObjects_EncryptionBypass(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]

		apiKey := planet.Uplinks[0].Projects[0].APIKey
		access, err := uplink.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "mypassphrase")
		require.NoError(t, err)

		bucketName := "testbucket"
		err = planet.Uplinks[0].CreateBucket(ctx, satellite, bucketName)
		require.NoError(t, err)

		objectKeys := []string{
			"a", "aa", "b", "bb", "c",
			"a/xa", "a/xaa", "a/xb", "a/xbb", "a/xc",
			"b/ya", "b/yaa", "b/yb", "b/ybb", "b/yc",
		}

		for _, key := range objectKeys {
			err = planet.Uplinks[0].Upload(ctx, satellite, bucketName, key, testrand.Bytes(memory.KiB))
			require.NoError(t, err)
		}
		sort.Strings(objectKeys)

		// Enable encryption bypass
		err = privateAccess.EnablePathEncryptionBypass(access)
		require.NoError(t, err)

		project, err := uplink.OpenProject(ctx, access)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		objects := project.ListObjects(ctx, bucketName, &uplink.ListObjectsOptions{
			Recursive: true,
		})

		// TODO verify that decoded string can be decrypted to defined filePaths,
		// currently it's not possible because we have no access encryption access store.
		for objects.Next() {
			item := objects.Item()

			iter := paths.NewUnencrypted(item.Key).Iterator()
			for !iter.Done() {
				next := iter.Next()

				// verify that path segments are encoded with base64
				_, err = base64.URLEncoding.DecodeString(next)
				require.NoError(t, err)
			}
		}
		require.NoError(t, objects.Err())
	})
}

func TestDeleteObject_EncryptionBypass(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]

		apiKey := planet.Uplinks[0].Projects[0].APIKey
		access, err := uplink.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "mypassphrase")
		require.NoError(t, err)

		bucketName := "testbucket"
		err = planet.Uplinks[0].CreateBucket(ctx, satellite, bucketName)
		require.NoError(t, err)

		err = planet.Uplinks[0].Upload(ctx, satellite, bucketName, "test-file", testrand.Bytes(memory.KiB))
		require.NoError(t, err)

		err = privateAccess.EnablePathEncryptionBypass(access)
		require.NoError(t, err)

		project, err := uplink.OpenProject(ctx, access)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		objects := project.ListObjects(ctx, bucketName, &uplink.ListObjectsOptions{
			Recursive: true,
		})

		for objects.Next() {
			item := objects.Item()

			_, err = base64.URLEncoding.DecodeString(item.Key)
			require.NoError(t, err)

			_, err = project.DeleteObject(ctx, bucketName, item.Key)
			require.NoError(t, err)
		}
		require.NoError(t, objects.Err())

		// this means that object was deleted and empty bucket can be deleted
		_, err = project.DeleteBucket(ctx, bucketName)
		require.NoError(t, err)
	})
}
