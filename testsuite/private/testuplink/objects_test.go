// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package testuplink_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/encryption"
	"storj.io/common/memory"
	"storj.io/common/paths"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams"
	"storj.io/uplink/private/stream"
)

const TestFile = "test-file"

func TestCreateObject(t *testing.T) {
	runTestWithoutSN(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		bucket, err := db.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)

		for i, tt := range []struct {
			create *metaclient.CreateObject
		}{
			{
				create: nil,
			},
			{
				create: &metaclient.CreateObject{},
			},
		} {
			errTag := fmt.Sprintf("%d. %+v", i, tt)

			obj, err := db.CreateObject(ctx, bucket.Name, TestFile, tt.create)
			require.NoError(t, err)

			info := obj.Info()

			assert.Equal(t, TestBucket, info.Bucket.Name, errTag)
			assert.Equal(t, TestFile, info.Path, errTag)
			assert.EqualValues(t, 0, info.Size, errTag)
		}
	})
}

func TestGetObject(t *testing.T) {
	runTestWithoutSN(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		bucket, err := db.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)
		upload(ctx, t, db, streams, bucket.Name, TestFile, nil)

		_, err = db.GetObject(ctx, "", "", nil)
		assert.True(t, metaclient.ErrNoBucket.Has(err))

		_, err = db.GetObject(ctx, bucket.Name, "", nil)
		assert.True(t, metaclient.ErrNoPath.Has(err))

		_, err = db.GetObject(ctx, "non-existing-bucket", TestFile, nil)
		assert.True(t, metaclient.ErrObjectNotFound.Has(err))

		_, err = db.GetObject(ctx, bucket.Name, "non-existing-file", nil)
		assert.True(t, metaclient.ErrObjectNotFound.Has(err))

		object, err := db.GetObject(ctx, bucket.Name, TestFile, nil)
		require.NoError(t, err)
		assert.Equal(t, TestFile, object.Path)
		assert.Equal(t, TestBucket, object.Bucket.Name)
		assert.NotNil(t, uint32(1), object.Version)
	})
}

func TestDownloadObject(t *testing.T) {
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		data := testrand.Bytes(32 * memory.KiB)

		bucket, err := db.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)

		upload(ctx, t, db, streams, bucket.Name, "empty-file", nil)
		upload(ctx, t, db, streams, bucket.Name, "small-file", []byte("test"))
		upload(ctx, t, db, streams, bucket.Name, "large-file", data)

		_, err = db.GetObject(ctx, "", "", nil)
		assert.True(t, metaclient.ErrNoBucket.Has(err))

		_, err = db.GetObject(ctx, bucket.Name, "", nil)
		assert.True(t, metaclient.ErrNoPath.Has(err))

		assertData(ctx, t, db, streams, bucket.Name, "empty-file", []byte{})
		assertData(ctx, t, db, streams, bucket.Name, "small-file", []byte("test"))
		assertData(ctx, t, db, streams, bucket.Name, "large-file", data)

		/* TODO: Disable stopping due to flakiness.
		// Stop randomly half of the storage nodes and remove them from satellite's overlay
		perm := mathrand.Perm(len(planet.StorageNodes))
		for _, i := range perm[:(len(perm) / 2)] {
			assert.NoError(t, planet.StopPeer(planet.StorageNodes[i]))
			_, err := planet.Satellites[0].Overlay.Service.UpdateUptime(ctx, planet.StorageNodes[i].ID(), false)
			assert.NoError(t, err)
		}

		// try downloading the large file again
		assertStream(ctx, t, db, streams, bucket, "large-file", 32*memory.KiB.Int64(), data)
		*/
	})
}

func upload(ctx context.Context, t *testing.T, db *metaclient.DB, streams *streams.Store, bucket, key string, data []byte) {
	obj, err := db.CreateObject(ctx, bucket, key, nil)
	require.NoError(t, err)

	str, err := obj.CreateStream(ctx)
	require.NoError(t, err)

	upload := stream.NewUpload(ctx, str, streams)

	_, err = upload.Write(data)
	require.NoError(t, err)

	err = upload.Commit()
	require.NoError(t, err)
}

func assertData(ctx context.Context, t *testing.T, db *metaclient.DB, streams *streams.Store, bucketName, objectKey string, content []byte) {
	info, err := db.DownloadObject(ctx, bucketName, objectKey, nil, metaclient.DownloadOptions{})
	require.NoError(t, err)

	download := stream.NewDownload(ctx, info, streams)
	defer func() {
		err := download.Close()
		assert.NoError(t, err)
	}()

	data := make([]byte, len(content))
	n, err := io.ReadFull(download, data)
	require.NoError(t, err)

	assert.Equal(t, len(content), n)
	assert.Equal(t, content, data)
}

func TestDeleteObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		encStore := newTestEncStore(TestEncKey)
		encStore.SetDefaultPathCipher(storj.EncAESGCM)

		db, streams, cleanup, err := newMetainfoParts(planet, encStore)
		require.NoError(t, err)
		defer ctx.Check(cleanup)

		bucket, err := db.CreateBucket(ctx, TestBucket)
		if !assert.NoError(t, err) {
			return
		}

		unencryptedObjectKey := paths.NewUnencrypted(TestFile)
		encryptedObjectKey, err := encryption.EncryptPathWithStoreCipher(bucket.Name, unencryptedObjectKey, encStore)
		require.NoError(t, err)

		for i, key := range []string{unencryptedObjectKey.String(), encryptedObjectKey.String()} {
			upload(ctx, t, db, streams, bucket.Name, key, nil)

			if i < 0 {
				// Enable encryption bypass
				encStore.EncryptionBypass = true
			}

			_, err = db.DeleteObject(ctx, "", "", nil)
			assert.True(t, metaclient.ErrNoBucket.Has(err))

			_, err = db.DeleteObject(ctx, bucket.Name, "", nil)
			assert.True(t, metaclient.ErrNoPath.Has(err))

			_, err = db.DeleteObject(ctx, bucket.Name+"-not-exist", TestFile, nil)
			assert.Nil(t, err)

			_, err = db.DeleteObject(ctx, bucket.Name, "non-existing-file", nil)
			assert.Nil(t, err)

			object, err := db.DeleteObject(ctx, bucket.Name, key, nil)
			if assert.NoError(t, err) {
				assert.Equal(t, key, object.Path)
			}
		}
	})
}

func TestListObjectsEmpty(t *testing.T) {
	runTestWithoutSN(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		testBucketInfo, err := db.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)

		_, err = db.ListObjects(ctx, "", metaclient.ListOptions{})
		assert.True(t, metaclient.ErrNoBucket.Has(err))

		_, err = db.ListObjects(ctx, testBucketInfo.Name, metaclient.ListOptions{})
		assert.EqualError(t, err, "metainfo: invalid direction 0")

		// TODO for now we are supporting only metainfo.After
		for _, direction := range []metaclient.ListDirection{
			// metainfo.Forward,
			metaclient.After,
		} {
			list, err := db.ListObjects(ctx, testBucketInfo.Name, metaclient.ListOptions{Direction: direction})
			if assert.NoError(t, err) {
				assert.False(t, list.More)
				assert.Equal(t, 0, len(list.Items))
			}
		}
	})
}

func TestListObjects_EncryptionBypass(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 0, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		encStore := newTestEncStore(TestEncKey)
		encStore.SetDefaultPathCipher(storj.EncAESGCM)

		db, streams, cleanup, err := newMetainfoParts(planet, encStore)
		require.NoError(t, err)
		defer ctx.Check(cleanup)

		bucket, err := db.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)

		objectKeys := []string{
			"a", "aa", "b", "bb", "c",
			"a/xa", "a/xaa", "a/xb", "a/xbb", "a/xc",
			"b/ya", "b/yaa", "b/yb", "b/ybb", "b/yc",
		}

		for _, key := range objectKeys {
			upload(ctx, t, db, streams, bucket.Name, key, nil)
		}
		sort.Strings(objectKeys)

		// Enable encryption bypass
		encStore.EncryptionBypass = true

		opts := options("", "", 0)
		opts.Recursive = true
		encodedList, err := db.ListObjects(ctx, bucket.Name, opts)
		require.NoError(t, err)
		require.Equal(t, len(objectKeys), len(encodedList.Items))

		seenPaths := make(map[string]struct{})
		for _, item := range encodedList.Items {
			iter := paths.NewUnencrypted(item.Path).Iterator()
			var decoded, next string
			for !iter.Done() {
				next = iter.Next()

				decodedNextBytes, err := base64.URLEncoding.DecodeString(next)
				require.NoError(t, err)

				decoded += string(decodedNextBytes) + "/"
			}
			decoded = strings.TrimRight(decoded, "/")
			encryptedObjectKey := paths.NewEncrypted(decoded)

			decryptedPath, err := encryption.DecryptPathWithStoreCipher(bucket.Name, encryptedObjectKey, encStore)
			require.NoError(t, err)

			// NB: require decrypted path is a member of `filePaths`.
			result := sort.Search(len(objectKeys), func(i int) bool {
				return !paths.NewUnencrypted(objectKeys[i]).Less(decryptedPath)
			})
			require.NotEqual(t, len(objectKeys), result)

			// NB: ensure each path is only seen once.
			_, ok := seenPaths[decryptedPath.String()]
			require.False(t, ok)

			seenPaths[decryptedPath.String()] = struct{}{}
		}
	})
}

func TestListObjects(t *testing.T) {
	runTestWithPathCipher(t, 0, storj.EncNull, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		bucket, err := db.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)

		objectKeys := []string{
			"a", "aa", "b", "bb", "c",
			"a/xa", "a/xaa", "a/xb", "a/xbb", "a/xc",
			"b/ya", "b/yaa", "b/yb", "b/ybb", "b/yc",
		}

		for _, key := range objectKeys {
			upload(ctx, t, db, streams, bucket.Name, key, nil)
		}

		otherBucket, err := db.CreateBucket(ctx, "otherbucket")
		require.NoError(t, err)

		upload(ctx, t, db, streams, otherBucket.Name, "file-in-other-bucket", nil)

		for i, tt := range []struct {
			options metaclient.ListOptions
			more    bool
			result  []string
		}{
			{
				options: options("", "", 0),
				result:  []string{"a", "a/", "aa", "b", "b/", "bb", "c"},
			},
			{
				options: options("", "`", 0),
				result:  []string{"a", "a/", "aa", "b", "b/", "bb", "c"},
			},
			{
				options: options("", "b", 0),
				result:  []string{"b/", "bb", "c"},
			},
			{
				options: options("", "c", 0),
				result:  []string{},
			},
			{
				options: options("", "ca", 0),
				result:  []string{},
			},
			{
				options: options("", "", 1),
				more:    true,
				result:  []string{"a"},
			},
			{
				options: options("", "`", 1),
				more:    true,
				result:  []string{"a"},
			},
			{
				options: options("", "aa", 1),
				more:    true,
				result:  []string{"b"},
			},
			{
				options: options("", "c", 1),
				result:  []string{},
			},
			{
				options: options("", "ca", 1),
				result:  []string{},
			},
			{
				options: options("", "", 2),
				more:    true,
				result:  []string{"a", "a/"},
			},
			{
				options: options("", "`", 2),
				more:    true,
				result:  []string{"a", "a/"},
			},
			{
				options: options("", "aa", 2),
				more:    true,
				result:  []string{"b", "b/"},
			},
			{
				options: options("", "bb", 2),
				result:  []string{"c"},
			},
			{
				options: options("", "c", 2),
				result:  []string{},
			},
			{
				options: options("", "ca", 2),
				result:  []string{},
			},
			{
				options: optionsRecursive("", "", 0),
				result:  []string{"a", "a/xa", "a/xaa", "a/xb", "a/xbb", "a/xc", "aa", "b", "b/ya", "b/yaa", "b/yb", "b/ybb", "b/yc", "bb", "c"},
			},
			{
				options: options("a/", "", 0),
				result:  []string{"xa", "xaa", "xb", "xbb", "xc"},
			},
			{
				options: options("a/", "xb", 0),
				result:  []string{"xbb", "xc"},
			},
			{
				options: optionsRecursive("", "a/xbb", 5),
				more:    true,
				result:  []string{"a/xc", "aa", "b", "b/ya", "b/yaa"},
			},
			{
				options: options("a/", "xaa", 2),
				more:    true,
				result:  []string{"xb", "xbb"},
			},
		} {
			errTag := fmt.Sprintf("%d. %+v", i, tt)

			list, err := db.ListObjects(ctx, bucket.Name, tt.options)

			if assert.NoError(t, err, errTag) {
				assert.Equal(t, tt.more, list.More, errTag)
				if assert.Len(t, list.Items, len(tt.result), errTag) {
					for i, item := range list.Items {
						assert.Equal(t, tt.result[i], item.Path, errTag)
						assert.Equal(t, TestBucket, item.Bucket.Name, errTag)
					}
				}
			}
		}
	})
}

func TestListObjects_PagingWithDiffPassphrase(t *testing.T) {
	runTestWithPathCipher(t, 0, storj.EncAESGCM, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, dbA *metaclient.DB, streams *streams.Store) {
		bucket, err := dbA.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)

		objectKeys := []string{
			"a", "aa", "b", "bb", "c",
			"a/xa", "a/xaa", "a/xb", "a/xbb", "a/xc",
			"b/ya", "b/yaa", "b/yb", "b/ybb", "b/yc",
		}

		for _, key := range objectKeys {
			upload(ctx, t, dbA, streams, bucket.Name, key, nil)
		}

		// Upload one object with different passphrase
		encAccess := newTestEncStore(TestEncKey + "different")
		encAccess.SetDefaultPathCipher(storj.EncAESGCM)

		dbB, streams, cleanup, err := newMetainfoParts(planet, encAccess)
		require.NoError(t, err)
		defer func() {
			err := cleanup()
			assert.NoError(t, err)
		}()
		upload(ctx, t, dbB, streams, bucket.Name, "object_with_different_passphrase", nil)

		for i, tt := range []struct {
			options metaclient.ListOptions
			more    bool
			result  []string
			db      *metaclient.DB
		}{
			{
				options: optionsRecursive("", "", 1),
				result:  []string{"object_with_different_passphrase"},
				db:      dbB,
				more:    true,
			},
		} {
			errTag := fmt.Sprintf("%d. %+v", i, tt)

			list, err := tt.db.ListObjects(ctx, bucket.Name, tt.options)

			if assert.NoError(t, err, errTag) {
				assert.Equal(t, tt.more, list.More, errTag)
				if assert.Len(t, list.Items, len(tt.result), errTag) {
					for i, item := range list.Items {
						assert.Equal(t, tt.result[i], item.Path, errTag)
						assert.Equal(t, TestBucket, item.Bucket.Name, errTag)
					}
				}
			}
		}
	})
}

func options(prefix, cursor string, limit int) metaclient.ListOptions {
	return metaclient.ListOptions{
		Prefix:    prefix,
		Cursor:    cursor,
		Direction: metaclient.After,
		Limit:     limit,
	}
}

func optionsRecursive(prefix, cursor string, limit int) metaclient.ListOptions {
	return metaclient.ListOptions{
		Prefix:    prefix,
		Cursor:    cursor,
		Direction: metaclient.After,
		Limit:     limit,
		Recursive: true,
	}
}
