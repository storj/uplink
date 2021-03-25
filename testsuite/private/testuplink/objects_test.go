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
	"storj.io/uplink/private/metainfo"
	"storj.io/uplink/private/storage/streams"
	"storj.io/uplink/private/stream"
)

const TestFile = "test-file"

func TestCreateObject(t *testing.T) {
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
		bucket, err := db.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)

		for i, tt := range []struct {
			create *metainfo.CreateObject
		}{
			{
				create: nil,
			},
			{
				create: &metainfo.CreateObject{},
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
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
		bucket, err := db.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)
		upload(ctx, t, db, streams, bucket.Name, TestFile, nil)

		_, err = db.GetObject(ctx, "", "")
		assert.True(t, metainfo.ErrNoBucket.Has(err))

		_, err = db.GetObject(ctx, bucket.Name, "")
		assert.True(t, metainfo.ErrNoPath.Has(err))

		_, err = db.GetObject(ctx, "non-existing-bucket", TestFile)
		assert.True(t, metainfo.ErrObjectNotFound.Has(err))

		_, err = db.GetObject(ctx, bucket.Name, "non-existing-file")
		assert.True(t, metainfo.ErrObjectNotFound.Has(err))

		object, err := db.GetObject(ctx, bucket.Name, TestFile)
		if assert.NoError(t, err) {
			assert.Equal(t, TestFile, object.Path)
			assert.Equal(t, TestBucket, object.Bucket.Name)
		}
	})
}

func TestDownloadObject(t *testing.T) {
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
		data := testrand.Bytes(32 * memory.KiB)

		bucket, err := db.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)

		emptyFile := upload(ctx, t, db, streams, bucket.Name, "empty-file", nil)
		smallFile := upload(ctx, t, db, streams, bucket.Name, "small-file", []byte("test"))
		largeFile := upload(ctx, t, db, streams, bucket.Name, "large-file", data)

		_, err = db.GetObject(ctx, "", "")
		assert.True(t, metainfo.ErrNoBucket.Has(err))

		_, err = db.GetObject(ctx, bucket.Name, "")
		assert.True(t, metainfo.ErrNoPath.Has(err))

		assertData(ctx, t, db, streams, bucket, emptyFile, []byte{})
		assertData(ctx, t, db, streams, bucket, smallFile, []byte("test"))
		assertData(ctx, t, db, streams, bucket, largeFile, data)

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

func upload(ctx context.Context, t *testing.T, db *metainfo.DB, streams *streams.Store, bucket, key string, data []byte) metainfo.Object {
	obj, err := db.CreateObject(ctx, bucket, key, nil)
	require.NoError(t, err)

	str, err := obj.CreateStream(ctx)
	require.NoError(t, err)

	upload := stream.NewUpload(ctx, str, streams)

	_, err = upload.Write(data)
	require.NoError(t, err)

	err = upload.Close()
	require.NoError(t, err)

	info, err := db.GetObject(ctx, bucket, key)
	require.NoError(t, err)

	return info
}

func assertData(ctx context.Context, t *testing.T, db *metainfo.DB, streams *streams.Store, bucket metainfo.Bucket, object metainfo.Object, content []byte) {
	download := stream.NewDownload(ctx, object, streams)
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
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
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

		unencryptedPath := paths.NewUnencrypted(TestFile)
		encryptedPath, err := encryption.EncryptPathWithStoreCipher(bucket.Name, unencryptedPath, encStore)
		require.NoError(t, err)

		for i, key := range []string{unencryptedPath.String(), encryptedPath.String()} {
			upload(ctx, t, db, streams, bucket.Name, key, nil)

			if i < 0 {
				// Enable encryption bypass
				encStore.EncryptionBypass = true
			}

			_, err = db.DeleteObject(ctx, "", "")
			assert.True(t, metainfo.ErrNoBucket.Has(err))

			_, err = db.DeleteObject(ctx, bucket.Name, "")
			assert.True(t, metainfo.ErrNoPath.Has(err))

			_, err = db.DeleteObject(ctx, bucket.Name+"-not-exist", TestFile)
			assert.Nil(t, err)

			_, err = db.DeleteObject(ctx, bucket.Name, "non-existing-file")
			assert.Nil(t, err)

			object, err := db.DeleteObject(ctx, bucket.Name, key)
			if assert.NoError(t, err) {
				assert.Equal(t, key, object.Path)
			}
		}
	})
}

func TestListObjectsEmpty(t *testing.T) {
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
		testBucketInfo, err := db.CreateBucket(ctx, TestBucket)
		require.NoError(t, err)

		_, err = db.ListObjects(ctx, "", metainfo.ListOptions{})
		assert.True(t, metainfo.ErrNoBucket.Has(err))

		_, err = db.ListObjects(ctx, testBucketInfo.Name, metainfo.ListOptions{})
		assert.EqualError(t, err, "metainfo: invalid direction 0")

		// TODO for now we are supporting only metainfo.After
		for _, direction := range []metainfo.ListDirection{
			// metainfo.Forward,
			metainfo.After,
		} {
			list, err := db.ListObjects(ctx, testBucketInfo.Name, metainfo.ListOptions{Direction: direction})
			if assert.NoError(t, err) {
				assert.False(t, list.More)
				assert.Equal(t, 0, len(list.Items))
			}
		}
	})
}

func TestListObjects_EncryptionBypass(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
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
			encryptedPath := paths.NewEncrypted(decoded)

			decryptedPath, err := encryption.DecryptPathWithStoreCipher(bucket.Name, encryptedPath, encStore)
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
	runTestWithPathCipher(t, storj.EncNull, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
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
			options metainfo.ListOptions
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

func options(prefix, cursor string, limit int) metainfo.ListOptions {
	return metainfo.ListOptions{
		Prefix:    prefix,
		Cursor:    cursor,
		Direction: metainfo.After,
		Limit:     limit,
	}
}

func optionsRecursive(prefix, cursor string, limit int) metainfo.ListOptions {
	return metainfo.ListOptions{
		Prefix:    prefix,
		Cursor:    cursor,
		Direction: metainfo.After,
		Limit:     limit,
		Recursive: true,
	}
}
