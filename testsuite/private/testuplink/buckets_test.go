// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package testuplink_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/infectious"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink/private/ecclient"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams"
)

const (
	TestEncKey = "test-encryption-key"
	TestBucket = "test-bucket"
)

func TestBucketsBasic(t *testing.T) {
	runTestWithoutSN(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		// Create new bucket
		bucket, err := db.CreateBucket(ctx, TestBucket)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}

		// Check that bucket list include the new bucket
		bucketList, err := db.ListBuckets(ctx, metaclient.BucketListOptions{Direction: metaclient.After})
		if assert.NoError(t, err) {
			assert.False(t, bucketList.More)
			assert.Equal(t, 1, len(bucketList.Items))
			assert.Equal(t, TestBucket, bucketList.Items[0].Name)
		}

		// Check that we can get the new bucket explicitly
		bucket, err = db.GetBucket(ctx, TestBucket)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}

		// Delete the bucket
		bucket, err = db.DeleteBucket(ctx, TestBucket, false)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}

		// Check that the bucket list is empty
		bucketList, err = db.ListBuckets(ctx, metaclient.BucketListOptions{Direction: metaclient.After})
		if assert.NoError(t, err) {
			assert.False(t, bucketList.More)
			assert.Equal(t, 0, len(bucketList.Items))
		}

		// Check that the bucket cannot be get explicitly
		_, err = db.GetBucket(ctx, TestBucket)
		assert.True(t, metaclient.ErrBucketNotFound.Has(err))
	})
}

func TestBucketsReadWrite(t *testing.T) {
	runTestWithoutSN(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		// Create new bucket
		bucket, err := db.CreateBucket(ctx, TestBucket)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}

		// Check that bucket list include the new bucket
		bucketList, err := db.ListBuckets(ctx, metaclient.BucketListOptions{Direction: metaclient.After})
		if assert.NoError(t, err) {
			assert.False(t, bucketList.More)
			assert.Equal(t, 1, len(bucketList.Items))
			assert.Equal(t, TestBucket, bucketList.Items[0].Name)
		}

		// Check that we can get the new bucket explicitly
		bucket, err = db.GetBucket(ctx, TestBucket)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}

		// Delete the bucket
		bucket, err = db.DeleteBucket(ctx, TestBucket, false)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}

		// Check that the bucket list is empty
		bucketList, err = db.ListBuckets(ctx, metaclient.BucketListOptions{Direction: metaclient.After})
		if assert.NoError(t, err) {
			assert.False(t, bucketList.More)
			assert.Equal(t, 0, len(bucketList.Items))
		}

		// Check that the bucket cannot be get explicitly
		_, err = db.GetBucket(ctx, TestBucket)
		assert.True(t, metaclient.ErrBucketNotFound.Has(err))
	})
}

func TestErrNoBucket(t *testing.T) {
	runTestWithoutSN(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		_, err := db.CreateBucket(ctx, "")
		assert.True(t, metaclient.ErrNoBucket.Has(err))

		_, err = db.GetBucket(ctx, "")
		assert.True(t, metaclient.ErrNoBucket.Has(err))

		_, err = db.DeleteBucket(ctx, "", false)
		assert.True(t, metaclient.ErrNoBucket.Has(err))
	})
}

func TestBucketDeleteAll(t *testing.T) {
	runTestWithoutSN(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		bucket, err := db.CreateBucket(ctx, TestBucket)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}

		// Check that we can get the new bucket explicitly
		bucket, err = db.GetBucket(ctx, TestBucket)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}

		// Upload an object to the bucket
		upload(ctx, t, db, streams, bucket.Name, "small-file", []byte("test"))

		// Force delete the bucket
		bucket, err = db.DeleteBucket(ctx, TestBucket, true)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}
	})
}

func TestListBucketsEmpty(t *testing.T) {
	runTestWithoutSN(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		bucketList, err := db.ListBuckets(ctx, metaclient.BucketListOptions{Direction: metaclient.Forward})
		if assert.NoError(t, err) {
			assert.False(t, bucketList.More)
			assert.Equal(t, 0, len(bucketList.Items))
		}
	})
}

func TestListBuckets(t *testing.T) {
	runTestWithoutSN(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metaclient.DB, streams *streams.Store) {
		bucketNames := []string{"a00", "aa0", "b00", "bb0", "c00"}

		for _, name := range bucketNames {
			_, err := db.CreateBucket(ctx, name)
			require.NoError(t, err)
		}

		for i, tt := range []struct {
			cursor string
			dir    metaclient.ListDirection
			limit  int
			more   bool
			result []string
		}{
			{cursor: "", dir: metaclient.Forward, limit: 0, more: false, result: []string{"a00", "aa0", "b00", "bb0", "c00"}},
			{cursor: "`", dir: metaclient.Forward, limit: 0, more: false, result: []string{"a00", "aa0", "b00", "bb0", "c00"}},
			{cursor: "b00", dir: metaclient.Forward, limit: 0, more: false, result: []string{"b00", "bb0", "c00"}},
			{cursor: "c00", dir: metaclient.Forward, limit: 0, more: false, result: []string{"c00"}},
			{cursor: "ca", dir: metaclient.Forward, limit: 0, more: false, result: []string{}},
			{cursor: "", dir: metaclient.Forward, limit: 1, more: true, result: []string{"a00"}},
			{cursor: "`", dir: metaclient.Forward, limit: 1, more: true, result: []string{"a00"}},
			{cursor: "aa0", dir: metaclient.Forward, limit: 1, more: true, result: []string{"aa0"}},
			{cursor: "c00", dir: metaclient.Forward, limit: 1, more: false, result: []string{"c00"}},
			{cursor: "ca", dir: metaclient.Forward, limit: 1, more: false, result: []string{}},
			{cursor: "", dir: metaclient.Forward, limit: 2, more: true, result: []string{"a00", "aa0"}},
			{cursor: "`", dir: metaclient.Forward, limit: 2, more: true, result: []string{"a00", "aa0"}},
			{cursor: "aa0", dir: metaclient.Forward, limit: 2, more: true, result: []string{"aa0", "b00"}},
			{cursor: "bb0", dir: metaclient.Forward, limit: 2, more: false, result: []string{"bb0", "c00"}},
			{cursor: "c00", dir: metaclient.Forward, limit: 2, more: false, result: []string{"c00"}},
			{cursor: "ca", dir: metaclient.Forward, limit: 2, more: false, result: []string{}},
		} {
			errTag := fmt.Sprintf("%d. %+v", i, tt)

			bucketList, err := db.ListBuckets(ctx, metaclient.BucketListOptions{
				Cursor:    tt.cursor,
				Direction: tt.dir,
				Limit:     tt.limit,
			})

			if assert.NoError(t, err, errTag) {
				assert.Equal(t, tt.more, bucketList.More, errTag)
				assert.Equal(t, tt.result, getBucketNames(bucketList), errTag)
			}
		}
	})
}

func getBucketNames(bucketList metaclient.BucketList) []string {
	names := make([]string, len(bucketList.Items))

	for i, item := range bucketList.Items {
		names[i] = item.Name
	}

	return names
}

func runTest(t *testing.T, test func(*testing.T, context.Context, *testplanet.Planet, *metaclient.DB, *streams.Store)) {
	runTestWithPathCipher(t, 4, storj.EncAESGCM, test)
}

func runTestWithoutSN(t *testing.T, test func(*testing.T, context.Context, *testplanet.Planet, *metaclient.DB, *streams.Store)) {
	runTestWithPathCipher(t, 0, storj.EncAESGCM, test)
}

func runTestWithPathCipher(t *testing.T, storageNodeCount int, pathCipher storj.CipherSuite, test func(*testing.T, context.Context, *testplanet.Planet, *metaclient.DB, *streams.Store)) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: storageNodeCount, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		encAccess := newTestEncStore(TestEncKey)
		encAccess.SetDefaultPathCipher(pathCipher)

		db, streams, cleanup, err := newMetainfoParts(planet, encAccess)
		require.NoError(t, err)
		defer ctx.Check(cleanup)

		test(t, ctx, planet, db, streams)
	})
}

func newTestEncStore(keyStr string) *encryption.Store {
	key := new(storj.Key)
	copy(key[:], keyStr)

	store := encryption.NewStore()
	store.SetDefaultKey(key)

	return store
}

func newMetainfoParts(planet *testplanet.Planet, encStore *encryption.Store) (_ *metaclient.DB, _ *streams.Store, _ func() error, err error) {
	apiKey := planet.Uplinks[0].APIKey[planet.Satellites[0].ID()]
	metainfoClient, err := planet.Uplinks[0].DialMetainfo(context.Background(), planet.Satellites[0], apiKey)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, metainfoClient.Close())
		}
	}()

	ec := ecclient.New(planet.Uplinks[0].Dialer, 0)
	fc, err := infectious.NewFEC(2, 4)
	if err != nil {
		return nil, nil, nil, err
	}

	rs, err := eestream.NewRedundancyStrategy(eestream.NewRSScheme(fc, 1*memory.KiB.Int()), 0, 0)
	if err != nil {
		return nil, nil, nil, err
	}

	const stripesPerBlock = 2

	encryptionParameters := storj.EncryptionParameters{
		BlockSize:   int32(stripesPerBlock * rs.StripeSize()),
		CipherSuite: storj.EncAESGCM,
	}
	inlineThreshold := 8 * memory.KiB.Int()
	streams, err := streams.NewStreamStore(metainfoClient, ec, 64*memory.MiB.Int64(), encStore, encryptionParameters, inlineThreshold, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	return metaclient.New(metainfoClient, storj.EncryptionParameters{}, encStore), streams, metainfoClient.Close, nil
}
