// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package testuplink_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vivint/infectious"
	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/macaroon"
	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite/console"
	"storj.io/uplink/private/ecclient"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/metainfo"
	"storj.io/uplink/private/storage/streams"
)

const (
	TestEncKey = "test-encryption-key"
	TestBucket = "test-bucket"
)

func TestBucketsBasic(t *testing.T) {
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
		// Create new bucket
		bucket, err := db.CreateBucket(ctx, TestBucket)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}

		// Check that bucket list include the new bucket
		bucketList, err := db.ListBuckets(ctx, storj.BucketListOptions{Direction: storj.After})
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
		bucketList, err = db.ListBuckets(ctx, storj.BucketListOptions{Direction: storj.After})
		if assert.NoError(t, err) {
			assert.False(t, bucketList.More)
			assert.Equal(t, 0, len(bucketList.Items))
		}

		// Check that the bucket cannot be get explicitly
		bucket, err = db.GetBucket(ctx, TestBucket)
		assert.True(t, storj.ErrBucketNotFound.Has(err))
	})
}

func TestBucketsReadWrite(t *testing.T) {
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
		// Create new bucket
		bucket, err := db.CreateBucket(ctx, TestBucket)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}

		// Check that bucket list include the new bucket
		bucketList, err := db.ListBuckets(ctx, storj.BucketListOptions{Direction: storj.After})
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
		bucketList, err = db.ListBuckets(ctx, storj.BucketListOptions{Direction: storj.After})
		if assert.NoError(t, err) {
			assert.False(t, bucketList.More)
			assert.Equal(t, 0, len(bucketList.Items))
		}

		// Check that the bucket cannot be get explicitly
		bucket, err = db.GetBucket(ctx, TestBucket)
		assert.True(t, storj.ErrBucketNotFound.Has(err))
	})
}

func TestErrNoBucket(t *testing.T) {
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
		_, err := db.CreateBucket(ctx, "")
		assert.True(t, storj.ErrNoBucket.Has(err))

		_, err = db.GetBucket(ctx, "")
		assert.True(t, storj.ErrNoBucket.Has(err))

		_, err = db.DeleteBucket(ctx, "", false)
		assert.True(t, storj.ErrNoBucket.Has(err))
	})
}

func TestBucketDeleteAll(t *testing.T) {
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
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
		upload(ctx, t, db, streams, bucket, "small-file", []byte("test"))

		// Force delete the bucket
		bucket, err = db.DeleteBucket(ctx, TestBucket, true)
		if assert.NoError(t, err) {
			assert.Equal(t, TestBucket, bucket.Name)
		}
	})
}

func TestListBucketsEmpty(t *testing.T) {
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
		bucketList, err := db.ListBuckets(ctx, storj.BucketListOptions{Direction: storj.Forward})
		if assert.NoError(t, err) {
			assert.False(t, bucketList.More)
			assert.Equal(t, 0, len(bucketList.Items))
		}
	})
}

func TestListBuckets(t *testing.T) {
	runTest(t, func(t *testing.T, ctx context.Context, planet *testplanet.Planet, db *metainfo.DB, streams *streams.Store) {
		bucketNames := []string{"a00", "aa0", "b00", "bb0", "c00"}

		for _, name := range bucketNames {
			_, err := db.CreateBucket(ctx, name)
			require.NoError(t, err)
		}

		for i, tt := range []struct {
			cursor string
			dir    storj.ListDirection
			limit  int
			more   bool
			result []string
		}{
			{cursor: "", dir: storj.Forward, limit: 0, more: false, result: []string{"a00", "aa0", "b00", "bb0", "c00"}},
			{cursor: "`", dir: storj.Forward, limit: 0, more: false, result: []string{"a00", "aa0", "b00", "bb0", "c00"}},
			{cursor: "b00", dir: storj.Forward, limit: 0, more: false, result: []string{"b00", "bb0", "c00"}},
			{cursor: "c00", dir: storj.Forward, limit: 0, more: false, result: []string{"c00"}},
			{cursor: "ca", dir: storj.Forward, limit: 0, more: false, result: []string{}},
			{cursor: "", dir: storj.Forward, limit: 1, more: true, result: []string{"a00"}},
			{cursor: "`", dir: storj.Forward, limit: 1, more: true, result: []string{"a00"}},
			{cursor: "aa0", dir: storj.Forward, limit: 1, more: true, result: []string{"aa0"}},
			{cursor: "c00", dir: storj.Forward, limit: 1, more: false, result: []string{"c00"}},
			{cursor: "ca", dir: storj.Forward, limit: 1, more: false, result: []string{}},
			{cursor: "", dir: storj.Forward, limit: 2, more: true, result: []string{"a00", "aa0"}},
			{cursor: "`", dir: storj.Forward, limit: 2, more: true, result: []string{"a00", "aa0"}},
			{cursor: "aa0", dir: storj.Forward, limit: 2, more: true, result: []string{"aa0", "b00"}},
			{cursor: "bb0", dir: storj.Forward, limit: 2, more: false, result: []string{"bb0", "c00"}},
			{cursor: "c00", dir: storj.Forward, limit: 2, more: false, result: []string{"c00"}},
			{cursor: "ca", dir: storj.Forward, limit: 2, more: false, result: []string{}},
		} {
			errTag := fmt.Sprintf("%d. %+v", i, tt)

			bucketList, err := db.ListBuckets(ctx, storj.BucketListOptions{
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

func getBucketNames(bucketList storj.BucketList) []string {
	names := make([]string, len(bucketList.Items))

	for i, item := range bucketList.Items {
		names[i] = item.Name
	}

	return names
}

func runTest(t *testing.T, test func(*testing.T, context.Context, *testplanet.Planet, *metainfo.DB, *streams.Store)) {
	runTestWithPathCipher(t, storj.EncAESGCM, test)
}

func runTestWithPathCipher(t *testing.T, pathCipher storj.CipherSuite, test func(*testing.T, context.Context, *testplanet.Planet, *metainfo.DB, *streams.Store)) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
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

func newMetainfoParts(planet *testplanet.Planet, encStore *encryption.Store) (_ *metainfo.DB, _ *streams.Store, _ func() error, err error) {
	// TODO(kaloyan): We should have a better way for configuring the Satellite's API Key
	// add project to satisfy constraint
	project, err := planet.Satellites[0].DB.Console().Projects().Insert(context.Background(), &console.Project{
		Name: "testProject",
	})
	if err != nil {
		return nil, nil, nil, err
	}

	apiKey, err := macaroon.NewAPIKey([]byte("testSecret"))
	if err != nil {
		return nil, nil, nil, err
	}

	apiKeyInfo := console.APIKeyInfo{
		ProjectID: project.ID,
		Name:      "testKey",
		Secret:    []byte("testSecret"),
	}

	// add api key to db
	_, err = planet.Satellites[0].DB.Console().APIKeys().Create(context.Background(), apiKey.Head(), apiKeyInfo)
	if err != nil {
		return nil, nil, nil, err
	}

	metainfoClient, err := planet.Uplinks[0].DialMetainfo(context.Background(), planet.Satellites[0], apiKey)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, metainfoClient.Close())
		}
	}()

	ec := ecclient.NewClient(planet.Uplinks[0].Log.Named("ecclient"), planet.Uplinks[0].Dialer, 0)
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
	streams, err := streams.NewStreamStore(metainfoClient, ec, 64*memory.MiB.Int64(), encStore, encryptionParameters, inlineThreshold)
	if err != nil {
		return nil, nil, nil, err
	}
	return metainfo.New(metainfoClient, encStore), streams, metainfoClient.Close, nil
}
