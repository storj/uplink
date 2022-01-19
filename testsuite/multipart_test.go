// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite/metabase"
	"storj.io/uplink"
	"storj.io/uplink/private/testuplink"
)

// TODO add more tests with ETag

func TestBeginUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = project.BeginUpload(ctx, "not-existing-testbucket", "multipart-object", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		createBucket(t, ctx, project, "testbucket")

		// assert there is no pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil)

		info, err := project.BeginUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		// assert there is only one pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil, "multipart-object")

		// we allow to start several multipart uploads for the same key
		_, err = project.BeginUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		info, err = project.BeginUpload(ctx, "testbucket", "multipart-object-1", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		// assert there are two pending multipart uploads
		assertUploadList(ctx, t, project, "testbucket", nil, "multipart-object", "multipart-object-1")
	})
}

func TestBeginUpload_Expires(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			// Reconfigure RS for ensuring that we don't have long-tail cancellations
			Satellite: testplanet.Combine(
				testplanet.ReconfigureRS(2, 2, 4, 4),
			),
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		expiresAt := time.Now().Add(time.Hour)
		info, err := project.BeginUpload(ctx, "testbucket", "multipart-object", &uplink.UploadOptions{
			Expires: expiresAt,
		})
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		upload, err := project.UploadPart(ctx, "testbucket", "multipart-object", info.UploadID, 1)
		require.NoError(t, err)
		_, err = upload.Write(testrand.Bytes(5 * memory.KiB))
		require.NoError(t, err)
		require.NoError(t, upload.Commit())

		// assert there is one pending multipart upload and it has an expiration date
		assertUploadList(ctx, t, project, "testbucket", nil, "multipart-object")
		list := project.ListUploads(ctx, "testbucket", &uplink.ListUploadsOptions{
			System: true,
		})
		require.NoError(t, list.Err())
		require.True(t, list.Next())
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		require.Equal(t, "multipart-object", list.Item().Key)
		require.NotZero(t, list.Item().System.Expires)
		require.Equal(t, expiresAt.Unix(), list.Item().System.Expires.Unix())
		require.False(t, list.Next())
		require.NoError(t, list.Err())
		require.Nil(t, list.Item())

		// check that storage nodes have pieces that will expire
		for _, sn := range planet.StorageNodes {
			expiredInfos, err := sn.Storage2.Store.GetExpired(ctx, expiresAt.Add(time.Hour), 100)
			require.NoError(t, err)
			require.NotEmpty(t, expiredInfos)
		}
	})
}

func TestUploadPart(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		newCtx := testuplink.WithMaxSegmentSize(ctx, 50*memory.KiB)

		project, err := planet.Uplinks[0].OpenProject(newCtx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		// bucket not exists
		_, err = project.BeginUpload(newCtx, "non-existing-testbucket", "multipart-object", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		info, err := project.BeginUpload(newCtx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		// assert there is only one pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil, "multipart-object")

		{
			_, err = project.UploadPart(newCtx, "", "multipart-object", info.UploadID, 0)
			require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

			_, err = project.UploadPart(newCtx, "testbucket", "", info.UploadID, 0)
			require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

			// empty streamID
			_, err = project.UploadPart(newCtx, "testbucket", "multipart-object", "", 1)
			require.Error(t, err)

			// empty input data reader: committing objects with zero-byte part
			// sizes shouldn't return an error.
			upload, err := project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 0)
			require.NoError(t, err)
			err = upload.Commit()
			require.NoError(t, err)
		}

		randData := testrand.Bytes(memory.Size(100+testrand.Intn(500)) * memory.KiB)
		// inline segment
		remoteInlineSource := randData[:3*memory.KiB.Int()]
		// single segment
		remoteSource1 := randData[len(remoteInlineSource) : len(remoteInlineSource)+10*memory.KiB.Int()]
		// multiple segments
		remoteSource2 := randData[len(remoteInlineSource)+len(remoteSource1):]

		upload, err := project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 1)
		require.NoError(t, err)
		_, err = upload.Write(remoteSource1)
		require.NoError(t, err)
		err = upload.Commit()
		require.NoError(t, err)

		segmentsBefore, err := planet.Satellites[0].Metabase.DB.TestingAllSegments(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(segmentsBefore))

		// start uploading part but abort before committing
		upload, err = project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 2)
		require.NoError(t, err)
		_, err = upload.Write(remoteSource2)
		require.NoError(t, err)
		err = upload.Abort()
		require.NoError(t, err)

		// if abort from previous upload will fail we will see here error that segment already exists
		// but that should not happen
		upload, err = project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 2)
		require.NoError(t, err)
		_, err = upload.Write(remoteSource2)
		require.NoError(t, err)
		err = upload.Commit()
		require.NoError(t, err)

		upload, err = project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 0)
		require.NoError(t, err)
		_, err = upload.Write(remoteInlineSource)
		require.NoError(t, err)
		err = upload.Commit()
		require.NoError(t, err)

		// it should be possible to upload zero-byte part as a last one
		upload, err = project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 3)
		require.NoError(t, err)
		err = upload.Commit()
		require.NoError(t, err)

		// all requests after committing should return errors
		_, err = upload.Write([]byte{1})
		require.Error(t, err)
		err = upload.SetETag([]byte{1})
		require.Error(t, err)
		err = upload.Commit()
		require.Error(t, err)
		err = upload.Abort()
		require.Error(t, err)

		// TODO verify that segment (1, 1) is inline

		_, err = project.CommitUpload(newCtx, "testbucket", "multipart-object", info.UploadID, nil)
		require.NoError(t, err)

		// assert there is no pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil)

		download, err := project.DownloadObject(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		defer ctx.Check(download.Close)

		downloaded, err := ioutil.ReadAll(download)
		require.NoError(t, err)
		require.Equal(t, len(randData), len(downloaded))
		require.Equal(t, randData, downloaded)

		// create part for committed object
		upload, err = project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 0)
		require.NoError(t, err)
		_, err = upload.Write([]byte("test"))
		require.NoError(t, err)
		err = upload.Commit()
		require.Error(t, err)
	})
}

func TestCheckMetadataWhileUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1,
		UplinkCount:    1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		info, err := project.BeginUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		_, err = project.CommitUpload(ctx, "testbucket", "multipart-object", info.UploadID, nil)
		require.NoError(t, err)

		obs, err := planet.Satellites[0].Metabase.DB.TestingAllObjects(ctx)
		require.NoError(t, err)
		require.Nil(t, obs[0].EncryptedMetadataNonce)
		require.Nil(t, obs[0].EncryptedMetadataNonce)
		require.Nil(t, obs[0].EncryptedMetadataNonce)

		_, err = project.DeleteObject(ctx, "testbucket", "multipart-object")
		require.NoError(t, err)

		info2, err := project.BeginUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		metadata := make(map[string]string)
		metadata["key1"] = "value1"

		_, err = project.CommitUpload(ctx, "testbucket", "multipart-object", info2.UploadID, &uplink.CommitUploadOptions{CustomMetadata: metadata})
		require.NoError(t, err)

		obs, err = planet.Satellites[0].Metabase.DB.TestingAllObjects(ctx)
		require.NoError(t, err)
		require.NotNil(t, obs[0].EncryptedMetadataNonce)
		require.NotNil(t, obs[0].EncryptedMetadata)
		require.NotNil(t, obs[0].EncryptedMetadataEncryptedKey)
	})
}

func TestUploadPart_ETag(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		segmentSize := 10 * memory.KiB
		newCtx := testuplink.WithMaxSegmentSize(ctx, segmentSize)

		project, err := planet.Uplinks[0].OpenProject(newCtx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		info, err := project.BeginUpload(newCtx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		randData := testrand.Bytes(5 * memory.KiB)
		upload, err := project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 1)
		require.NoError(t, err)
		_, err = upload.Write(randData)
		require.NoError(t, err)
		etag := sha256.Sum256(randData)
		err = upload.SetETag(etag[:])
		require.NoError(t, err)
		err = upload.Commit()
		require.NoError(t, err)

		// all requests after committing should return errors
		_, err = upload.Write([]byte{1})
		require.Error(t, err)
		err = upload.SetETag([]byte{1})
		require.Error(t, err)
		err = upload.Commit()
		require.Error(t, err)
		err = upload.Abort()
		require.Error(t, err)

		part := upload.Info()
		require.EqualValues(t, 1, part.PartNumber)
		require.EqualValues(t, len(randData), part.Size)
		require.Equal(t, etag[:], part.ETag)

		_, err = project.CommitUpload(newCtx, "testbucket", "multipart-object", info.UploadID, nil)
		require.NoError(t, err)

		iterator := project.ListUploadParts(ctx, "testbucket", "multipart-object", info.UploadID, nil)
		require.True(t, iterator.Next())
		item := iterator.Item()
		require.Equal(t, etag[:], item.ETag)

		// verify that ETags from last segments in parts are decrypted correctly
		objectKey := "multipart-object-many-parts"
		info, err = project.BeginUpload(newCtx, "testbucket", objectKey, nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		testCases := []int{
			1 * segmentSize.Int(),
			2 * segmentSize.Int(),
			3 * segmentSize.Int(),
			4 * segmentSize.Int(),
			// + 1KiB to have last segment not equal to maximum segment size
			5*segmentSize.Int() + memory.KiB.Int(),

			// TODO currently we are not supporting zero-byte part at the end
			// 0,
		}

		for i, partSize := range testCases {
			part := i + 1
			randData := testrand.BytesInt(partSize)
			upload, err := project.UploadPart(newCtx, "testbucket", objectKey, info.UploadID, uint32(part))
			require.NoError(t, err)
			_, err = upload.Write(randData)
			require.NoError(t, err)

			err = upload.SetETag([]byte{33, byte(part)})
			require.NoError(t, err)

			err = upload.Commit()
			require.NoError(t, err)
		}

		iterator = project.ListUploadParts(ctx, "testbucket", objectKey, info.UploadID, nil)

		parts := 0
		for iterator.Next() {
			item := iterator.Item()
			require.Equal(t, []byte{33, byte(item.PartNumber)}, item.ETag)
			parts++
		}
		require.NoError(t, iterator.Err())
		require.Equal(t, len(testCases), parts)

		// TODO add more cases
	})
}

func TestUploadPart_CheckNoEmptyInlineSegment(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		newCtx := testuplink.WithMaxSegmentSize(ctx, 10*memory.KiB)

		project, err := planet.Uplinks[0].OpenProject(newCtx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		bucket := "testbucket"
		objectKey := "multipart-object"

		createBucket(t, ctx, project, bucket)

		info, err := project.BeginUpload(newCtx, bucket, objectKey, nil)
		require.NoError(t, err)
		require.NotEmpty(t, info.UploadID)

		expectedData := testrand.Bytes(30 * memory.KiB)
		upload, err := project.UploadPart(newCtx, bucket, objectKey, info.UploadID, 1)
		require.NoError(t, err)
		_, err = upload.Write(expectedData)
		require.NoError(t, err)
		err = upload.Commit()
		require.NoError(t, err)

		_, err = project.CommitUpload(newCtx, bucket, objectKey, info.UploadID, nil)
		require.NoError(t, err)

		data, err := planet.Uplinks[0].Download(newCtx, planet.Satellites[0], bucket, objectKey)
		require.NoError(t, err)
		require.Equal(t, expectedData, data)

		// verify that part has 3 segments and no empty inline segment at the end
		objects, err := planet.Satellites[0].Metabase.DB.TestingAllCommittedObjects(ctx, planet.Uplinks[0].Projects[0].ID, bucket)
		require.NoError(t, err)
		require.Len(t, objects, 1)

		segments, err := planet.Satellites[0].Metabase.DB.TestingAllObjectSegments(ctx, metabase.ObjectLocation{
			ProjectID:  planet.Uplinks[0].Projects[0].ID,
			BucketName: bucket,
			ObjectKey:  objects[0].ObjectKey,
		})
		require.NoError(t, err)
		require.Len(t, segments, 3)
		require.NotZero(t, segments[2].PlainSize)
	})
}

func TestDownloadObjectWithManySegments(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		info, err := project.BeginUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)

		expectedData := []byte{}

		// Current max limit for segments listing is 1000, we need
		// to check if more than that will be handlel correctly
		for i := 0; i < 1010; i++ {
			expectedData = append(expectedData, byte(i))

			// TODO maybe put it in parallel
			upload, err := project.UploadPart(ctx, "testbucket", "multipart-object", info.UploadID, uint32(i+1))
			require.NoError(t, err)
			_, err = upload.Write([]byte{byte(i)})
			require.NoError(t, err)
			err = upload.Commit()
			require.NoError(t, err)
		}

		_, err = project.CommitUpload(ctx, "testbucket", "multipart-object", info.UploadID, nil)
		require.NoError(t, err)

		data, err := planet.Uplinks[0].Download(ctx, planet.Satellites[0], "testbucket", "multipart-object")
		require.NoError(t, err)
		require.Equal(t, expectedData, data)
	})
}

func TestAbortUpload_Multipart(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		info, err := project.BeginUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		// assert there is only one pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil, "multipart-object")

		err = project.AbortUpload(ctx, "", "multipart-object", info.UploadID)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

		err = project.AbortUpload(ctx, "testbucket", "", info.UploadID)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

		err = project.AbortUpload(ctx, "testbucket", "multipart-object", "")
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrUploadIDInvalid))

		// TODO: testcases we cannot do now:
		// - right streamID/wrong bucket or project ID
		// - existing bucket/existing key/existing streamID, but not the good one

		err = project.AbortUpload(ctx, "testbucket", "multipart-object", info.UploadID)
		require.NoError(t, err)

		// assert there is no pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil)
	})
}

func TestListUploads_NonExistingBucket(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		list := project.ListUploads(ctx, "non-existing-bucket", nil)
		require.NoError(t, list.Err())
		require.Nil(t, list.Item())
		require.False(t, list.Next())
		require.Error(t, list.Err())
		require.True(t, errors.Is(list.Err(), uplink.ErrBucketNotFound))
	})
}

func TestListUploads_EmptyBucket(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		// assert the there is no pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil)
	})
}

func TestListUploads_Prefix(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		_, err = project.BeginUpload(ctx, "testbucket", "a/b/c/multipart-object", nil)
		require.NoError(t, err)

		// assert there is one pending multipart upload with prefix "a/b/"
		assertUploadList(ctx, t, project, "testbucket", &uplink.ListUploadsOptions{
			Prefix:    "a/b/",
			Recursive: true,
		}, "a/b/c/multipart-object")

		// assert there is no pending multipart upload with prefix "b/"
		assertUploadList(ctx, t, project, "testbucket", &uplink.ListUploadsOptions{
			Prefix:    "b/",
			Recursive: true,
		})

		// assert there is one prefix of pending multipart uploads with prefix "a/b/"
		list := project.ListUploads(ctx, "testbucket", &uplink.ListUploadsOptions{
			Prefix: "a/b/",
		})
		require.NoError(t, list.Err())
		require.True(t, list.Next())
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.True(t, list.Item().IsPrefix)
		require.Equal(t, "a/b/c/", list.Item().Key)
		require.False(t, list.Next())
		require.NoError(t, list.Err())
		require.Nil(t, list.Item())
	})
}

func TestListUploads_Cursor(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		expectedObjects := map[string]bool{
			"multipart-upload-1": true,
			"multipart-upload-2": true,
		}

		for object := range expectedObjects {
			_, err := project.BeginUpload(ctx, "testbucket", object, nil)
			require.NoError(t, err)
		}

		// get the first list item and make it a cursor for the next list request
		list := project.ListUploads(ctx, "testbucket", nil)
		require.NoError(t, list.Err())
		more := list.Next()
		require.True(t, more)
		require.NoError(t, list.Err())
		delete(expectedObjects, list.Item().Key)
		cursor := list.Item().Key

		// list again with cursor set to the first item from previous list request
		list = project.ListUploads(ctx, "testbucket", &uplink.ListUploadsOptions{Cursor: cursor})
		require.NoError(t, list.Err())

		// expect the second item as the first item in this new list request
		more = list.Next()
		require.True(t, more)
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		delete(expectedObjects, list.Item().Key)

		require.Empty(t, expectedObjects)
		require.False(t, list.Next())
		require.NoError(t, list.Err())
		require.Nil(t, list.Item())
	})
}

func TestListUploadParts(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		newCtx := testuplink.WithMaxSegmentSize(ctx, 50*memory.KiB)

		project, err := planet.Uplinks[0].OpenProject(newCtx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		randData := testrand.Bytes(memory.Size(100+testrand.Intn(500)) * memory.KiB)
		part0size := len(randData) * 3 / 10
		part4size := len(randData) - part0size
		source0 := randData[:part0size]
		source0SHA256 := sha256.Sum256(source0)
		source4 := randData[part0size:]
		source4SHA256 := sha256.Sum256(source4)

		info, err := project.BeginUpload(newCtx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		// assert there is only one pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil, "multipart-object")

		{
			iterator := project.ListUploadParts(newCtx, "", "multipart-object", info.UploadID, nil)
			require.False(t, iterator.Next())
			require.True(t, errors.Is(iterator.Err(), uplink.ErrBucketNameInvalid))

			iterator = project.ListUploadParts(newCtx, "testbucket", "", info.UploadID, nil)
			require.False(t, iterator.Next())
			require.True(t, errors.Is(iterator.Err(), uplink.ErrObjectKeyInvalid))

			// empty streamID
			iterator = project.ListUploadParts(newCtx, "testbucket", "multipart-object", "", nil)
			require.False(t, iterator.Next())
			require.Error(t, iterator.Err())

			// TODO add test case for bucket/key that doesn't match UploadID
		}

		// list multipart upload with no uploaded parts
		iterator := project.ListUploadParts(ctx, "testbucket", "multipart-object", info.UploadID, nil)
		require.False(t, iterator.Next())
		require.NoError(t, iterator.Err())

		uploadTime := time.Now()
		upload, err := project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 0)
		require.NoError(t, err)
		_, err = upload.Write(source0)
		require.NoError(t, err)
		require.NoError(t, upload.SetETag(source0SHA256[:]))
		require.NoError(t, upload.Commit())

		upload, err = project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 4)
		require.NoError(t, err)
		_, err = upload.Write(source4)
		require.NoError(t, err)
		require.NoError(t, upload.SetETag(source4SHA256[:]))
		require.NoError(t, upload.Commit())

		// list parts of on going multipart upload
		iterator = project.ListUploadParts(ctx, "testbucket", "multipart-object", info.UploadID, nil)

		require.True(t, iterator.Next())
		part01 := iterator.Item()

		require.EqualValues(t, 0, part01.PartNumber)
		require.Equal(t, int64(part0size), part01.Size)
		require.WithinDuration(t, uploadTime, part01.Modified, 10*time.Second)
		require.Equal(t, source0SHA256[:], part01.ETag)

		require.True(t, iterator.Next())
		part04 := iterator.Item()
		require.EqualValues(t, 4, part04.PartNumber)
		require.Equal(t, int64(part4size), part04.Size)
		require.WithinDuration(t, uploadTime, part04.Modified, 10*time.Second)
		require.Equal(t, source4SHA256[:], part04.ETag)

		_, err = project.CommitUpload(newCtx, "testbucket", "multipart-object", info.UploadID, nil)
		require.NoError(t, err)

		// assert there is no pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil)

		// TODO we should not list parts for committed objects
		// list parts of a completed multipart upload
		// iterator = project.ListUploadParts(ctx, "testbucket", "multipart-object", info.UploadID, nil)
		// require.False(t, iterator.Next())
		// require.NoError(t, iterator.Err())

		// TODO: this should pass once we correctly handle the maxParts parameter
		// require.Equal(t, false, parts.More)
	})
}

func TestListUploadParts_Ordering(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		const partCount = 10
		data := testrand.Bytes(partCount + 1)

		info, err := project.BeginUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		for i := 0; i < partCount; i++ {
			upload, err := project.UploadPart(ctx, "testbucket", "multipart-object", info.UploadID, uint32(i))
			require.NoError(t, err)

			_, err = upload.Write(data[:i+1])
			require.NoError(t, err)
			require.NoError(t, upload.Commit())
		}

		_, err = project.CommitUpload(ctx, "testbucket", "multipart-object", info.UploadID, nil)
		require.NoError(t, err)

		// list parts with a cursor starting after all parts
		iterator := project.ListUploadParts(ctx, "testbucket", "multipart-object", info.UploadID, nil)

		for i := 0; i < partCount; i++ {
			require.True(t, iterator.Next())
			item := iterator.Item()
			require.EqualValues(t, i, item.PartNumber)
			require.EqualValues(t, i+1, item.Size)
		}

		// no more entries
		require.False(t, iterator.Next())
	})
}

func TestListUploadParts_Cursor(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		const partCount = 10
		data := testrand.Bytes(partCount + 1)

		info, err := project.BeginUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		for i := 0; i < partCount; i++ {
			upload, err := project.UploadPart(ctx, "testbucket", "multipart-object", info.UploadID, uint32(i))
			require.NoError(t, err)

			_, err = upload.Write(data[:i+1])
			require.NoError(t, err)
			require.NoError(t, upload.Commit())
		}

		_, err = project.CommitUpload(ctx, "testbucket", "multipart-object", info.UploadID, nil)
		require.NoError(t, err)

		cursor := 5
		// list parts with a cursor starting after all parts
		iterator := project.ListUploadParts(ctx, "testbucket", "multipart-object", info.UploadID, &uplink.ListUploadPartsOptions{
			Cursor: uint32(cursor),
		})
		for i := cursor + 1; i < partCount; i++ {
			require.True(t, iterator.Next())
			item := iterator.Item()
			require.EqualValues(t, i, item.PartNumber)
			require.EqualValues(t, i+1, item.Size)
		}

		// no more entries
		require.False(t, iterator.Next())
		require.NoError(t, iterator.Err())

		// list parts with a cursor starting after all parts
		iterator = project.ListUploadParts(ctx, "testbucket", "multipart-object", info.UploadID, &uplink.ListUploadPartsOptions{
			Cursor: 20,
		})
		require.False(t, iterator.Next())
		require.NoError(t, iterator.Err())
	})
}

func assertUploadList(ctx context.Context, t *testing.T, project *uplink.Project, bucket string, options *uplink.ListUploadsOptions, objectKeys ...string) {
	list := project.ListUploads(ctx, bucket, options)
	require.NoError(t, list.Err())
	require.Nil(t, list.Item())

	itemKeys := make(map[string]struct{})
	for list.Next() {
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		itemKeys[list.Item().Key] = struct{}{}
	}

	for _, objectKey := range objectKeys {
		if assert.Contains(t, itemKeys, objectKey) {
			delete(itemKeys, objectKey)
		}
	}

	require.Empty(t, itemKeys)

	require.False(t, list.Next())
	require.NoError(t, list.Err())
	require.Nil(t, list.Item())
}
