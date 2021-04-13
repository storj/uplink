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
	"storj.io/storj/satellite/metainfo/metabase"
	"storj.io/uplink"
	"storj.io/uplink/private/multipart"
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
		list := multipart.ListMultipartUploads(ctx, project, "testbucket", &multipart.ListMultipartUploadsOptions{
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

			// empty input data reader
			upload, err := project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 0)
			require.NoError(t, err)
			err = upload.Commit()
			require.Error(t, err)
		}

		randData := testrand.Bytes(memory.Size(100+testrand.Intn(500)) * memory.KiB)
		remoteInlineSource := randData[:51*memory.KiB.Int()]
		remoteSource1 := randData[len(remoteInlineSource) : len(remoteInlineSource)+10*memory.KiB.Int()]
		remoteSource2 := randData[len(remoteInlineSource)+len(remoteSource1):]

		upload, err := project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 1)
		require.NoError(t, err)
		_, err = upload.Write(remoteSource1)
		require.NoError(t, err)
		err = upload.Commit()
		require.NoError(t, err)

		upload, err = project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 2)
		require.NoError(t, err)
		_, err = upload.Write(remoteSource2)
		require.NoError(t, err)
		err = upload.Commit()
		require.NoError(t, err)

		// TODO aborting is not working correctly yet
		// upload, err = project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 3)
		// require.NoError(t, err)
		// _, err = upload.Write(remoteInlineSource)
		// require.NoError(t, err)
		// err = upload.Abort()
		// require.NoError(t, err)

		upload, err = project.UploadPart(newCtx, "testbucket", "multipart-object", info.UploadID, 0)
		require.NoError(t, err)
		_, err = upload.Write(remoteInlineSource)
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

func TestUploadPart_ETag(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		newCtx := testuplink.WithMaxSegmentSize(ctx, 10*memory.KiB)

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

		parts, err := multipart.ListObjectParts(ctx, project, "testbucket", "multipart-object", info.UploadID, 0, 100)
		require.NoError(t, err)
		require.Equal(t, etag[:], parts.Items[0].ETag)

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
		objects, err := planet.Satellites[0].Metainfo.Metabase.TestingAllCommittedObjects(ctx, planet.Uplinks[0].Projects[0].ID, bucket)
		require.NoError(t, err)
		require.Len(t, objects, 1)

		segments, err := planet.Satellites[0].Metainfo.Metabase.TestingAllObjectSegments(ctx, metabase.ObjectLocation{
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

func assertUploadList(ctx context.Context, t *testing.T, project *uplink.Project, bucket string, options *multipart.ListMultipartUploadsOptions, objectKeys ...string) {
	list := multipart.ListMultipartUploads(ctx, project, bucket, options)
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
