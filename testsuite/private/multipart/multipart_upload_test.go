// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package multipartupload_test

import (
	"bytes"
	"context"
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
	"storj.io/uplink/private/multipart"
	"storj.io/uplink/private/testuplink"
)

func TestNewMultipartUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = multipart.NewMultipartUpload(ctx, project, "not-existing-testbucket", "multipart-object", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		createBucket(t, ctx, project, "testbucket")

		// assert there is no pending multipart upload
		assertMultipartUploadList(ctx, t, project, "testbucket", nil)

		info, err := multipart.NewMultipartUpload(ctx, project, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		// assert there is only one pending multipart upload
		assertMultipartUploadList(ctx, t, project, "testbucket", nil, "multipart-object")

		// we allow to start several multipart uploads for the same key
		// TODO check why its not possible anymore
		// _, err = project.NewMultipartUpload(ctx, "testbucket", "multipart-object", nil)
		// require.NoError(t, err)
		// require.NotNil(t, info.StreamID)

		info, err = multipart.NewMultipartUpload(ctx, project, "testbucket", "multipart-object-1", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		// assert there are two pending multipart uploads
		assertMultipartUploadList(ctx, t, project, "testbucket", nil, "multipart-object", "multipart-object-1")
	})
}

func TestNewMultipartUpload_Expires(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = multipart.NewMultipartUpload(ctx, project, "not-existing-testbucket", "multipart-object", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		createBucket(t, ctx, project, "testbucket")

		// assert there is no pending multipart upload
		assertMultipartUploadList(ctx, t, project, "testbucket", nil)

		expiresAt := time.Now().Add(time.Hour)
		info, err := multipart.NewMultipartUpload(ctx, project, "testbucket", "multipart-object", &multipart.UploadOptions{
			Expires: expiresAt,
		})
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		// assert there is one pending multipart upload and it has an expiration date
		assertMultipartUploadList(ctx, t, project, "testbucket", nil, "multipart-object")
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
	})
}

func TestCompleteMultipartUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		{
			_, err := multipart.CompleteMultipartUpload(ctx, project, "", "", "", nil)
			require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

			_, err = multipart.CompleteMultipartUpload(ctx, project, "testbucket", "", "", nil)
			require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

			_, err = multipart.CompleteMultipartUpload(ctx, project, "testbucket", "multipart-object", "", nil)
			require.Error(t, err) // TODO should we create an error like ErrInvalidArgument
		}

		{
			info, err := multipart.NewMultipartUpload(ctx, project, "testbucket", "multipart-object", nil)
			require.NoError(t, err)
			require.NotNil(t, info.StreamID)

			// assert there is only one pending multipart upload
			assertMultipartUploadList(ctx, t, project, "testbucket", nil, "multipart-object")

			_, err = multipart.CompleteMultipartUpload(ctx, project, "testbucket", "multipart-object", info.StreamID, nil)
			require.NoError(t, err)

			// assert there is no pending multipart upload
			assertMultipartUploadList(ctx, t, project, "testbucket", nil)

			_, err = project.StatObject(ctx, "testbucket", "multipart-object")
			require.NoError(t, err)

			// object is already committed
			_, err = multipart.CompleteMultipartUpload(ctx, project, "testbucket", "multipart-object", info.StreamID, nil)
			require.Error(t, err)
		}

		{
			info, err := multipart.NewMultipartUpload(ctx, project, "testbucket", "multipart-object-metadata", nil)
			require.NoError(t, err)
			require.NotNil(t, info.StreamID)

			// assert there is only one pending multipart upload
			assertMultipartUploadList(ctx, t, project, "testbucket", nil, "multipart-object-metadata")

			expectedMetadata := uplink.CustomMetadata{
				"TestField1": "TestFieldValue1",
				"TestField2": "TestFieldValue2",
			}
			_, err = multipart.CompleteMultipartUpload(ctx, project, "testbucket", "multipart-object-metadata", info.StreamID, &multipart.ObjectOptions{
				CustomMetadata: expectedMetadata,
			})
			require.NoError(t, err)

			// assert there is no pending multipart upload
			assertMultipartUploadList(ctx, t, project, "testbucket", nil)

			object, err := project.StatObject(ctx, "testbucket", "multipart-object-metadata")
			require.NoError(t, err)
			require.Equal(t, expectedMetadata, object.Custom)
		}
		// TODO add more tests
	})
}

func TestAbortMultipartUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		info, err := multipart.NewMultipartUpload(ctx, project, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		// assert there is only one pending multipart upload
		assertMultipartUploadList(ctx, t, project, "testbucket", nil, "multipart-object")

		err = multipart.AbortMultipartUpload(ctx, project, "", "multipart-object", info.StreamID)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

		err = multipart.AbortMultipartUpload(ctx, project, "testbucket", "", info.StreamID)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

		err = multipart.AbortMultipartUpload(ctx, project, "testbucket", "multipart-object", "")
		require.Error(t, err)
		require.True(t, errors.Is(err, multipart.ErrStreamIDInvalid))

		// TODO: testcases we cannot do now:
		// - right streamID/wrong bucket or project ID
		// - existing bucket/existing key/existing streamID, but not the good one

		err = multipart.AbortMultipartUpload(ctx, project, "testbucket", "multipart-object", info.StreamID)
		require.NoError(t, err)

		// assert there is no pending multipart upload
		assertMultipartUploadList(ctx, t, project, "testbucket", nil)
	})
}

func TestPutObjectPart(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		newCtx := testuplink.WithMaxSegmentSize(ctx, 50*memory.KiB)

		projectInfo := planet.Uplinks[0].Projects[0]

		uplinkConfig := uplink.Config{}
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		project, err := uplinkConfig.OpenProject(newCtx, access)
		require.NoError(t, err)

		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		// bucket not exists
		_, err = multipart.NewMultipartUpload(newCtx, project, "non-existing-testbucket", "multipart-object", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		info, err := multipart.NewMultipartUpload(newCtx, project, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		// assert there is only one pending multipart upload
		assertMultipartUploadList(ctx, t, project, "testbucket", nil, "multipart-object")

		{
			_, err = multipart.PutObjectPart(newCtx, project, "", "multipart-object", info.StreamID, 0, nil)
			require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

			_, err = multipart.PutObjectPart(newCtx, project, "testbucket", "", info.StreamID, 0, nil)
			require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

			// empty streamID
			_, err = multipart.PutObjectPart(newCtx, project, "testbucket", "multipart-object", "", 1, nil)
			require.Error(t, err)

			// negative partID
			_, err = multipart.PutObjectPart(newCtx, project, "testbucket", "multipart-object", info.StreamID, -1, nil)
			require.Error(t, err)

			// empty input data reader
			_, err = multipart.PutObjectPart(newCtx, project, "testbucket", "multipart-object", info.StreamID, 0, bytes.NewBuffer([]byte{}))
			require.Error(t, err)
		}

		randData := testrand.Bytes(memory.Size(100+testrand.Intn(500)) * memory.KiB)
		remoteInlineSource := randData[:51*memory.KiB.Int()]
		remoteSource1 := randData[len(remoteInlineSource) : len(remoteInlineSource)+10*memory.KiB.Int()]
		remoteSource2 := randData[len(remoteInlineSource)+len(remoteSource1):]

		_, err = multipart.PutObjectPart(newCtx, project, "testbucket", "multipart-object", info.StreamID, 1, bytes.NewBuffer(remoteSource1))
		require.NoError(t, err)

		_, err = multipart.PutObjectPart(newCtx, project, "testbucket", "multipart-object", info.StreamID, 2, bytes.NewBuffer(remoteSource2))
		require.NoError(t, err)

		_, err = multipart.PutObjectPart(newCtx, project, "testbucket", "multipart-object", info.StreamID, 0, bytes.NewBuffer(remoteInlineSource))
		require.NoError(t, err)

		// TODO verify that segment (1, 1) is inline

		_, err = multipart.CompleteMultipartUpload(newCtx, project, "testbucket", "multipart-object", info.StreamID, nil)
		require.NoError(t, err)

		// assert there is no pending multipart upload
		assertMultipartUploadList(ctx, t, project, "testbucket", nil)

		download, err := project.DownloadObject(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)

		defer ctx.Check(download.Close)
		var downloaded bytes.Buffer
		_, err = io.Copy(&downloaded, download)
		require.NoError(t, err)
		require.Equal(t, len(randData), len(downloaded.Bytes()))
		require.Equal(t, randData, downloaded.Bytes())

		// create part for committed object
		_, err = multipart.PutObjectPart(newCtx, project, "testbucket", "multipart-object", info.StreamID, 0, bytes.NewBuffer(nil))
		require.Error(t, err)
	})
}

func TestListParts(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		newCtx := testuplink.WithMaxSegmentSize(ctx, 50*memory.KiB)

		projectInfo := planet.Uplinks[0].Projects[0]

		uplinkConfig := uplink.Config{}
		access, err := uplinkConfig.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		project, err := uplinkConfig.OpenProject(newCtx, access)
		require.NoError(t, err)

		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		randData := testrand.Bytes(memory.Size(100+testrand.Intn(500)) * memory.KiB)
		firstPartLen := int(float32(len(randData)) * 0.3)
		source1 := bytes.NewBuffer(randData[:firstPartLen])
		source2 := bytes.NewBuffer(randData[firstPartLen:])

		info, err := multipart.NewMultipartUpload(newCtx, project, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		// assert there is only one pending multipart upload
		assertMultipartUploadList(ctx, t, project, "testbucket", nil, "multipart-object")

		{
			_, err = multipart.ListObjectParts(newCtx, project, "", "multipart-object", info.StreamID, 0, 10)
			require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

			_, err = multipart.ListObjectParts(newCtx, project, "testbucket", "", info.StreamID, 0, 10)
			require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

			// empty streamID
			_, err = multipart.ListObjectParts(newCtx, project, "testbucket", "multipart-object", "", 0, 10)
			require.Error(t, err)
		}

		// list multipart upload with no uploaded parts
		parts, err := multipart.ListObjectParts(ctx, project, "testbucket", "multipart-object", info.StreamID, 0, 10)
		require.NoError(t, err)
		require.Equal(t, 0, len(parts.Items))

		_, err = multipart.PutObjectPart(newCtx, project, "testbucket", "multipart-object", info.StreamID, 0, source2)
		require.NoError(t, err)

		_, err = multipart.PutObjectPart(newCtx, project, "testbucket", "multipart-object", info.StreamID, 4, source1)
		require.NoError(t, err)

		// list parts of on going multipart upload
		parts, err = multipart.ListObjectParts(ctx, project, "testbucket", "multipart-object", info.StreamID, 0, 10)
		require.NoError(t, err)
		require.Equal(t, 2, len(parts.Items))

		_, err = multipart.CompleteMultipartUpload(newCtx, project, "testbucket", "multipart-object", info.StreamID, nil)
		require.NoError(t, err)

		// assert there is no pending multipart upload
		assertMultipartUploadList(ctx, t, project, "testbucket", nil)

		// list parts of a completed multipart upload
		parts, err = multipart.ListObjectParts(ctx, project, "testbucket", "multipart-object", info.StreamID, 0, 10)
		require.NoError(t, err)
		require.Equal(t, 2, len(parts.Items))
		// TODO: this should pass once we correctly handle the maxParts parameter
		// require.Equal(t, false, parts.More)

		// list parts with a limit of 1
		parts, err = multipart.ListObjectParts(ctx, project, "testbucket", "multipart-object", info.StreamID, 0, 1)
		require.NoError(t, err)
		require.Equal(t, 1, len(parts.Items))
		// TODO: this should pass once we correctly handle the maxParts parameter
		// require.Equal(t, false, parts.More)

		// list parts with a cursor starting after all parts
		parts, err = multipart.ListObjectParts(ctx, project, "testbucket", "multipart-object", info.StreamID, 5, 10)
		require.NoError(t, err)
		require.Equal(t, 0, len(parts.Items))
		require.Equal(t, false, parts.More)
	})
}

func TestListMultipartUploads_NonExistingBucket(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		list := multipart.ListMultipartUploads(ctx, project, "non-existing-bucket", nil)
		require.NoError(t, list.Err())
		require.Nil(t, list.Item())
		require.False(t, list.Next())
		require.Error(t, list.Err())
		require.True(t, errors.Is(list.Err(), uplink.ErrBucketNotFound))
	})
}

func TestListMultipartUploads_EmptyBucket(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		// assert the there is no pending multipart upload
		assertMultipartUploadList(ctx, t, project, "testbucket", nil)
	})
}

func TestListMultipartUploads_Prefix(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		_, err = multipart.NewMultipartUpload(ctx, project, "testbucket", "a/b/c/multipart-object", nil)
		require.NoError(t, err)

		// assert there is one pending multipart upload with prefix "a/b/"
		assertMultipartUploadList(ctx, t, project, "testbucket", &multipart.ListMultipartUploadsOptions{
			Prefix:    "a/b/",
			Recursive: true,
		}, "a/b/c/multipart-object")

		// assert there is no pending multipart upload with prefix "b/"
		assertMultipartUploadList(ctx, t, project, "testbucket", &multipart.ListMultipartUploadsOptions{
			Prefix:    "b/",
			Recursive: true,
		})

		// assert there is one prefix of pending multipart uploads with prefix "a/b/"
		list := multipart.ListMultipartUploads(ctx, project, "testbucket", &multipart.ListMultipartUploadsOptions{
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

func TestListMultipartUploads_Cursor(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		expectedObjects := map[string]bool{
			"multipart-upload-1": true,
			"multipart-upload-2": true,
		}

		for object := range expectedObjects {
			_, err := multipart.NewMultipartUpload(ctx, project, "testbucket", object, nil)
			require.NoError(t, err)
		}

		// get the first list item and make it a cursor for the next list request
		list := multipart.ListMultipartUploads(ctx, project, "testbucket", nil)
		require.NoError(t, list.Err())
		more := list.Next()
		require.True(t, more)
		require.NoError(t, list.Err())
		delete(expectedObjects, list.Item().Key)
		cursor := list.Item().Key

		// list again with cursor set to the first item from previous list request
		list = multipart.ListMultipartUploads(ctx, project, "testbucket", &multipart.ListMultipartUploadsOptions{Cursor: cursor})
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

func TestListPendingObjectStreams(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := uplink.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		// without prefix
		info, err := multipart.NewMultipartUpload(ctx, project, "testbucket", "multipart-object", nil)
		require.NoError(t, err)

		assertPendingObjectStreamsList(ctx, t, project, "testbucket", "multipart-object", multipart.ListMultipartUploadsOptions{}, info.StreamID)

		// with prefix
		info, err = multipart.NewMultipartUpload(ctx, project, "testbucket", "a/prefixed/multipart-object2", nil)
		require.NoError(t, err)

		assertPendingObjectStreamsList(ctx, t, project, "testbucket", "a/prefixed/multipart-object2", multipart.ListMultipartUploadsOptions{}, info.StreamID)

		// non-existing object
		assertPendingObjectStreamsList(ctx, t, project, "testbucket", "a/prefixed/multipart-object", multipart.ListMultipartUploadsOptions{})
	})
}

func assertPendingObjectStreamsList(ctx context.Context, t *testing.T, project *uplink.Project, bucket, objectKey string, opts multipart.ListMultipartUploadsOptions, streams ...string) {
	list := multipart.ListPendingObjectStreams(ctx, project, bucket, objectKey, &opts)
	require.NoError(t, list.Err())
	require.Nil(t, list.Item())

	itemStreams := make(map[string]struct{})

	for list.Next() {
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		itemStreams[list.Item().StreamID] = struct{}{}
		require.Equal(t, objectKey, list.Item().Key)
	}

	require.Len(t, itemStreams, len(streams))
	for _, stream := range streams {
		if assert.Contains(t, itemStreams, stream) {
			delete(itemStreams, stream)
		}
	}
	require.Empty(t, itemStreams)

	require.False(t, list.Next())
	require.NoError(t, list.Err())
	require.Nil(t, list.Item())
}

func assertMultipartUploadList(ctx context.Context, t *testing.T, project *uplink.Project, bucket string, options *multipart.ListMultipartUploadsOptions, objectKeys ...string) {
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

func createBucket(t *testing.T, ctx *testcontext.Context, project *uplink.Project, bucketName string) *uplink.Bucket {
	bucket, err := project.EnsureBucket(ctx, bucketName)
	require.NoError(t, err)
	require.NotNil(t, bucket)
	require.Equal(t, bucketName, bucket.Name)
	require.WithinDuration(t, time.Now(), bucket.Created, 10*time.Second)
	return bucket
}
