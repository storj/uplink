// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
	"storj.io/uplink/private/testuplink"
)

func TestProject_NewMultipartUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		_, err := project.NewMultipartUpload(ctx, "not-existing-testbucket", "multipart-object", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		createBucket(t, ctx, project, "testbucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		info, err := project.NewMultipartUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		// we allow to start several multipart uploads for the same key
		// TODO check why its not possible anymore
		// _, err = project.NewMultipartUpload(ctx, "testbucket", "multipart-object", nil)
		// require.NoError(t, err)
		// require.NotNil(t, info.StreamID)

		info, err = project.NewMultipartUpload(ctx, "testbucket", "multipart-object-1", &uplink.MultipartUploadOptions{
			Expires: time.Now().Add(time.Hour),
		})
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)
	})
}

func TestProject_CompleteMultipartUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		{
			_, err := project.CompleteMultipartUpload(ctx, "", "", "", nil)
			require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

			_, err = project.CompleteMultipartUpload(ctx, "testbucket", "", "", nil)
			require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

			_, err = project.CompleteMultipartUpload(ctx, "testbucket", "multipart-object", "", nil)
			require.Error(t, err) // TODO should we create an error like ErrInvalidArgument
		}

		{
			info, err := project.NewMultipartUpload(ctx, "testbucket", "multipart-object", nil)
			require.NoError(t, err)
			require.NotNil(t, info.StreamID)

			_, err = project.CompleteMultipartUpload(ctx, "testbucket", "multipart-object", info.StreamID, nil)
			require.NoError(t, err)

			_, err = project.StatObject(ctx, "testbucket", "multipart-object")
			require.NoError(t, err)

			// object is already committed
			_, err = project.CompleteMultipartUpload(ctx, "testbucket", "multipart-object", info.StreamID, nil)
			require.Error(t, err)
		}

		{
			info, err := project.NewMultipartUpload(ctx, "testbucket", "multipart-object-metadata", nil)
			require.NoError(t, err)
			require.NotNil(t, info.StreamID)

			expectedMetadata := uplink.CustomMetadata{
				"TestField1": "TestFieldValue1",
				"TestField2": "TestFieldValue2",
			}
			_, err = project.CompleteMultipartUpload(ctx, "testbucket", "multipart-object-metadata", info.StreamID, &uplink.MultipartObjectOptions{
				CustomMetadata: expectedMetadata,
			})
			require.NoError(t, err)

			object, err := project.StatObject(ctx, "testbucket", "multipart-object-metadata")
			require.NoError(t, err)
			require.Equal(t, expectedMetadata, object.Custom)
		}
		// TODO add more tests
	})
}

func TestProject_AbortMultipartUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		info, err := project.NewMultipartUpload(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		err = project.AbortMultipartUpload(ctx, "", "multipart-object", info.StreamID)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

		err = project.AbortMultipartUpload(ctx, "testbucket", "", info.StreamID)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

		err = project.AbortMultipartUpload(ctx, "testbucket", "multipart-object", "")
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrStreamIDInvalid))

		// TODO: testcases we cannot do now:
		// - right streamID/wrong bucket or project ID
		// - existing bucket/existing key/existing streamID, but not the good one

		err = project.AbortMultipartUpload(ctx, "testbucket", "multipart-object", info.StreamID)
		require.NoError(t, err)
		// TODO: once we get listing of on-going multipart uploads, it's not there anymore
	})
}

func TestProject_PutObjectPart(t *testing.T) {
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
		defer func() {
			_, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		// bucket not exists
		_, err = project.NewMultipartUpload(newCtx, "non-existing-testbucket", "multipart-object", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		randData := testrand.Bytes(memory.Size(100+testrand.Intn(500)) * memory.KiB)
		firstPartLen := int(float32(len(randData)) * 0.3)
		source1 := bytes.NewBuffer(randData[:firstPartLen])
		source2 := bytes.NewBuffer(randData[firstPartLen:])

		info, err := project.NewMultipartUpload(newCtx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		{
			_, err = project.PutObjectPart(newCtx, "", "multipart-object", info.StreamID, 1, source2)
			require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

			_, err = project.PutObjectPart(newCtx, "testbucket", "", info.StreamID, 1, source2)
			require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

			// empty streamID
			_, err = project.PutObjectPart(newCtx, "testbucket", "multipart-object", "", 1, source2)
			require.Error(t, err)

			// negative partID
			_, err = project.PutObjectPart(newCtx, "testbucket", "multipart-object", info.StreamID, 0, source2)
			require.Error(t, err)

			// empty input data reader
			_, err = project.PutObjectPart(newCtx, "testbucket", "multipart-object", info.StreamID, 1, bytes.NewBuffer([]byte{}))
			require.Error(t, err)
		}

		_, err = project.PutObjectPart(newCtx, "testbucket", "multipart-object", info.StreamID, 2, source2)
		require.NoError(t, err)

		_, err = project.PutObjectPart(newCtx, "testbucket", "multipart-object", info.StreamID, 1, source1)
		require.NoError(t, err)

		_, err = project.CompleteMultipartUpload(newCtx, "testbucket", "multipart-object", info.StreamID, nil)
		require.NoError(t, err)

		download, err := project.DownloadObject(ctx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)

		defer ctx.Check(download.Close)
		var downloaded bytes.Buffer
		_, err = io.Copy(&downloaded, download)
		require.NoError(t, err)
		require.Equal(t, len(randData), len(downloaded.Bytes()))
		require.Equal(t, randData, downloaded.Bytes())

		// create part for committed object
		_, err = project.PutObjectPart(newCtx, "testbucket", "multipart-object", info.StreamID, 1, source2)
		require.Error(t, err)
	})
}

func TestProject_ListParts(t *testing.T) {
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
		defer func() {
			_, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		randData := testrand.Bytes(memory.Size(100+testrand.Intn(500)) * memory.KiB)
		firstPartLen := int(float32(len(randData)) * 0.3)
		source1 := bytes.NewBuffer(randData[:firstPartLen])
		source2 := bytes.NewBuffer(randData[firstPartLen:])

		info, err := project.NewMultipartUpload(newCtx, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.StreamID)

		{
			_, err = project.ListParts(newCtx, "", "multipart-object", info.StreamID, 1, 10)
			require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

			_, err = project.ListParts(newCtx, "testbucket", "", info.StreamID, 1, 10)
			require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

			// empty streamID
			_, err = project.ListParts(newCtx, "testbucket", "multipart-object", "", 1, 10)
			require.Error(t, err)
		}

		// list multipart upload with no uploaded parts
		parts, err := project.ListParts(ctx, "testbucket", "multipart-object", info.StreamID, 1, 10)
		require.NoError(t, err)
		require.Equal(t, 0, len(parts.Items))

		_, err = project.PutObjectPart(newCtx, "testbucket", "multipart-object", info.StreamID, 1, source2)
		require.NoError(t, err)

		_, err = project.PutObjectPart(newCtx, "testbucket", "multipart-object", info.StreamID, 5, source1)
		require.NoError(t, err)

		// list parts of on going multipart upload
		parts, err = project.ListParts(ctx, "testbucket", "multipart-object", info.StreamID, 1, 10)
		require.NoError(t, err)
		require.Equal(t, 2, len(parts.Items))

		_, err = project.CompleteMultipartUpload(newCtx, "testbucket", "multipart-object", info.StreamID, nil)
		require.NoError(t, err)

		// list parts of a completed multipart upload
		parts, err = project.ListParts(ctx, "testbucket", "multipart-object", info.StreamID, 1, 10)
		require.NoError(t, err)
		require.Equal(t, 2, len(parts.Items))
		// TODO: this should pass once we correctly handle the maxParts parameter
		// require.Equal(t, false, parts.More)

		// list parts with a limit of 1
		parts, err = project.ListParts(ctx, "testbucket", "multipart-object", info.StreamID, 1, 1)
		require.NoError(t, err)
		require.Equal(t, 1, len(parts.Items))
		// TODO: this should pass once we correctly handle the maxParts parameter
		// require.Equal(t, false, parts.More)

		// list parts with a cursor starting after all parts
		parts, err = project.ListParts(ctx, "testbucket", "multipart-object", info.StreamID, 6, 10)
		require.NoError(t, err)
		require.Equal(t, 0, len(parts.Items))
		require.Equal(t, false, parts.More)
	})
}
