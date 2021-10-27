// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/errs2"
	"storj.io/common/fpath"
	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/storagenode"
	"storj.io/uplink"
	"storj.io/uplink/private/testuplink"
)

func TestObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")
		defer func() {
			_, err := project.DeleteBucket(ctx, "testbucket")
			require.NoError(t, err)
		}()

		testCases := []struct {
			Name       string
			ObjectSize memory.Size
		}{
			{"empty", 0},
			{"inline", memory.KiB},
			{"remote", 10 * memory.KiB},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.Name, func(t *testing.T) {
				upload, err := project.UploadObject(ctx, "testbucket", tc.Name, nil)
				require.NoError(t, err)
				assertObjectEmptyCreated(t, upload.Info(), tc.Name)

				randData := testrand.Bytes(tc.ObjectSize)
				source := bytes.NewBuffer(randData)
				_, err = io.Copy(upload, source)
				require.NoError(t, err)
				assertObjectEmptyCreated(t, upload.Info(), tc.Name)

				err = upload.Commit()
				require.NoError(t, err)
				assertObject(t, upload.Info(), tc.Name)

				obj, err := project.StatObject(ctx, "testbucket", tc.Name)
				require.NoError(t, err)
				assertObject(t, obj, tc.Name)

				err = upload.Commit()
				require.True(t, errors.Is(err, uplink.ErrUploadDone))

				uploadInfo := upload.Info()
				assertObject(t, uploadInfo, tc.Name)
				require.True(t, uploadInfo.System.Expires.IsZero())
				require.False(t, uploadInfo.IsPrefix)
				require.EqualValues(t, len(randData), uploadInfo.System.ContentLength)

				objectSize := tc.ObjectSize.Int64()

				var tests = []*uplink.DownloadOptions{
					nil,
					{Offset: 0, Length: -1},
					{Offset: 1, Length: 7408},
					{Offset: 1, Length: 7423},
					{Offset: objectSize / 2, Length: -1},
					{Offset: objectSize, Length: -1},
					{Offset: 0, Length: 0},
					{Offset: objectSize / 2, Length: 0},
					{Offset: objectSize, Length: 0},
					{Offset: 0, Length: 15},
					{Offset: objectSize / 2, Length: 15},
					{Offset: -objectSize / 3, Length: -1},
					// {Offset: -objectSize / 3, Length: 15}, // TODO: unsupported at the moment
				}
				for _, test := range tests {
					if test != nil && test.Length > objectSize {
						continue
					}

					test := test
					t.Run(fmt.Sprintf("%#v", test), func(t *testing.T) {
						download, err := project.DownloadObject(ctx, "testbucket", tc.Name, test)
						require.NoError(t, err)
						assertObject(t, download.Info(), tc.Name)

						downloaded, err := ioutil.ReadAll(download)
						require.NoError(t, err)
						require.NoError(t, download.Close())

						if test == nil {
							require.Equal(t, randData, downloaded)
							return
						}

						start := test.Offset
						if start < 0 {
							start = objectSize + start
						}

						length := test.Length
						if length < 0 {
							length = objectSize
						}
						if start+length > objectSize {
							length = objectSize - start
						}

						require.Equal(t, randData[start:start+length], downloaded)
					})
				}

				// delete
				deleted, err := project.DeleteObject(ctx, "testbucket", tc.Name)
				require.NoError(t, err)
				require.NotNil(t, deleted)
				require.Equal(t, tc.Name, deleted.Key)

				// stat object
				obj, err = project.StatObject(ctx, "testbucket", tc.Name)
				require.True(t, errors.Is(err, uplink.ErrObjectNotFound))
				require.Nil(t, obj)

				// delete missing object
				deleted, err = project.DeleteObject(ctx, "testbucket", tc.Name)
				assert.Nil(t, err)
				require.Nil(t, deleted)
			})
		}

	})
}

func TestInmemoryUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		_, err := project.EnsureBucket(ctx, "testbucket")
		require.NoError(t, err)

		inmemCtx := fpath.WithTempData(ctx, "", true)
		sizedCtx := testuplink.WithMaxSegmentSize(inmemCtx, memory.MiB)
		expected := testrand.Bytes(2 * memory.MiB)

		upload, err := project.UploadObject(sizedCtx, "testbucket", "obj", nil)
		require.NoError(t, err)
		_, err = upload.Write(expected)
		require.NoError(t, err)
		require.NoError(t, upload.Commit())

		down, err := project.DownloadObject(sizedCtx, "testbucket", "obj", nil)
		require.NoError(t, err)
		downloaded, err := ioutil.ReadAll(down)
		require.NoError(t, err)
		require.NoError(t, down.Close())

		require.Equal(t, expected, downloaded)
	})
}

func TestUploadObjectWithOverMaxParts(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.MaxNumberOfParts = 2
				config.Metainfo.MinPartSize = 5 * memory.MiB
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)

		defer ctx.Check(project.Close)

		bucket := "testbucket"
		objectKey := "multipart-object"

		createBucket(t, ctx, project, bucket)

		info, err := project.BeginUpload(ctx, bucket, objectKey, nil)
		require.NoError(t, err)
		require.NotEmpty(t, info.UploadID)

		t.Run("parts in default order", func(t *testing.T) {
			expectedData := testrand.Bytes(30 * memory.KiB)
			for i := 0; i < 3; i++ {
				upload, err := project.UploadPart(ctx, bucket, objectKey, info.UploadID, uint32(i))
				require.NoError(t, err)
				_, err = upload.Write(expectedData)
				require.NoError(t, err)
				err = upload.Commit()
				require.NoError(t, err)
			}

			_, err = project.CommitUpload(ctx, bucket, objectKey, info.UploadID, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "exceeded maximum number of parts: 2")
		})

		t.Run("parts in random order", func(t *testing.T) {
			expectedData := testrand.Bytes(30 * memory.KiB)
			for i := 0; i < 3; i++ {
				upload, err := project.UploadPart(ctx, bucket, objectKey, info.UploadID, uint32(i+3))
				require.NoError(t, err)
				_, err = upload.Write(expectedData)
				require.NoError(t, err)
				err = upload.Commit()
				require.NoError(t, err)
			}

			_, err = project.CommitUpload(ctx, bucket, objectKey, info.UploadID, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "exceeded maximum number of parts: 2")
		})
	})
}

func TestUploadObjectWithSmallParts(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.MaxNumberOfParts = 10
				config.Metainfo.MinPartSize = 5 * memory.MiB
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)

		defer ctx.Check(project.Close)

		bucket := "testbucket"
		objectKey := "multipart-object"

		createBucket(t, ctx, project, bucket)

		info, err := project.BeginUpload(ctx, bucket, objectKey, nil)
		require.NoError(t, err)
		require.NotEmpty(t, info.UploadID)

		t.Run("parts less then minimum threshold", func(t *testing.T) {
			expectedData := testrand.Bytes(30 * memory.KiB)
			for i := 1; i < 3; i++ {
				upload, err := project.UploadPart(ctx, bucket, objectKey, info.UploadID, uint32(i))
				require.NoError(t, err)
				_, err = upload.Write(expectedData)
				require.NoError(t, err)
				err = upload.Commit()
				require.NoError(t, err)
			}

			_, err = project.CommitUpload(ctx, bucket, objectKey, info.UploadID, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "size of part number 1 is below minimum threshold, got: 30.0 KiB, min: 5.0 MiB")
		})

		t.Run("only last part less then minimum threshold", func(t *testing.T) {
			info, err = project.BeginUpload(ctx, bucket, objectKey, nil)
			require.NoError(t, err)
			require.NotEmpty(t, info.UploadID)

			parts := []memory.Size{5 * memory.MiB, 30 * memory.KiB}
			for i, size := range parts {
				upload, err := project.UploadPart(ctx, bucket, objectKey, info.UploadID, uint32(i))
				require.NoError(t, err)
				_, err = upload.Write(testrand.Bytes(size))
				require.NoError(t, err)
				err = upload.Commit()
				require.NoError(t, err)
			}

			_, err = project.CommitUpload(ctx, bucket, objectKey, info.UploadID, nil)
			require.NoError(t, err)
		})
	})
}

func TestAbortUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		upload, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		randData := testrand.Bytes(10 * memory.KiB)
		_, err = upload.Write(randData)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		// we should have one pending object
		objects, err := planet.Satellites[0].Metabase.DB.TestingAllObjects(ctx)
		require.NoError(t, err)
		require.Len(t, objects, 1)

		err = upload.Abort()
		require.NoError(t, err)

		// we should have NO objects after abort
		objects, err = planet.Satellites[0].Metabase.DB.TestingAllObjects(ctx)
		require.NoError(t, err)
		require.Len(t, objects, 0)

		err = upload.Commit()
		require.Error(t, err)

		_, err = project.StatObject(ctx, "testbucket", "test.dat")
		require.True(t, errors.Is(err, uplink.ErrObjectNotFound))

		err = upload.Abort()
		require.True(t, errors.Is(err, uplink.ErrUploadDone))

		uploads := project.ListUploads(ctx, "testbucket", nil)
		for uploads.Next() {
			item := uploads.Item()
			require.Fail(t, "expected no pending uploads", item)
		}
		require.NoError(t, uploads.Err())
	})
}

func TestContextCancelUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		uploadctx, uploadcancel := context.WithCancel(ctx)

		upload, err := project.UploadObject(uploadctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		randData := testrand.Bytes(10 * memory.KiB)
		_, err = upload.Write(randData)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		uploadcancel()
		_, err = upload.Write(randData)
		require.Error(t, err)
		require.True(t, errs2.IsCanceled(err))

		err = upload.Abort()
		require.NoError(t, err)

		err = upload.Commit()
		require.Error(t, err)

		_, err = project.StatObject(ctx, "testbucket", "test.dat")
		require.True(t, errors.Is(err, uplink.ErrObjectNotFound))

		err = upload.Abort()
		require.True(t, errors.Is(err, uplink.ErrUploadDone))

		uploads := project.ListUploads(ctx, "testbucket", nil)
		for uploads.Next() {
			item := uploads.Item()
			require.Fail(t, "expected no pending uploads", item)
		}
		require.NoError(t, uploads.Err())
	})
}

func TestConcurrentUploadAndCommit(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: testplanet.MaxSegmentSize(20 * memory.KiB),
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		// start upload
		upload1, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload1.Info(), "test.dat")

		randData := testrand.Bytes(30 * memory.KiB)
		_, err = upload1.Write(randData)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload1.Info(), "test.dat")

		// start second upload to the same path,
		// this will currently delete the first upload.
		upload2, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload2.Info(), "test.dat")

		_, err = upload2.Write(randData)
		require.NoError(t, err)

		err = upload2.Commit()
		require.NoError(t, err)

		// Try to commit the first object, but this will fail because it was deleted.
		err = upload1.Commit()
		require.Error(t, err)

		_, err = project.StatObject(ctx, "testbucket", "test.dat")
		require.NoError(t, err)

		uploads := project.ListUploads(ctx, "testbucket", nil)
		for uploads.Next() {
			item := uploads.Item()
			require.Fail(t, "expected no pending uploads", item)
		}
		require.NoError(t, uploads.Err())
	})
}

func TestConcurrentUploadAndCancel(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: testplanet.MaxSegmentSize(20 * memory.KiB),
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		// start upload
		upload1, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload1.Info(), "test.dat")

		randData := testrand.Bytes(30 * memory.KiB)
		_, err = upload1.Write(randData)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload1.Info(), "test.dat")

		// start second upload to the same path,
		// this will currently delete the first upload.
		upload2, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload2.Info(), "test.dat")

		_, err = upload2.Write(randData)
		require.NoError(t, err)

		err = upload2.Commit()
		require.NoError(t, err)

		// Try to abort the first object, but this will fail because it was deleted.
		// It shouldn't also delete the existing object.
		err = upload1.Abort()
		require.NoError(t, err)

		_, err = project.StatObject(ctx, "testbucket", "test.dat")
		require.NoError(t, err)

		uploads := project.ListUploads(ctx, "testbucket", nil)
		for uploads.Next() {
			item := uploads.Item()
			require.Fail(t, "expected no pending uploads", item)
		}
		require.NoError(t, uploads.Err())
	})
}

func TestUploadError(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		upload, err := project.UploadObject(ctx, "testbucket", "test.dat", nil)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		randData := testrand.Bytes(1 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)
		assertObjectEmptyCreated(t, upload.Info(), "test.dat")

		require.NoError(t, planet.StopPeer(planet.Satellites[0]))

		err = upload.Commit()
		require.Error(t, err)
	})
}

func TestVeryLongDownload(t *testing.T) {
	const (
		segmentSize           = 100 * memory.KiB
		orderLimitGracePeriod = 5 * time.Second
	)

	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: testplanet.MaxSegmentSize(segmentSize),
			StorageNode: func(index int, config *storagenode.Config) {
				config.Storage2.OrderLimitGracePeriod = orderLimitGracePeriod
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		createBucket(t, ctx, project, "testbucket")

		data := testrand.Bytes(segmentSize * 3 / 2) // 2 segments
		err := planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "testbucket", "test.dat", data)
		require.NoError(t, err)

		download, cleanup, err := planet.Uplinks[0].DownloadStream(ctx, planet.Satellites[0], "testbucket", "test.dat")
		require.NoError(t, err)
		defer ctx.Check(download.Close)
		defer ctx.Check(cleanup)

		// read the first half of the first segment
		buf := make([]byte, segmentSize.Int()/2)
		n, err := io.ReadFull(download, buf)
		require.NoError(t, err)
		assert.Equal(t, segmentSize.Int()/2, n)
		assert.Equal(t, data[:segmentSize.Int()/2], buf)

		// sleep to expire already created orders
		time.Sleep(orderLimitGracePeriod + 1*time.Second)

		// read the rest: the remainder of the first segment, and the second (last) segment
		buf = make([]byte, segmentSize.Int())
		n, err = io.ReadFull(download, buf)
		require.NoError(t, err)
		assert.Equal(t, segmentSize.Int(), n)
		assert.Equal(t, data[segmentSize.Int()/2:segmentSize.Int()*3/2], buf)
	})
}

func assertObject(t *testing.T, obj *uplink.Object, expectedKey string) {
	assert.Equal(t, expectedKey, obj.Key)
	assert.WithinDuration(t, time.Now(), obj.System.Created, 10*time.Second)
}

func assertObjectEmptyCreated(t *testing.T, obj *uplink.Object, expectedKey string) {
	assert.Equal(t, expectedKey, obj.Key)
	assert.Empty(t, obj.System.Created)
}

func uploadObject(t *testing.T, ctx *testcontext.Context, project *uplink.Project, bucket, key string, objectSize memory.Size) *uplink.Object {
	return uploadObjectWithMetadata(t, ctx, project, bucket, key, objectSize, uplink.CustomMetadata{})
}

func uploadObjectWithMetadata(t *testing.T, ctx *testcontext.Context, project *uplink.Project, bucket, key string, objectSize memory.Size, metadata uplink.CustomMetadata) *uplink.Object {
	upload, err := project.UploadObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	assertObjectEmptyCreated(t, upload.Info(), key)

	randData := testrand.Bytes(objectSize)
	source := bytes.NewBuffer(randData)
	_, err = io.Copy(upload, source)
	require.NoError(t, err)
	assertObjectEmptyCreated(t, upload.Info(), key)

	if len(metadata) > 0 {
		err = upload.SetCustomMetadata(ctx, metadata)
		require.NoError(t, err)
	}

	err = upload.Commit()
	require.NoError(t, err)
	assertObject(t, upload.Info(), key)

	return upload.Info()
}
