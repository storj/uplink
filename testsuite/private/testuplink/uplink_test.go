// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testuplink_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/pb"
	"storj.io/common/rpc/rpcpool"
	"storj.io/common/rpc/rpctest"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/drpc"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite/metabase"
	"storj.io/uplink"
	"storj.io/uplink/private/testuplink"
)

func TestWithMaxSegmentSize(t *testing.T) {
	ctx := context.Background()
	_, ok := testuplink.GetMaxSegmentSize(ctx)
	require.False(t, ok)

	newCtx := testuplink.WithMaxSegmentSize(ctx, memory.KiB)
	segmentSize, ok := testuplink.GetMaxSegmentSize(newCtx)
	require.True(t, ok)
	require.EqualValues(t, memory.KiB, segmentSize)

}

func TestWithMaxSegmentSize_Upload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		newCtx := testuplink.WithMaxSegmentSize(ctx, 10*memory.KiB)

		expectedData := testrand.Bytes(19 * memory.KiB)
		err := planet.Uplinks[0].Upload(newCtx, planet.Satellites[0], "super-bucket", "super-object", expectedData)
		require.NoError(t, err)

		data, err := planet.Uplinks[0].Download(newCtx, planet.Satellites[0], "super-bucket", "super-object")
		require.NoError(t, err)
		require.Equal(t, expectedData, data)

		// verify we have two segments instead of one
		objects, err := planet.Satellites[0].Metabase.DB.TestingAllCommittedObjects(ctx, planet.Uplinks[0].Projects[0].ID, "super-bucket")
		require.NoError(t, err)
		require.Len(t, objects, 1)

		segments, err := planet.Satellites[0].Metabase.DB.TestingAllObjectSegments(ctx, metabase.ObjectLocation{
			ProjectID:  planet.Uplinks[0].Projects[0].ID,
			BucketName: "super-bucket",
			ObjectKey:  objects[0].ObjectKey,
		})
		require.NoError(t, err)
		require.Equal(t, 2, len(segments))
	})
}

func TestSatelliteRoundTrip(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		var requestctx context.Context = ctx

		requestctx = testuplink.WithMaxSegmentSize(requestctx, 10*memory.KiB)

		projectInfo := planet.Uplinks[0].Projects[0]
		access, err := uplink.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		callRecorder := NewCallRecorder()
		requestctx = rpcpool.WithDialerWrapper(requestctx,
			func(ctx context.Context, dialer rpcpool.Dialer) rpcpool.Dialer {
				return func(context.Context) (rpcpool.RawConn, *tls.ConnectionState, error) {
					conn, state, err := dialer(requestctx)
					if err != nil {
						return conn, state, err
					}
					return callRecorder.Attach(conn), state, err
				}
			})

		t.Run("create bucket", func(t *testing.T) {
			callRecorder.Reset()

			project, err := uplink.OpenProject(requestctx, access)
			require.NoError(t, err)

			_, err = project.CreateBucket(requestctx, "bucket")
			require.NoError(t, err)

			require.NoError(t, project.Close())

			require.Equal(t, []string{
				`/metainfo.Metainfo/CreateBucket`,
			}, callRecorder.History())
		})

		inlineData := make([]byte, 1*memory.KiB.Int())

		t.Run("upload/inline", func(t *testing.T) {
			callRecorder.Reset()

			project, err := uplink.OpenProject(requestctx, access)
			require.NoError(t, err)

			upload, err := project.UploadObject(requestctx, "bucket", "inline", nil)
			require.NoError(t, err)

			_, err = upload.Write(inlineData)
			require.NoError(t, err)

			require.NoError(t, upload.Commit())
			require.NoError(t, project.Close())

			require.Equal(t, []string{
				`/metainfo.Metainfo/CompressedBatch`,
				`->*pb.BatchRequestItem_ObjectBegin`,
				`->*pb.BatchRequestItem_SegmentMakeInline`,
				`->*pb.BatchRequestItem_ObjectCommit`,
			}, discardFromHistory(callRecorder.History(), "/piecestore"))
		})

		expectedData := make([]byte, 8*memory.KiB.Int())

		t.Run("upload/8kib", func(t *testing.T) {
			callRecorder.Reset()

			project, err := uplink.OpenProject(requestctx, access)
			require.NoError(t, err)

			upload, err := project.UploadObject(requestctx, "bucket", "object", nil)
			require.NoError(t, err)

			_, err = upload.Write(expectedData)
			require.NoError(t, err)

			require.NoError(t, upload.Commit())
			require.NoError(t, project.Close())

			require.Equal(t, []string{
				`/metainfo.Metainfo/CompressedBatch`,
				`->*pb.BatchRequestItem_ObjectBegin`,
				`->*pb.BatchRequestItem_SegmentBegin`,
				`/metainfo.Metainfo/CompressedBatch`,
				`->*pb.BatchRequestItem_SegmentCommit`,
				`->*pb.BatchRequestItem_ObjectCommit`,
			}, discardFromHistory(callRecorder.History(), "/piecestore"))
		})

		expectedLargeData := make([]byte, 16*memory.KiB.Int())
		t.Run("upload/16kib-2segments", func(t *testing.T) {
			callRecorder.Reset()

			project, err := uplink.OpenProject(requestctx, access)
			require.NoError(t, err)

			upload, err := project.UploadObject(requestctx, "bucket", "object2", nil)
			require.NoError(t, err)

			_, err = upload.Write(expectedLargeData)
			require.NoError(t, err)

			require.NoError(t, upload.Commit())
			require.NoError(t, project.Close())

			require.Equal(t, []string{
				"/metainfo.Metainfo/CompressedBatch",
				"->*pb.BatchRequestItem_ObjectBegin",
				"->*pb.BatchRequestItem_SegmentBegin",
				"/metainfo.Metainfo/CompressedBatch",
				"->*pb.BatchRequestItem_SegmentBegin",
				"/metainfo.Metainfo/CompressedBatch",
				"->*pb.BatchRequestItem_SegmentCommit",
				"->*pb.BatchRequestItem_SegmentCommit",
				"->*pb.BatchRequestItem_ObjectCommit",
			}, discardFromHistory(callRecorder.History(), "/piecestore"))
		})

		t.Run("upload/multipart/1-part", func(t *testing.T) {
			callRecorder.Reset()

			project, err := uplink.OpenProject(requestctx, access)
			require.NoError(t, err)

			info, err := project.BeginUpload(requestctx, "bucket", "multi", nil)
			require.NoError(t, err)

			upload, err := project.UploadPart(requestctx, "bucket", "multi", info.UploadID, 0)
			require.NoError(t, err)
			_, err = upload.Write(expectedData)
			require.NoError(t, err)
			require.NoError(t, upload.Commit())

			_, err = project.CommitUpload(requestctx, "bucket", "multi", info.UploadID, nil)
			require.NoError(t, err)

			require.Equal(t, []string{
				"/metainfo.Metainfo/BeginObject",
				`/metainfo.Metainfo/CompressedBatch`,
				`->*pb.BatchRequestItem_SegmentBegin`,
				`/metainfo.Metainfo/CompressedBatch`,
				`->*pb.BatchRequestItem_SegmentCommit`,
				`/metainfo.Metainfo/CommitObject`,
			}, discardFromHistory(callRecorder.History(), "/piecestore"))
		})

		t.Run("upload/multipart/2-parts", func(t *testing.T) {
			callRecorder.Reset()

			project, err := uplink.OpenProject(requestctx, access)
			require.NoError(t, err)

			info, err := project.BeginUpload(requestctx, "bucket", "multi2", nil)
			require.NoError(t, err)

			upload, err := project.UploadPart(requestctx, "bucket", "multi2", info.UploadID, 0)
			require.NoError(t, err)
			_, err = upload.Write(expectedData)
			require.NoError(t, err)
			require.NoError(t, upload.Commit())

			upload, err = project.UploadPart(requestctx, "bucket", "multi2", info.UploadID, 1)
			require.NoError(t, err)
			_, err = upload.Write(expectedData)
			require.NoError(t, err)
			require.NoError(t, upload.Commit())

			_, err = project.CommitUpload(requestctx, "bucket", "multi2", info.UploadID, nil)
			require.NoError(t, err)

			require.NoError(t, project.Close())

			require.Equal(t, []string{
				"/metainfo.Metainfo/BeginObject",
				"/metainfo.Metainfo/CompressedBatch",
				"->*pb.BatchRequestItem_SegmentBegin",
				"/metainfo.Metainfo/CompressedBatch",
				"->*pb.BatchRequestItem_SegmentCommit",
				"/metainfo.Metainfo/CompressedBatch",
				"->*pb.BatchRequestItem_SegmentBegin",
				"/metainfo.Metainfo/CompressedBatch",
				"->*pb.BatchRequestItem_SegmentCommit",
				"/metainfo.Metainfo/CommitObject",
			}, discardFromHistory(callRecorder.History(), "/piecestore"))
		})

		t.Run("download", func(t *testing.T) {
			callRecorder.Reset()

			project, err := uplink.OpenProject(requestctx, access)
			require.NoError(t, err)

			download, err := project.DownloadObject(requestctx, "bucket", "object", nil)
			require.NoError(t, err)

			data, err := io.ReadAll(download)
			require.NoError(t, err)
			require.Equal(t, expectedData, data)

			require.NoError(t, download.Close())
			require.NoError(t, project.Close())

			require.Equal(t, []string{
				`/metainfo.Metainfo/CompressedBatch`,
				`->*pb.BatchRequestItem_ObjectDownload`,
			}, discardFromHistory(callRecorder.History(), "/piecestore"))
		})
	})
}

func discardFromHistory(history []string, ignore ...string) (r []string) {
next:
	for _, v := range history {
		for _, ign := range ignore {
			if strings.HasPrefix(v, ign) {
				continue next
			}
		}
		r = append(r, v)
	}
	return r
}

// CallRecorder wraps drpc.Conn and record the rpc names for each calls.
// It uses an internal Mutex, therefore it's not recommended for production or
// performance critical operations.
type CallRecorder struct {
	mu    sync.Mutex
	calls []string
}

// NewCallRecorder returns with a properly initialized RPCounter.
func NewCallRecorder() CallRecorder {
	return CallRecorder{}
}

// Reset deletes all the existing counters and set everything to 0.
func (r *CallRecorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = r.calls[:0]
}

// RecordCall records the fact of one rpc call.
func (r *CallRecorder) RecordCall(rpc string, message drpc.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, rpc)

	if compressed, ok := message.(*pb.CompressedBatchRequest); ok {
		var data []byte
		switch compressed.Selected {
		case pb.CompressedBatchRequest_ZSTD:
			if decoder, err := zstd.NewReader(nil); err == nil {
				if uncompressed, err := decoder.DecodeAll(compressed.Data, nil); err == nil {
					data = uncompressed
				}
			}
		case pb.CompressedBatchRequest_NONE:
			data = compressed.Data
		}
		if len(data) > 0 {
			var internal pb.BatchRequest
			if err := pb.Unmarshal(data, &internal); err == nil {
				message = &internal
			}
		}
	}
	if batch, ok := message.(*pb.BatchRequest); ok {
		for _, req := range batch.Requests {
			r.calls = append(r.calls, "->"+fmt.Sprintf("%T", req.Request))
		}
	}
}

// History returns the list of rpc names which called on this connection.
func (r *CallRecorder) History() []string {
	return append([]string{}, r.calls...)
}

// Attach wraps a drpc.Conn connection and returns with one where the counters are hooked in.
func (r *CallRecorder) Attach(conn rpcpool.RawConn) rpcpool.RawConn {
	interceptor := rpctest.NewMessageInterceptor(conn)
	interceptor.RequestHook = func(rpc string, message drpc.Message, err error) {
		r.RecordCall(rpc, message)
	}
	return &interceptor
}
