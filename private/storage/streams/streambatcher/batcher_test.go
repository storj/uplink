// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package streambatcher

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
)

var (
	now           = time.Now().UTC().Truncate(time.Second)
	creationDate1 = now.Add(1 * time.Hour)
	creationDate2 = now.Add(2 * time.Hour)
	streamID1     = makeStreamID(creationDate1)
	streamID2     = makeStreamID(creationDate2)
)

func TestBatcher(t *testing.T) {
	type batchRun struct {
		items          []metaclient.BatchItem
		responses      []*pb.BatchResponseItem
		err            error
		expectStreamID storj.StreamID
		expectItems    []metaclient.BatchItem
		expectBatchErr string
		expectInfo     Info
	}

	for _, tc := range []struct {
		desc     string
		streamID storj.StreamID
		runs     []batchRun
	}{
		{
			desc: "batch fails when request missing batch request items",
			runs: []batchRun{
				{
					expectBatchErr: "programmer error: empty batch request",
				},
			},
		},
		{
			desc: "batch fails when response missing batch response items",
			runs: []batchRun{
				{
					items: []metaclient.BatchItem{
						&metaclient.BeginObjectParams{},
					},
					expectBatchErr: "programmer error: empty batch response",
				},
			},
		},
		{
			desc: "batch fails when metainfo client fails",
			runs: []batchRun{
				{
					err: errors.New("oh no"),
					items: []metaclient.BatchItem{
						&metaclient.BeginObjectParams{},
					},
					expectBatchErr: "oh no",
				},
			},
		},
		{
			desc: "batch fails when first batch does not contain a BeginObject call for object upload",
			runs: []batchRun{
				{
					items: []metaclient.BatchItem{
						&metaclient.MakeInlineSegmentParams{PlainSize: 123},
					},
					responses: []*pb.BatchResponseItem{
						{Response: &pb.BatchResponseItem_SegmentMakeInline{SegmentMakeInline: &pb.MakeInlineSegmentResponse{}}},
					},
					expectBatchErr: "programmer error: first batch must start with BeginObject: invalid response type",
				},
			},
		},
		{
			desc: "batch fails when stream ID is not provided by BeginObject response for object upload",
			runs: []batchRun{
				{
					items: []metaclient.BatchItem{
						&metaclient.BeginObjectParams{},
					},
					responses: []*pb.BatchResponseItem{
						{Response: &pb.BatchResponseItem_ObjectBegin{ObjectBegin: &pb.BeginObjectResponse{}}},
					},
					expectBatchErr: "stream ID missing from BeginObject response",
				},
			},
		},
		{
			desc: "upload object with single inline segment",
			runs: []batchRun{
				{
					items: []metaclient.BatchItem{
						&metaclient.BeginObjectParams{},
						&metaclient.MakeInlineSegmentParams{PlainSize: 123},
						&metaclient.CommitObjectParams{},
					},
					expectItems: []metaclient.BatchItem{
						&metaclient.BeginObjectParams{},
						&metaclient.MakeInlineSegmentParams{PlainSize: 123},
						&metaclient.CommitObjectParams{},
					},
					responses: []*pb.BatchResponseItem{
						{Response: &pb.BatchResponseItem_ObjectBegin{ObjectBegin: &pb.BeginObjectResponse{StreamId: streamID1}}},
						{Response: &pb.BatchResponseItem_SegmentMakeInline{SegmentMakeInline: &pb.MakeInlineSegmentResponse{}}},
						{Response: &pb.BatchResponseItem_ObjectCommit{ObjectCommit: &pb.CommitObjectResponse{Object: &pb.Object{
							CreatedAt: creationDate1,
							PlainSize: 123,
						}}}},
					},
					expectStreamID: streamID1,
					expectInfo:     Info{PlainSize: 123, CreationDate: creationDate1},
				},
			},
		},
		{
			desc: "upload object with a remote segment and inline segment",
			runs: []batchRun{
				{
					items: []metaclient.BatchItem{
						&metaclient.BeginObjectParams{},
						&metaclient.BeginSegmentParams{},
					},
					expectItems: []metaclient.BatchItem{
						&metaclient.BeginObjectParams{},
						&metaclient.BeginSegmentParams{},
					},
					responses: []*pb.BatchResponseItem{
						{Response: &pb.BatchResponseItem_ObjectBegin{ObjectBegin: &pb.BeginObjectResponse{StreamId: streamID1}}},
						{Response: &pb.BatchResponseItem_SegmentBegin{SegmentBegin: &pb.BeginSegmentResponse{}}},
					},
					expectStreamID: streamID1,
					expectInfo:     Info{PlainSize: 0},
				},
				{
					items: []metaclient.BatchItem{
						&metaclient.CommitSegmentParams{PlainSize: 123},
					},
					expectItems: []metaclient.BatchItem{
						&metaclient.CommitSegmentParams{PlainSize: 123},
					},
					responses: []*pb.BatchResponseItem{
						{Response: &pb.BatchResponseItem_SegmentCommit{SegmentCommit: &pb.CommitSegmentResponse{}}},
					},
					expectStreamID: streamID1,
					expectInfo:     Info{PlainSize: 123},
				},
				{
					items: []metaclient.BatchItem{
						&metaclient.MakeInlineSegmentParams{PlainSize: 321},
					},
					expectItems: []metaclient.BatchItem{
						&metaclient.MakeInlineSegmentParams{PlainSize: 321, StreamID: streamID1},
					},
					responses: []*pb.BatchResponseItem{
						{Response: &pb.BatchResponseItem_SegmentMakeInline{SegmentMakeInline: &pb.MakeInlineSegmentResponse{}}},
					},
					expectStreamID: streamID1,
					expectInfo:     Info{PlainSize: 444},
				},
			},
		},
		{
			desc:     "upload part with single inline segment",
			streamID: streamID2,
			runs: []batchRun{
				{
					items: []metaclient.BatchItem{
						&metaclient.MakeInlineSegmentParams{PlainSize: 123, StreamID: streamID2},
					},
					expectItems: []metaclient.BatchItem{
						&metaclient.MakeInlineSegmentParams{PlainSize: 123, StreamID: streamID2},
					},
					responses: []*pb.BatchResponseItem{
						{Response: &pb.BatchResponseItem_SegmentMakeInline{SegmentMakeInline: &pb.MakeInlineSegmentResponse{}}},
					},
					expectStreamID: streamID2,
					expectInfo:     Info{PlainSize: 123},
				},
			},
		},
		{
			desc:     "upload part with a remote segment and inline segment",
			streamID: streamID2,
			runs: []batchRun{
				{
					items: []metaclient.BatchItem{
						&metaclient.BeginSegmentParams{StreamID: streamID2},
					},
					expectItems: []metaclient.BatchItem{
						&metaclient.BeginSegmentParams{StreamID: streamID2},
					},
					responses: []*pb.BatchResponseItem{
						{Response: &pb.BatchResponseItem_SegmentBegin{SegmentBegin: &pb.BeginSegmentResponse{}}},
					},
					expectStreamID: streamID2,
					expectInfo:     Info{PlainSize: 0},
				},
				{
					items: []metaclient.BatchItem{
						&metaclient.CommitSegmentParams{PlainSize: 123},
					},
					expectItems: []metaclient.BatchItem{
						&metaclient.CommitSegmentParams{PlainSize: 123},
					},
					responses: []*pb.BatchResponseItem{
						{Response: &pb.BatchResponseItem_SegmentCommit{SegmentCommit: &pb.CommitSegmentResponse{}}},
					},
					expectStreamID: streamID2,
					expectInfo:     Info{PlainSize: 123},
				},
				{
					items: []metaclient.BatchItem{
						&metaclient.MakeInlineSegmentParams{PlainSize: 321, StreamID: streamID2},
					},
					expectItems: []metaclient.BatchItem{
						&metaclient.MakeInlineSegmentParams{PlainSize: 321, StreamID: streamID2},
					},
					responses: []*pb.BatchResponseItem{
						{Response: &pb.BatchResponseItem_SegmentMakeInline{SegmentMakeInline: &pb.MakeInlineSegmentResponse{}}},
					},
					expectStreamID: streamID2,
					expectInfo:     Info{PlainSize: 444},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			miBatcher := new(fakeMetainfoBatcher)
			streamBatcher := New(miBatcher, tc.streamID)

			require.Equal(t, tc.streamID, streamBatcher.StreamID())

			for i, run := range tc.runs {
				t.Run(strconv.Itoa(i), func(t *testing.T) {
					miBatcher.err = run.err
					miBatcher.items = nil
					miBatcher.responses = run.responses

					responses, err := streamBatcher.Batch(context.Background(), run.items...)
					if run.expectBatchErr != "" {
						require.EqualError(t, err, run.expectBatchErr)
						return
					}
					require.NoError(t, err)

					info, err := streamBatcher.Info()
					require.NoError(t, err)

					assert.Equal(t, run.expectStreamID, streamBatcher.StreamID(), "unexpected stream ID tracked by the stream batcher")
					assert.Len(t, responses, len(run.items), "stream batcher returned unexpected response count")
					assert.Equal(t, run.expectItems, miBatcher.items, "metainfo batcher received unexpected items")
					assert.Equal(t, run.expectInfo, info)
				})
			}
		})
	}
}

func TestStreamBatcherInfoReturnsErrorIfNoStreamIDHasBeenSet(t *testing.T) {
	streamBatcher := New(new(fakeMetainfoBatcher), nil)
	_, err := streamBatcher.Info()
	require.EqualError(t, err, "stream ID is unexpectedly nil")
}

type fakeMetainfoBatcher struct {
	items     []metaclient.BatchItem
	responses []*pb.BatchResponseItem
	err       error
}

func (mi *fakeMetainfoBatcher) Batch(ctx context.Context, items ...metaclient.BatchItem) ([]metaclient.BatchResponse, error) {
	if len(items) == 0 {
		return nil, errs.New("test/programmer error: batch should never be issued with no items")
	}
	mi.items = items
	if mi.err != nil {
		return nil, mi.err
	}
	var responses []metaclient.BatchResponse
	for i := 0; i < len(mi.responses); i++ {
		item := items[i]
		response := mi.responses[i]
		responses = append(responses, metaclient.MakeBatchResponse(item.BatchItem(), response))
	}
	return responses, nil
}

func makeStreamID(creationDate time.Time) storj.StreamID {
	streamID, _ := pb.Marshal(&pb.SatStreamID{CreationDate: creationDate})
	return storj.StreamID(streamID)
}
