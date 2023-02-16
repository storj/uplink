// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package batchaggregator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/pb"
	"storj.io/uplink/private/metaclient"
)

func TestAggregator(t *testing.T) {
	items := []metaclient.BatchItem{
		&metaclient.BeginSegmentParams{StreamID: []byte("A")},
		&metaclient.BeginSegmentParams{StreamID: []byte("B")},
		&metaclient.BeginSegmentParams{StreamID: []byte("C")},
		&metaclient.BeginSegmentParams{StreamID: []byte("D")},
	}

	responses := []*pb.BatchResponseItem{
		{Response: &pb.BatchResponseItem_SegmentBegin{SegmentBegin: &pb.BeginSegmentResponse{SegmentId: []byte("1")}}},
		{Response: &pb.BatchResponseItem_SegmentBegin{SegmentBegin: &pb.BeginSegmentResponse{SegmentId: []byte("2")}}},
		{Response: &pb.BatchResponseItem_SegmentBegin{SegmentBegin: &pb.BeginSegmentResponse{SegmentId: []byte("3")}}},
		{Response: &pb.BatchResponseItem_SegmentBegin{SegmentBegin: &pb.BeginSegmentResponse{SegmentId: []byte("4")}}},
	}

	t.Run("Schedule does not flush", func(t *testing.T) {
		batcher := new(fakeBatcher)

		aggregator := New(batcher)
		aggregator.Schedule(items[0])
		aggregator.Schedule(items[1])
		aggregator.Schedule(items[2])

		assert.Len(t, batcher.items, 0)
	})

	t.Run("ExecuteAndFlush flushes and returns last response with nothing else scheduled", func(t *testing.T) {
		batcher := new(fakeBatcher)
		batcher.responses = responses[:1]

		aggregator := New(batcher)

		resp, err := aggregator.ScheduleAndFlush(context.Background(), items[0])
		require.NoError(t, err)
		assert.Equal(t, items[:1], batcher.items)
		assert.Equal(t, metaclient.MakeBatchResponse(items[0].BatchItem(), responses[0]), *resp)
	})

	t.Run("ExecuteAndFlush flushes and returns last response with other items scheduled", func(t *testing.T) {
		batcher := new(fakeBatcher)
		batcher.responses = responses[:4]

		aggregator := New(batcher)
		aggregator.Schedule(items[0])
		aggregator.Schedule(items[1])
		aggregator.Schedule(items[2])

		resp, err := aggregator.ScheduleAndFlush(context.Background(), items[3])
		require.NoError(t, err)
		assert.Equal(t, items[:4], batcher.items)
		assert.Equal(t, metaclient.MakeBatchResponse(items[3].BatchItem(), responses[3]), *resp)
	})

	t.Run("ExecuteAndFlush returns batch error", func(t *testing.T) {
		batcher := new(fakeBatcher)
		batcher.err = errors.New("oh no")

		aggregator := New(batcher)

		resp, err := aggregator.ScheduleAndFlush(context.Background(), items[0])
		assert.EqualError(t, err, "oh no")
		assert.Nil(t, resp)
	})

	t.Run("ExecuteAndFlush fails if batch response is empty", func(t *testing.T) {
		batcher := new(fakeBatcher)
		aggregator := New(batcher)

		resp, err := aggregator.ScheduleAndFlush(context.Background(), items[0])
		assert.EqualError(t, err, "missing batch responses")
		assert.Nil(t, resp)
	})

	t.Run("Flush does not issue batch with nothing scheduled", func(t *testing.T) {
		batcher := new(fakeBatcher)
		aggregator := New(batcher)

		require.NoError(t, aggregator.Flush(context.Background()))
		assert.Empty(t, batcher.items)
	})

	t.Run("Flush flushes", func(t *testing.T) {
		batcher := new(fakeBatcher)
		batcher.responses = responses[3:]

		aggregator := New(batcher)
		aggregator.Schedule(items[0])
		aggregator.Schedule(items[1])
		aggregator.Schedule(items[2])

		err := aggregator.Flush(context.Background())
		require.NoError(t, err)
		assert.Equal(t, items[:3], batcher.items)
	})
}

type fakeBatcher struct {
	items     []metaclient.BatchItem
	responses []*pb.BatchResponseItem
	err       error
}

func (mi *fakeBatcher) Batch(ctx context.Context, items ...metaclient.BatchItem) ([]metaclient.BatchResponse, error) {
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
