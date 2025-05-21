// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package segmenttracker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/uplink/private/metaclient"
)

func TestTracker(t *testing.T) {
	setup := func(eTag string) (*Tracker, *fakeBatchScheduler) {
		eTagCh := make(chan []byte, 1)
		eTagCh <- []byte(eTag)
		scheduler := new(fakeBatchScheduler)
		return New(scheduler, eTagCh), scheduler
	}

	segment := func(index int32) Segment {
		return &fakeSegment{index: index}
	}

	badSegment := func(index int32) Segment {
		return &fakeSegment{index: index, err: errors.New("oh no")}
	}

	makeInlineSegmentWithEncryptedETag := func(index int32, encryptedETag string) metaclient.BatchItem {
		return &metaclient.MakeInlineSegmentParams{PlainSize: int64(index), EncryptedETag: []byte(encryptedETag)}
	}

	makeInlineSegment := func(index int32) metaclient.BatchItem {
		return makeInlineSegmentWithEncryptedETag(index, "")
	}

	commitSegmentWithEncryptedETag := func(index int32, encryptedETag string) metaclient.BatchItem {
		return &metaclient.CommitSegmentParams{PlainSize: int64(index), EncryptedETag: []byte(encryptedETag)}
	}

	commitSegment := func(index int32) metaclient.BatchItem {
		return commitSegmentWithEncryptedETag(index, "")
	}

	t.Run("SegmentDone holds back highest seen segment", func(t *testing.T) {
		tracker, scheduler := setup("")

		// 2 will should be held back
		tracker.SegmentDone(segment(2), makeInlineSegment(2))
		scheduler.AssertScheduledAndReset(t)

		// 3 done will allow 1 to be scheduled
		tracker.SegmentDone(segment(3), makeInlineSegment(3))
		scheduler.AssertScheduledAndReset(t, makeInlineSegment(2))

		// 1 will be scheduled immediate since 3 is known and higher
		tracker.SegmentDone(segment(1), makeInlineSegment(1))
		scheduler.AssertScheduledAndReset(t, makeInlineSegment(1))

		// 4 will still be held back (and 3 scheduled) even though 5 is
		// known to be the last segment.
		tracker.SegmentsScheduled(segment(5))
		tracker.SegmentDone(segment(4), makeInlineSegment(4))
		scheduler.AssertScheduledAndReset(t, makeInlineSegment(3))
	})

	t.Run("SegmentDone immediately schedules when etag is not a concern", func(t *testing.T) {
		scheduler := new(fakeBatchScheduler)
		tracker := New(scheduler, nil)

		tracker.SegmentDone(segment(1), makeInlineSegment(1))
		scheduler.AssertScheduledAndReset(t, makeInlineSegment(1))
	})

	t.Run("Flush flushes the last segment", func(t *testing.T) {
		t.Run("MakeInlineSegment", func(t *testing.T) {
			tracker, scheduler := setup("etag")
			tracker.SegmentDone(segment(1), makeInlineSegment(1))
			tracker.SegmentsScheduled(segment(1))

			scheduler.AssertScheduledAndReset(t)
			err := tracker.Flush(context.Background())
			require.NoError(t, err)
			scheduler.AssertScheduledAndReset(t, makeInlineSegmentWithEncryptedETag(1, "etag-1"))
		})
		t.Run("CommitSegment", func(t *testing.T) {
			tracker, scheduler := setup("etag")
			tracker.SegmentDone(segment(1), commitSegment(1))
			tracker.SegmentsScheduled(segment(1))

			scheduler.AssertScheduledAndReset(t)
			err := tracker.Flush(context.Background())
			require.NoError(t, err)
			scheduler.AssertScheduledAndReset(t, commitSegmentWithEncryptedETag(1, "etag-1"))
		})
	})

	t.Run("Flush responds to context cancellation waiting for etag", func(t *testing.T) {
		scheduler := new(fakeBatchScheduler)

		tracker := New(scheduler, make(chan []byte))
		tracker.SegmentDone(segment(1), makeInlineSegment(1))
		tracker.SegmentsScheduled(segment(1))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := tracker.Flush(ctx)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("Flush does not encrypt an empty etag", func(t *testing.T) {
		tracker, scheduler := setup("")
		tracker.SegmentDone(segment(1), makeInlineSegment(1))
		tracker.SegmentsScheduled(segment(1))

		err := tracker.Flush(context.Background())
		require.NoError(t, err)
		scheduler.AssertScheduledAndReset(t, makeInlineSegment(1))
	})

	t.Run("Flush fails if last segment was never done", func(t *testing.T) {
		tracker, _ := setup("etag")
		tracker.SegmentDone(segment(1), makeInlineSegment(1))
		tracker.SegmentsScheduled(segment(2))
		err := tracker.Flush(context.Background())
		require.EqualError(t, err, "programmer error: expected held back segment with index 1 to have last segment index 2")
	})

	t.Run("Flush fails if last segment batch item is unhandled", func(t *testing.T) {
		tracker, _ := setup("etag")
		tracker.SegmentDone(segment(1), &metaclient.BeginSegmentParams{})
		tracker.SegmentsScheduled(segment(1))
		err := tracker.Flush(context.Background())
		require.EqualError(t, err, "unhandled segment batch item type: *metaclient.BeginSegmentParams")
	})

	t.Run("Flush before SegmentDone is an error", func(t *testing.T) {
		tracker, _ := setup("")
		err := tracker.Flush(context.Background())
		require.EqualError(t, err, "programmer error: no segment has been held back")
	})

	t.Run("Flush before SegmentsScheduled is an error", func(t *testing.T) {
		tracker, _ := setup("")
		tracker.SegmentDone(segment(1), makeInlineSegment(1))
		err := tracker.Flush(context.Background())
		require.EqualError(t, err, "programmer error: cannot flush before last segment known")
	})

	t.Run("Flush fails if eTag cannot be encrypted", func(t *testing.T) {
		tracker, _ := setup("etag")
		tracker.SegmentDone(badSegment(1), makeInlineSegment(1))
		tracker.SegmentsScheduled(segment(1))
		err := tracker.Flush(context.Background())
		require.EqualError(t, err, "failed to encrypt eTag: oh no")
	})

	t.Run("Flush is no-op when etag is not a concern", func(t *testing.T) {
		tracker := New(new(fakeBatchScheduler), nil)
		err := tracker.Flush(context.Background())
		require.NoError(t, err)
	})
}

type fakeBatchScheduler struct {
	scheduled []metaclient.BatchItem
}

func (s *fakeBatchScheduler) Schedule(batchItem metaclient.BatchItem) {
	s.scheduled = append(s.scheduled, batchItem)
}

func (s *fakeBatchScheduler) AssertScheduledAndReset(t *testing.T, expected ...metaclient.BatchItem) {
	assert.Equal(t, expected, s.scheduled)
	s.scheduled = nil
}

type fakeSegment struct {
	index int32
	err   error
}

func (s *fakeSegment) Position() metaclient.SegmentPosition {
	return metaclient.SegmentPosition{Index: s.index}
}

func (s *fakeSegment) EncryptETag(eTag []byte) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	return fmt.Appendf(nil, "%s-%d", string(eTag), s.index), nil
}
