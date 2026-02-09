// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package segmenttracker

import (
	"context"
	"sync"

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/uplink/private/metaclient"
)

var mon = monkit.Package()

// Segment represents a segment being tracked.
type Segment interface {
	Position() metaclient.SegmentPosition
	EncryptUserData(metaclient.SegmentUserData) (metaclient.EncryptedSegmentUserData, error)
}

// BatchScheduler schedules batch items to be issued.
type BatchScheduler interface {
	Schedule(batchItem metaclient.BatchItem)
}

// Tracker tracks segments as they are completed for the purpose of encrypting
// and setting user data final segment. It hold backs scheduling of the last
// known segment until Flush is called, at which point it verifies that the
// held back segment is indeed the last segment, encrypts the user data using
// that segment, and injects it into the batch item that commits that segment
// (i.e. MakeInlineSegment or CommitSegment). If the segments are not part of
// a multipart upload (i.e. no eTag to apply), then the tracker schedules
// the segment batch items immediately.
type Tracker struct {
	scheduler  BatchScheduler
	userDataCh <-chan metaclient.SegmentUserData

	mu sync.Mutex

	lastIndex *int32

	heldBackSegment   Segment
	heldBackBatchItem metaclient.BatchItem
}

// New returns a new tracker that uses the given batch scheduler to schedule
// segment commit items.
func New(scheduler BatchScheduler, userDataCh <-chan metaclient.SegmentUserData) *Tracker {
	return &Tracker{
		scheduler:  scheduler,
		userDataCh: userDataCh,
	}
}

// SegmentDone notifies the tracker that a segment upload has completed and
// supplies the batchItem that needs to be scheduled to commit the segment ( or
// create the inline segment). If this is the last segment seen so far, it will
// not be scheduled immediately, and will instead be scheduled when a later
// segment finishes or Flush is called. If the tracker was given a nil userDataCh
// channel, then the segment batch item is scheduled immediately.
func (t *Tracker) SegmentDone(segment Segment, batchItem metaclient.BatchItem) {
	// If there is no data to encrypt, then there is no reason to gate the
	// scheduling.
	if t.userDataCh == nil {
		t.scheduler.Schedule(batchItem)
		return
	}

	index := segment.Position().Index

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.heldBackSegment != nil {
		// If the segment comes before the held back segment then it can be
		// scheduled immediately since we know it cannot be the last segment.
		if index < t.heldBackSegment.Position().Index {
			t.scheduler.Schedule(batchItem)
			return
		}
		// The held back segment can be scheduled immediately since it comes
		// before this segment and is therefore not the last segment.
		t.scheduler.Schedule(t.heldBackBatchItem)
	}

	t.heldBackSegment = segment
	t.heldBackBatchItem = batchItem
}

// SegmentsScheduled is invoked when the last segment upload has been
// scheduled. It allows the tracker to verify that the held back segment is
// actually the last segment when Flush is called.
func (t *Tracker) SegmentsScheduled(lastSegment Segment) {
	t.mu.Lock()
	defer t.mu.Unlock()
	lastIndex := lastSegment.Position().Index
	t.lastIndex = &lastIndex
}

// Flush schedules the held back segment. It must only be called at least one
// call to SegmentDone as well as SegmentsScheduled. It verifies that the held
// back segment is the last segment (as indicited by SegmentsScheduled). Before
// scheduling the last segment, it reads the user data from the userDataCh channel
// provided in New, encrypts that user data with the last segment, and then injects
// the encrypted user data into the batch item that commits that segment (i.e.
// MakeInlineSegment or CommitSegment).
func (t *Tracker) Flush(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)

	if t.userDataCh == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// There should ALWAYS be a held back segment here.
	if t.heldBackSegment == nil {
		return errs.New("programmer error: no segment has been held back")
	}

	if t.lastIndex == nil {
		return errs.New("programmer error: cannot flush before last segment known")
	}

	// The held back segment should ALWAYS be the last segment
	if heldBackIndex := t.heldBackSegment.Position().Index; heldBackIndex != *t.lastIndex {
		return errs.New("programmer error: expected held back segment with index %d to have last segment index %d", heldBackIndex, *t.lastIndex)
	}

	if err := t.addEncryptedUserData(ctx, t.heldBackSegment, t.heldBackBatchItem); err != nil {
		return errs.Wrap(err)
	}

	t.scheduler.Schedule(t.heldBackBatchItem)
	t.heldBackSegment = nil
	t.heldBackBatchItem = nil
	return nil
}

func (t *Tracker) addEncryptedUserData(ctx context.Context, lastSegment Segment, batchItem metaclient.BatchItem) (err error) {
	defer mon.Task()(&ctx)(&err)

	select {
	case userData := <-t.userDataCh:
		if userData.IsZero() {
			// Either an empty set of user data was provided by the caller, or - more likely -
			// the user data was not provided before Commit was called.
			return nil
		}

		encryptedUserData, err := lastSegment.EncryptUserData(userData)
		if err != nil {
			return errs.New("failed to encrypt user data: %w", err)
		}
		switch batchItem := batchItem.(type) {
		case *metaclient.MakeInlineSegmentParams:
			batchItem.EncryptedUserData = encryptedUserData
		case *metaclient.CommitSegmentParams:
			batchItem.EncryptedUserData = encryptedUserData
		default:
			return errs.New("unhandled segment batch item type: %T", batchItem)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
