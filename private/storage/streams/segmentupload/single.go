// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package segmentupload

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/eestream/scheduler"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams/pieceupload"
	"storj.io/uplink/private/storage/streams/splitter"
)

// Scheduler is used to coordinate and constrain resources between
// concurrent segment uploads.
type Scheduler interface {
	Join() scheduler.Handle
}

// Begin starts a segment upload identified by the segment ID provided in the
// beginSegment response. The returned upload will complete when enough piece
// uploads to fulfill the optimal threshold for the segment redundancy strategy
// plus a small long tail margin. It cancels remaining piece uploads once that
// threshold has been hit.
func Begin(ctx context.Context,
	beginSegment *metaclient.BeginSegmentResponse,
	segment splitter.Segment,
	limitsExchanger pieceupload.LimitsExchanger,
	piecePutter pieceupload.PiecePutter,
	scheduler Scheduler,
	longTailMargin int,
) (_ *Upload, err error) {
	// Join the scheduler so the concurrency can be limited appropriately.
	handle := scheduler.Join()
	defer func() {
		if err != nil {
			handle.Done()
		}
	}()

	if longTailMargin < 0 {
		return nil, errs.New("long tail margin must be non-negative")
	}
	if beginSegment.RedundancyStrategy.ErasureScheme == nil {
		return nil, errs.New("begin segment response is missing redundancy strategy")
	}
	if beginSegment.PiecePrivateKey.IsZero() {
		return nil, errs.New("begin segment response is missing piece private key")
	}

	optimalThreshold := beginSegment.RedundancyStrategy.OptimalThreshold()
	if optimalThreshold > len(beginSegment.Limits) {
		return nil, errs.New("begin segment response needs at least %d limits to meet optimal threshold but has %d", optimalThreshold, len(beginSegment.Limits))
	}

	// The number of uploads is enough to satisfy the optimal threshold plus
	// a small long tail margin, capped by the number of limits.
	uploaderCount := optimalThreshold + longTailMargin
	if uploaderCount > len(beginSegment.Limits) {
		uploaderCount = len(beginSegment.Limits)
	}

	mgr := pieceupload.NewManager(
		limitsExchanger,
		&pieceReader{segment, beginSegment.RedundancyStrategy},
		beginSegment.SegmentID,
		beginSegment.Limits,
	)

	wg := new(sync.WaitGroup)
	defer func() {
		if err != nil {
			wg.Wait()
		}
	}()

	// Create a context that we can use to cancel piece uploads when we have enough.
	longTailCtx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	results := make(chan segmentResult, uploaderCount)
	var successful int32
	for i := 0; i < uploaderCount; i++ {
		res, ok := handle.Get(ctx)
		if !ok {
			return nil, errs.New("failed to obtain piece upload resource")
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			// Whether the upload is ultimately successful or not, when this
			// function returns, the scheduler resource MUST be released to
			// allow other piece uploads to take place.
			defer res.Done()
			uploaded, err := pieceupload.UploadOne(longTailCtx, ctx, mgr, piecePutter, beginSegment.PiecePrivateKey)
			results <- segmentResult{uploaded: uploaded, err: err}
			if uploaded {
				// Piece upload was successful. If we have met or surpassed the
				// optimal threshold, we can cancel the rest.
				if int(atomic.AddInt32(&successful, 1)) >= optimalThreshold {
					cancel()
				}
			}
		}()
	}

	return &Upload{
		ctx:              ctx,
		optimalThreshold: beginSegment.RedundancyStrategy.OptimalThreshold(),
		handle:           handle,
		results:          results,
		cancel:           cancel,
		wg:               wg,
		mgr:              mgr,
		segment:          segment,
	}, nil
}

type pieceReader struct {
	segment    splitter.Segment
	redundancy eestream.RedundancyStrategy
}

func (r *pieceReader) PieceReader(num int) io.Reader {
	segment := r.segment.Reader()
	stripeSize := r.redundancy.StripeSize()
	paddedData := encryption.PadReader(io.NopCloser(segment), stripeSize)
	return NewEncodedReader(paddedData, r.redundancy, num)
}

type segmentResult struct {
	uploaded bool
	err      error
}

// Upload is a segment upload that has been started and returned by the Begin
// method.
type Upload struct {
	ctx              context.Context
	optimalThreshold int
	handle           scheduler.Handle
	results          chan segmentResult
	cancel           context.CancelFunc
	wg               *sync.WaitGroup
	mgr              *pieceupload.Manager
	segment          splitter.Segment
}

// Wait blocks until the segment upload completes. It will be successful as
// long as enough pieces have uploaded successfully.
func (upload *Upload) Wait() (*metaclient.CommitSegmentParams, error) {
	defer upload.handle.Done()
	defer upload.cancel()

	var eg errs.Group
	var successful int
	for i := 0; i < cap(upload.results); i++ {
		result := <-upload.results
		if result.uploaded {
			successful++
		}
		eg.Add(result.err)
	}

	// The goroutines should all be on their way to exiting since the loop
	// above guarantees they have written their results to the channel. Wait
	// for them to all finish and release the scheduler resource. This is
	// really only necessary for deterministic testing.
	upload.wg.Wait()

	var err error
	if successful < upload.optimalThreshold {
		err = errs.Combine(errs.New("failed to upload enough pieces (needed at least %d but got %d)", upload.optimalThreshold, successful), eg.Err())
	}
	upload.segment.DoneReading(err)
	if err != nil {
		return nil, err
	}

	info := upload.segment.Finalize()
	segmentID, results := upload.mgr.Results()

	return &metaclient.CommitSegmentParams{
		SegmentID:         segmentID,
		Encryption:        info.Encryption,
		SizeEncryptedData: info.EncryptedSize,
		PlainSize:         info.PlainSize,
		EncryptedTag:      nil, // encrypted eTag is injected by a different layer
		UploadResult:      results,
	}, nil
}
