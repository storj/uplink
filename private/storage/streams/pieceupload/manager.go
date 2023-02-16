// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package pieceupload

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/zeebo/errs"

	"storj.io/common/pb"
	"storj.io/common/storj"
)

// PieceReader provides a reader for a piece with the given number.
type PieceReader interface {
	PieceReader(num int) io.Reader
}

// LimitsExchanger exchanges piece upload limits.
type LimitsExchanger interface {
	ExchangeLimits(ctx context.Context, segmentID storj.SegmentID, pieceNumbers []int) (storj.SegmentID, []*pb.AddressedOrderLimit, error)
}

// Manager tracks piece uploads for a segment. It provides callers with piece
// data and limits and tracks which uploads have been successful (or not). It
// also manages obtaining new piece upload limits for failed uploads to
// add resiliency to segment uploads. The manager also keeps track of the
// upload results, which the caller can use to commit the segment, including
// the segment ID, which is updated as limits are exchanged.
type Manager struct {
	exchanger   LimitsExchanger
	pieceReader PieceReader

	mu        sync.Mutex
	segmentID storj.SegmentID
	limits    []*pb.AddressedOrderLimit
	next      []int
	failed    []int
	results   []*pb.SegmentPieceUploadResult
}

// NewManager returns a new piece upload manager.
func NewManager(exchanger LimitsExchanger, pieceReader PieceReader, segmentID storj.SegmentID, limits []*pb.AddressedOrderLimit) *Manager {
	next := make([]int, len(limits))
	for num := range next {
		next[num] = len(next) - 1 - num // descending order, because it feels right.
	}
	return &Manager{
		exchanger:   exchanger,
		pieceReader: pieceReader,
		segmentID:   segmentID,
		next:        next,
		limits:      limits,
	}
}

// NextPiece returns a reader and limit for the next piece to upload. It also
// returns a callback that the caller uses to indicate success (along with the
// results) or not. NextPiece may return data with a new limit for a piece that
// was previously attempted but failed. It will return an error failed if there
// are no more pieces to upload or if it was unable to exchange the limit for a
// failed piece.
func (mgr *Manager) NextPiece(ctx context.Context) (_ io.Reader, _ *pb.AddressedOrderLimit, _ func(hash *pb.PieceHash, uploaded bool), err error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if len(mgr.next) == 0 {
		if err := mgr.exchangeLimitsUnderLock(ctx); err != nil {
			return nil, nil, nil, err
		}
	}

	// Grab the next piece to upload from the tail of the next list
	num := mgr.next[len(mgr.next)-1]
	mgr.next = mgr.next[:len(mgr.next)-1]

	limit := mgr.limits[num]
	piece := mgr.pieceReader.PieceReader(num)

	done := func(hash *pb.PieceHash, uploaded bool) {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()

		// If the piece upload failed, then add it to the failed list so that
		// it can be retried with a new order limit if/when needed to keep the
		// upload going.
		if !uploaded {
			mgr.failed = append(mgr.failed, num)
			return
		}

		mgr.results = append(mgr.results, &pb.SegmentPieceUploadResult{
			PieceNum: int32(num),
			NodeId:   limit.Limit.StorageNodeId,
			Hash:     hash,
		})
	}

	return piece, limit, done, nil
}

// Results returns the results of each piece successfully updated as well as
// the segment ID, which may differ from that passed into NewManager if piece
// limits needed to be exchanged for failed piece uploads.
func (mgr *Manager) Results() (storj.SegmentID, []*pb.SegmentPieceUploadResult) {
	mgr.mu.Lock()
	segmentID := mgr.segmentID
	results := append([]*pb.SegmentPieceUploadResult(nil), mgr.results...)
	mgr.mu.Unlock()

	// The satellite expects the results to be sorted by piece number...
	sort.Slice(results, func(i, j int) bool {
		return results[i].PieceNum < results[j].PieceNum
	})

	return segmentID, results
}

func (mgr *Manager) exchangeLimitsUnderLock(ctx context.Context) error {
	if len(mgr.failed) == 0 {
		// purely defensive: shouldn't happen.
		return errs.New("failed piece list is empty")
	}

	segmentID, limits, err := mgr.exchanger.ExchangeLimits(ctx, mgr.segmentID, mgr.failed)
	if err != nil {
		return errs.New("piece limit exchange failed: %w", err)
	}
	mgr.segmentID = segmentID
	mgr.limits = limits
	mgr.next = append(mgr.next, mgr.failed...)
	mgr.failed = mgr.failed[:0]
	return nil
}
