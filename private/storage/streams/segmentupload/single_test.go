// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package segmentupload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/eestream/scheduler"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams/splitter"
)

const (
	optimalShares = 3
)

var (
	fastKind = nodeKind{0: 1}
	slowKind = nodeKind{0: 2}
	badKind  = nodeKind{0: 3}

	rs             = mustNewRedundancyStrategy()
	fakeSegmentID  = storj.SegmentID{0xFF}
	fakePrivateKey = mustNewPiecePrivateKey()
	minimumLimits  = makeLimits(fastKind, fastKind, fastKind)

	fakeSegmentInfo = &splitter.SegmentInfo{
		Encryption:    metaclient.SegmentEncryption{EncryptedKeyNonce: storj.Nonce{0: 1}},
		PlainSize:     123,
		EncryptedSize: 456,
	}
)

func TestBegin(t *testing.T) {
	const safetyMargin = 1

	for _, tc := range []struct {
		desc                 string
		beginSegment         *metaclient.BeginSegmentResponse
		overrideContext      func(*testing.T, context.Context) context.Context
		overrideSafetyMargin func() int
		expectBeginErr       string
		expectWaitErr        string
		expectUploaderCount  int // expected number of many concurrent piece uploads
	}{
		{
			desc:           "begin segment response missing private key",
			beginSegment:   &metaclient.BeginSegmentResponse{RedundancyStrategy: rs, Limits: minimumLimits},
			expectBeginErr: "begin segment response is missing piece private key",
		},
		{
			desc:           "begin segment response missing redundancy strategy",
			beginSegment:   &metaclient.BeginSegmentResponse{Limits: minimumLimits, PiecePrivateKey: fakePrivateKey},
			expectBeginErr: "begin segment response is missing redundancy strategy",
		},
		{
			desc:           "begin segment response does not have any limits",
			beginSegment:   &metaclient.BeginSegmentResponse{RedundancyStrategy: rs, PiecePrivateKey: fakePrivateKey},
			expectBeginErr: fmt.Sprintf("begin segment response needs at least %d limits to meet optimal threshold but has 0", optimalShares),
		},
		{
			desc:           "begin segment response does not have enough limits",
			beginSegment:   makeBeginSegment(fastKind, fastKind),
			expectBeginErr: fmt.Sprintf("begin segment response needs at least %d limits to meet optimal threshold but has %d", optimalShares, optimalShares-1),
		},
		{
			desc:                 "negative safety margin",
			beginSegment:         &metaclient.BeginSegmentResponse{RedundancyStrategy: rs, Limits: minimumLimits},
			overrideSafetyMargin: func() int { return -1 },
			expectBeginErr:       "safety margin must be non-negative",
		},
		{
			desc:                 "zero safety margin",
			beginSegment:         makeBeginSegment(fastKind, fastKind, fastKind, fastKind),
			overrideSafetyMargin: func() int { return 0 },
			expectUploaderCount:  optimalShares,
		},
		{
			desc:                "upload count is capped to limits",
			beginSegment:        makeBeginSegment(fastKind, fastKind, fastKind),
			expectUploaderCount: optimalShares,
		},
		{
			desc:                "upload count does not exceed optimal threshold + safety margin",
			beginSegment:        makeBeginSegment(fastKind, fastKind, fastKind, slowKind, fastKind),
			expectUploaderCount: optimalShares + safetyMargin,
		},
		{
			desc:                "slow piece uploads are cancelled after optimal threshold hit",
			beginSegment:        makeBeginSegment(fastKind, fastKind, fastKind, slowKind),
			expectUploaderCount: optimalShares + safetyMargin,
		},
		{
			desc:         "aborts immediately when context already cancelled",
			beginSegment: makeBeginSegment(slowKind, fastKind, fastKind, fastKind),
			overrideContext: func(t *testing.T, ctx context.Context) context.Context {
				ctx, cancel := context.WithCancel(ctx)
				cancel()
				return ctx
			},
			expectBeginErr: "failed to obtain piece upload resource",
		},
		{
			desc:          "fails when not enough successful pieces were uploaded",
			beginSegment:  makeBeginSegment(badKind, fastKind, fastKind),
			expectWaitErr: "failed to upload enough pieces (needed at least 3 but got 2); piece limit exchange failed: exchanges disallowed for test",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var (
				segment         = new(fakeSegment)
				limitsExchanger = new(fakeLimitsExchanger)
				piecePutter     = new(fakePiecePutter)
				sched           = newWrappedScheduler()
				safetyMargin    = safetyMargin
			)
			ctx := context.Background()
			if tc.overrideContext != nil {
				ctx = tc.overrideContext(t, ctx)
			}
			if tc.overrideSafetyMargin != nil {
				safetyMargin = tc.overrideSafetyMargin()
			}
			upload, err := Begin(ctx, tc.beginSegment, segment, limitsExchanger, piecePutter, sched, safetyMargin)
			if tc.expectBeginErr != "" {
				require.EqualError(t, err, tc.expectBeginErr)
				require.NoError(t, sched.check(0))
				return
			}
			require.NoError(t, err)

			commitSegment, err := upload.Wait()

			// pass or fail, the segment should always be marked as done reading with the upload error
			require.Equal(t, err, segment.err)

			if tc.expectWaitErr != "" {
				require.EqualError(t, err, tc.expectWaitErr)
				return
			}

			require.NoError(t, err)

			require.Equal(t, &metaclient.CommitSegmentParams{
				SegmentID:         fakeSegmentID,
				Encryption:        fakeSegmentInfo.Encryption,
				SizeEncryptedData: fakeSegmentInfo.EncryptedSize,
				PlainSize:         fakeSegmentInfo.PlainSize,
				EncryptedTag:      nil,
				// The uploads with the first three limits/pieces always
				// succeed due to the way the tests are constructed above. If
				// that changes then this code needs to be updated to be more
				// flexible here.
				UploadResult: []*pb.SegmentPieceUploadResult{
					{PieceNum: 0, NodeId: fastNodeID(0), Hash: &pb.PieceHash{PieceId: pieceID(0)}},
					{PieceNum: 1, NodeId: fastNodeID(1), Hash: &pb.PieceHash{PieceId: pieceID(1)}},
					{PieceNum: 2, NodeId: fastNodeID(2), Hash: &pb.PieceHash{PieceId: pieceID(2)}},
				},
			}, commitSegment)
			require.NoError(t, sched.check(tc.expectUploaderCount))
		})
	}
}

type nodeKind storj.NodeID

func isNodeKind(nodeID storj.NodeID, kind nodeKind) bool {
	return nodeID[0] == kind[0]
}

func makeBeginSegment(kinds ...nodeKind) *metaclient.BeginSegmentResponse {
	return &metaclient.BeginSegmentResponse{
		SegmentID:          fakeSegmentID,
		Limits:             makeLimits(kinds...),
		RedundancyStrategy: rs,
		PiecePrivateKey:    fakePrivateKey,
	}
}

func makeNodeID(i int, kind nodeKind) storj.NodeID {
	nodeID := storj.NodeID(kind)
	nodeID[1] = byte(i)
	return nodeID
}

func fastNodeID(i int) storj.NodeID {
	return makeNodeID(i, fastKind)
}

func makeNodeIDs(kinds ...nodeKind) []storj.NodeID {
	var nodes []storj.NodeID
	for i, kind := range kinds {
		nodes = append(nodes, makeNodeID(i, kind))
	}
	return nodes
}

func makeLimits(kinds ...nodeKind) []*pb.AddressedOrderLimit {
	var limits []*pb.AddressedOrderLimit
	for i, nodeID := range makeNodeIDs(kinds...) {
		limits = append(limits, &pb.AddressedOrderLimit{
			Limit: &pb.OrderLimit{
				StorageNodeId: nodeID,
				PieceId:       pieceID(i),
			},
		})
	}
	return limits
}

func pieceID(num int) storj.PieceID {
	return storj.PieceID{0: byte(num)}
}

type fakeSegment struct {
	splitter.Segment
	err error
}

func (fakeSegment) Reader() io.Reader {
	return bytes.NewReader(nil)
}

func (s *fakeSegment) DoneReading(err error) {
	s.err = err
}

func (fakeSegment) Finalize() *splitter.SegmentInfo {
	return fakeSegmentInfo
}

type fakeLimitsExchanger struct{}

func (fakeLimitsExchanger) ExchangeLimits(ctx context.Context, segmentID storj.SegmentID, pieceNumbers []int) (storj.SegmentID, []*pb.AddressedOrderLimit, error) {
	return nil, nil, errors.New("exchanges disallowed for test")
}

type fakePiecePutter struct{}

func (fakePiecePutter) PutPiece(longTailCtx, uploadCtx context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (hash *pb.PieceHash, deprecated *struct{}, err error) {
	if !bytes.Equal(fakePrivateKey.Bytes(), privateKey.Bytes()) {
		return nil, nil, errs.New("private key was not passed correctly")
	}

	if _, err := io.ReadAll(data); err != nil {
		return nil, nil, err
	}

	switch {
	case isNodeKind(limit.Limit.StorageNodeId, badKind):
		return nil, nil, errs.New("piece upload failed")
	case isNodeKind(limit.Limit.StorageNodeId, slowKind):
		select {
		case <-longTailCtx.Done():
			return nil, nil, longTailCtx.Err()
		case <-uploadCtx.Done():
			return nil, nil, uploadCtx.Err()
		}
	}
	return &pb.PieceHash{PieceId: limit.Limit.PieceId}, nil, nil
}

type wrappedScheduler struct {
	wrapped *scheduler.Scheduler
	handles []*wrappedHandle
}

func newWrappedScheduler() *wrappedScheduler {
	return &wrappedScheduler{
		wrapped: scheduler.New(scheduler.Options{MaximumConcurrent: 200}),
	}
}

func (w *wrappedScheduler) Join() scheduler.Handle {
	handle := &wrappedHandle{wrapped: w.wrapped.Join()}
	w.handles = append(w.handles, handle)
	return handle
}

func (w *wrappedScheduler) check(expectedUploaderCount int) error {
	if len(w.handles) != 1 {
		return errs.New("expected one handle but got %d", len(w.handles))
	}
	return w.handles[0].check(expectedUploaderCount)
}

type wrappedHandle struct {
	wrapped scheduler.Handle

	mu        sync.Mutex
	done      int
	resources []*wrappedResource
}

func (w *wrappedHandle) Get(ctx context.Context) (scheduler.Resource, bool) {
	if err := ctx.Err(); err != nil {
		return nil, false
	}
	wrapped, ok := w.wrapped.Get(ctx)
	if !ok {
		return nil, false
	}
	resource := &wrappedResource{wrapped: wrapped}
	w.resources = append(w.resources, resource)
	return resource, true
}

func (w *wrappedHandle) Done() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.done++
}

func (w *wrappedHandle) check(resources int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case w.done == 0:
		return errs.New("done not called")
	case w.done > 1:
		return errs.New("done more than once (%d times)", w.done)
	case len(w.resources) != resources:
		return errs.New("expected %d resource(s) but got %d", resources, len(w.resources))
	}
	var eg errs.Group
	for i, resource := range w.resources {
		if err := resource.check(); err != nil {
			eg.Add(errs.New("resource(%d): %w", i, err))
		}
	}
	return eg.Err()
}

type wrappedResource struct {
	wrapped scheduler.Resource

	mu   sync.Mutex
	done int
}

func (r *wrappedResource) Done() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done++
}

func (r *wrappedResource) check() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.done == 0:
		return errs.New("done not called")
	case r.done > 1:
		return errs.New("done more than once (%d times)", r.done)
	}
	return nil
}

func mustNewPiecePrivateKey() storj.PiecePrivateKey {
	pk, err := storj.PiecePrivateKeyFromBytes(bytes.Repeat([]byte{1}, 64))
	if err != nil {
		panic(err)
	}
	return pk
}

func mustNewRedundancyStrategy() eestream.RedundancyStrategy {
	rs, err := eestream.NewRedundancyStrategyFromStorj(storj.RedundancyScheme{
		Algorithm:      storj.ReedSolomon,
		ShareSize:      64,
		RequiredShares: 1,
		RepairShares:   2,
		OptimalShares:  int16(optimalShares),
		TotalShares:    4,
	})
	if err != nil {
		panic(err)
	}
	return rs
}
