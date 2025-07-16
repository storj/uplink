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
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/eestream/scheduler"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/stalldetection"
	"storj.io/uplink/private/storage/streams/splitter"
)

const (
	optimalShares = 3
	totalShares   = 4
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
	const longTailMargin = 1

	for _, tc := range []struct {
		desc                   string
		beginSegment           *metaclient.BeginSegmentResponse
		overrideContext        func(*testing.T, context.Context) context.Context
		overrideLongTailMargin func() int
		stallConfig            *stalldetection.Config
		expectBeginErr         string
		expectWaitErr          string
		expectUploaderCount    int // expected number of many concurrent piece uploads
		expectedStalls         int // expected number of stalled uploads
	}{
		{
			desc:           "begin segment response missing private key",
			beginSegment:   &metaclient.BeginSegmentResponse{RedundancyStrategy: rs, Limits: minimumLimits},
			expectBeginErr: "begin segment response is missing piece private key",
			expectedStalls: 0,
		},
		{
			desc:           "begin segment response missing redundancy strategy",
			beginSegment:   &metaclient.BeginSegmentResponse{Limits: minimumLimits, PiecePrivateKey: fakePrivateKey},
			expectBeginErr: "begin segment response is missing redundancy strategy",
			expectedStalls: 0,
		},
		{
			desc:           "begin segment response does not have any limits",
			beginSegment:   &metaclient.BeginSegmentResponse{RedundancyStrategy: rs, PiecePrivateKey: fakePrivateKey},
			expectBeginErr: fmt.Sprintf("begin segment response needs at least %d limits to meet optimal threshold but has 0", optimalShares),
			expectedStalls: 0,
		},
		{
			desc:           "begin segment response does not have enough limits",
			beginSegment:   makeBeginSegment(fastKind, fastKind),
			expectBeginErr: fmt.Sprintf("begin segment response needs at least %d limits to meet optimal threshold but has %d", optimalShares, optimalShares-1),
			expectedStalls: 0,
		},
		{
			desc:                   "negative long tail margin",
			beginSegment:           makeBeginSegment(fastKind, fastKind, fastKind, slowKind),
			overrideLongTailMargin: func() int { return -1 },
			expectUploaderCount:    totalShares,
			expectedStalls:         0,
		},
		{
			desc:                   "zero long tail margin",
			beginSegment:           makeBeginSegment(fastKind, fastKind, fastKind, slowKind),
			overrideLongTailMargin: func() int { return 0 },
			expectUploaderCount:    optimalShares,
			expectedStalls:         0,
		},
		{
			desc:                "upload count is capped to limits",
			beginSegment:        makeBeginSegment(fastKind, fastKind, fastKind),
			expectUploaderCount: optimalShares,
			expectedStalls:      0,
		},
		{
			desc:                "upload count does not exceed optimal threshold + long tail margin",
			beginSegment:        makeBeginSegment(fastKind, fastKind, fastKind, slowKind, fastKind),
			expectUploaderCount: optimalShares + longTailMargin,
			expectedStalls:      0,
		},
		{
			desc:                "slow piece uploads are cancelled after optimal threshold hit",
			beginSegment:        makeBeginSegment(fastKind, fastKind, fastKind, slowKind),
			expectUploaderCount: optimalShares + longTailMargin,
			expectedStalls:      0,
		},
		{
			desc:         "aborts immediately when context already cancelled",
			beginSegment: makeBeginSegment(slowKind, fastKind, fastKind, fastKind),
			overrideContext: func(t *testing.T, ctx context.Context) context.Context {
				ctx, cancel := context.WithCancel(ctx)
				cancel()
				return ctx
			},
			expectBeginErr: "failed to obtain piece upload handle",
			expectedStalls: 0,
		},
		{
			desc:           "fails when not enough successful pieces were uploaded",
			beginSegment:   makeBeginSegment(badKind, fastKind, fastKind),
			expectWaitErr:  "failed to upload enough pieces (needed at least 3 but got 2); piece limit exchange failed: exchanges disallowed for test",
			expectedStalls: 0,
		},
		{
			desc:                "successful long tail cancellation after optimal threshold reached",
			beginSegment:        makeBeginSegment(fastKind, fastKind, fastKind, slowKind, slowKind),
			expectUploaderCount: optimalShares + longTailMargin,
			expectedStalls:      0,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var (
				segment         = new(fakeSegment)
				limitsExchanger = new(fakeLimitsExchanger)
				piecePutter     = new(fakePiecePutter)
				sched           = newWrappedScheduler()
				longTailMargin  = longTailMargin
			)
			ctx := context.Background()
			if tc.overrideContext != nil {
				ctx = tc.overrideContext(t, ctx)
			}
			if tc.overrideLongTailMargin != nil {
				longTailMargin = tc.overrideLongTailMargin()
			}
			upload, err := Begin(ctx, tc.beginSegment, segment, limitsExchanger, piecePutter, sched, longTailMargin, tc.stallConfig)
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
				require.Equal(t, tc.expectedStalls, upload.stallsDetected)
				return
			}

			require.NoError(t, err)

			require.Equal(t, &metaclient.CommitSegmentParams{
				SegmentID:         fakeSegmentID,
				Encryption:        fakeSegmentInfo.Encryption,
				SizeEncryptedData: fakeSegmentInfo.EncryptedSize,
				PlainSize:         fakeSegmentInfo.PlainSize,
				EncryptedETag:     nil,
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
			require.Equal(t, tc.expectedStalls, upload.stallsDetected)
			require.NoError(t, sched.check(tc.expectUploaderCount))
		})
	}
}

func TestBeginWithStalls(t *testing.T) {
	const longTailMargin = 1

	for _, tc := range []struct {
		desc                   string
		beginSegment           *metaclient.BeginSegmentResponse
		overrideContext        func(*testing.T, context.Context) context.Context
		overrideLongTailMargin func() int
		stallConfig            *stalldetection.Config
		expectBeginErr         string
		expectWaitErr          string
		expectUploaderCount    int // expected number of many concurrent piece uploads
		expectedStalls         int // expected number of stalled uploads
	}{
		{
			// Stall detection: basic functionality test, no long tail
			// Tests that stall manager is properly initialized and configured
			// After 1 base upload succeed, stall timeout = max(upload_time * 1, 5ms)
			// expect 2 slow nodes to get cancelled.
			// Expected: unsuccessful upload with stall detection enabled
			desc:         "stall manager enabled, no long tail",
			beginSegment: makeBeginSegment(fastKind, slowKind, slowKind),
			stallConfig: &stalldetection.Config{
				BaseUploads:      1,
				Factor:           1,
				MinStallDuration: 5 * time.Millisecond,
			},
			overrideLongTailMargin: func() int { return 0 },
			expectUploaderCount:    optimalShares,
			expectWaitErr:          "failed to upload enough pieces (needed at least 3 but got 1)",
			expectedStalls:         2,
		},
		{
			// Stall detection: basic functionality test, with longtail
			// Tests that stall manager is properly initialized and configured
			// After 1 base upload succeed, stall timeout = max(upload_time * 1, 5ms)
			// expect 3 slow nodes to get cancelled.
			// Expected: unsuccessful upload with stall detection enabled
			desc:         "stall manager enabled, with longtail",
			beginSegment: makeBeginSegment(fastKind, slowKind, slowKind, slowKind),
			stallConfig: &stalldetection.Config{
				BaseUploads:      1,
				Factor:           1,
				MinStallDuration: 5 * time.Millisecond,
			},
			expectUploaderCount: optimalShares + longTailMargin,
			expectWaitErr:       "failed to upload enough pieces (needed at least 3 but got 1)",
			expectedStalls:      3,
		},
		{
			// Stall detection: disabled test
			// Tests that nil stallConfig disables stall detection entirely
			// No stall manager created, uploads rely only on long tail cancellation
			// 3 fast pieces succeed, slow piece waits for long tail cancellation
			// Expected: successful upload without any stall detection
			desc:                "stall detection disabled",
			beginSegment:        makeBeginSegment(fastKind, fastKind, fastKind, slowKind),
			stallConfig:         nil,
			expectUploaderCount: optimalShares + longTailMargin,
			expectedStalls:      0,
		},
		{
			// Long tail vs stall detection: optimal threshold wins race
			// Tests interaction when both mechanisms are active
			// Generous stall config (Factor 3, MinStallDuration 100ms) ensures stall timeout is long
			// 3 fast pieces reach optimal threshold before any stall timeouts trigger
			// Expected: successful upload, optimal threshold cancellation beats stall detection
			desc:         "long tail cancellation with stall detection both active",
			beginSegment: makeBeginSegment(fastKind, fastKind, fastKind, slowKind, slowKind),
			stallConfig: &stalldetection.Config{
				BaseUploads:      2,
				Factor:           3,
				MinStallDuration: 5 * time.Millisecond,
			},
			expectUploaderCount: optimalShares + longTailMargin,
			expectedStalls:      0,
		},
		{
			// Node failure: all bad nodes fail regardless of stall detection
			// Tests that upload fails when all nodes fail, regardless of stall detection settings
			// All 4 bad nodes fail immediately, no pieces succeed
			// Stall detection is irrelevant when no nodes succeed
			// Expected: Begin() succeeds, Wait() fails due to no successful pieces
			desc:         "all bad nodes should fail regardless of stall detection",
			beginSegment: makeBeginSegment(badKind, badKind, badKind, badKind),
			stallConfig: &stalldetection.Config{
				BaseUploads:      1,
				Factor:           1,
				MinStallDuration: 1 * time.Millisecond,
			},
			expectUploaderCount: optimalShares + longTailMargin,
			expectWaitErr:       "failed to upload enough pieces (needed at least 3 but got 0); piece limit exchange failed: exchanges disallowed for test; piece limit exchange failed: exchanges disallowed for test; piece limit exchange failed: exchanges disallowed for test; piece limit exchange failed: exchanges disallowed for test",
			expectedStalls:      0, // Upload should fail when all nodes fail, regardless of stall detection
		},
		{
			// Mixed node types: bad and slow nodes with stall detection
			// Tests stall detection with mixed node types (1 fast, 1 bad, 2 slow)
			// 1 fast piece succeeds, 1 bad piece fails, 2 slow pieces may get stalled
			// After 1st fast piece succeeds, aggressive stall detection may stall slow pieces
			// Result: insufficient successful pieces (1 < 3 optimal threshold)
			// Expected: Begin() succeeds, Wait() fails due to insufficient pieces
			desc:         "mixed bad and slow nodes with stall detection",
			beginSegment: makeBeginSegment(fastKind, badKind, slowKind, slowKind),
			stallConfig: &stalldetection.Config{
				BaseUploads:      1,
				Factor:           1,
				MinStallDuration: 5 * time.Millisecond,
			},
			expectUploaderCount: optimalShares + longTailMargin,
			expectWaitErr:       "failed to upload enough pieces (needed at least 3 but got 1); piece limit exchange failed: exchanges disallowed for test",
			expectedStalls:      2, // Should fail when not enough good nodes available
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var (
				segment         = new(fakeSegment)
				limitsExchanger = new(fakeLimitsExchanger)
				piecePutter     = new(fakePiecePutter)
				sched           = newWrappedScheduler()
				longTailMargin  = longTailMargin
			)
			ctx := context.Background()
			if tc.overrideContext != nil {
				ctx = tc.overrideContext(t, ctx)
			}
			if tc.overrideLongTailMargin != nil {
				longTailMargin = tc.overrideLongTailMargin()
			}
			upload, err := Begin(ctx, tc.beginSegment, segment, limitsExchanger, piecePutter, sched, longTailMargin, tc.stallConfig)
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
				require.Equal(t, tc.expectedStalls, upload.stallsDetected)
				return
			}

			require.NoError(t, err)

			require.Equal(t, &metaclient.CommitSegmentParams{
				SegmentID:         fakeSegmentID,
				Encryption:        fakeSegmentInfo.Encryption,
				SizeEncryptedData: fakeSegmentInfo.EncryptedSize,
				PlainSize:         fakeSegmentInfo.PlainSize,
				EncryptedETag:     nil,
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
			require.Equal(t, tc.expectedStalls, upload.stallsDetected)
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

func (fakeSegment) Position() metaclient.SegmentPosition {
	return metaclient.SegmentPosition{}
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

func (w *wrappedScheduler) Join(ctx context.Context) (scheduler.Handle, bool) {
	wrapped, ok := w.wrapped.Join(ctx)
	if !ok {
		return nil, false
	}
	handle := &wrappedHandle{wrapped: wrapped}
	w.handles = append(w.handles, handle)
	return handle, true
}

func (w *wrappedScheduler) check(expectedUploaderCount int) error {
	// we're allowed 0 handles if we expected no upload resources
	if expectedUploaderCount == 0 && len(w.handles) == 0 {
		return nil
	}
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
		TotalShares:    int16(totalShares),
	})
	if err != nil {
		panic(err)
	}
	return rs
}

func TestGenerateUploadID(t *testing.T) {
	t.Run("generates unique IDs", func(t *testing.T) {
		id1 := generateUploadID()
		id2 := generateUploadID()
		id3 := generateUploadID()

		require.NotEqual(t, id1, id2)
		require.NotEqual(t, id2, id3)
		require.NotEqual(t, id1, id3)
	})

	t.Run("IDs have correct format", func(t *testing.T) {
		id := generateUploadID()
		require.Contains(t, id, "upload-")
		require.Regexp(t, `^upload-\d+$`, id)
	})

	t.Run("IDs are incrementing", func(t *testing.T) {
		id1 := generateUploadID()
		id2 := generateUploadID()

		// Extract numbers from IDs
		var num1, num2 int64
		_, err1 := fmt.Sscanf(id1, "upload-%d", &num1)
		_, err2 := fmt.Sscanf(id2, "upload-%d", &num2)

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.Greater(t, num2, num1)
	})
}
