// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package pieceupload

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/pb"
)

func TestManager(t *testing.T) {
	t.Run("results returned in piece order", func(t *testing.T) {
		manager := newManager(2)

		done0 := requireNextPiece(t, manager, piecenum{0}, revision{0})
		requireNextPieceAndFinish(t, manager, piecenum{1}, revision{0}, true)
		done0(true)

		requireDone(t, manager)

		assertResults(t, manager, revision{0},
			makeResult(piecenum{0}, revision{0}),
			makeResult(piecenum{1}, revision{0}),
		)
	})

	t.Run("piece retried on failure", func(t *testing.T) {
		manager := newManager(3)

		// 0(0) fails
		// 1(0) succeeds
		// 2(0) fails
		requireNextPieceAndFinish(t, manager, piecenum{0}, revision{0}, false)
		requireNextPieceAndFinish(t, manager, piecenum{1}, revision{0}, true)
		requireNextPieceAndFinish(t, manager, piecenum{2}, revision{0}, false)

		// Now that the all uploads have been attempted, we expect the next
		// call to exchange the upload limits and return a piece to retry.

		// Retries happen in first-failed order, so:
		// 2(1) is retried and succeeds
		// 0(1) is retried and fails
		requireNextPieceAndFinish(t, manager, piecenum{0}, revision{1}, true)
		requireNextPieceAndFinish(t, manager, piecenum{2}, revision{1}, false)

		// 0(2) is retried again (after another limit exchange) and succeeds
		requireNextPieceAndFinish(t, manager, piecenum{2}, revision{2}, true)

		requireDone(t, manager)

		assertResults(t, manager, revision{2},
			makeResult(piecenum{0}, revision{1}),
			makeResult(piecenum{1}, revision{0}),
			makeResult(piecenum{2}, revision{2}),
		)
	})

	t.Run("piece retry fails if exchange fails", func(t *testing.T) {
		manager := newManagerWithExchanger(1, failExchange{})
		requireNextPieceAndFinish(t, manager, piecenum{0}, revision{0}, false)

		_, _, _, err := manager.NextPiece(context.Background())
		require.EqualError(t, err, "piece limit exchange failed: oh no")
	})
}

func makeResult(num piecenum, rev revision) *pb.SegmentPieceUploadResult {
	return &pb.SegmentPieceUploadResult{PieceNum: int32(num.value), Hash: hash(num), NodeId: nodeID(num, rev)}
}

func assertResults(t *testing.T, manager *Manager, expectedRev revision, expectedResults ...*pb.SegmentPieceUploadResult) {
	t.Helper()
	actualID, actualResults := manager.Results()
	assert.Equal(t, makeSegmentID(expectedRev), actualID)
	assert.Equal(t, expectedResults, actualResults)
}

func requireNextPiece(t *testing.T, manager *Manager, expectedNum piecenum, expectedRev revision) func(bool) {
	reader, limit, done, err := manager.NextPiece(context.Background())
	require.NoError(t, err, "expected next piece")
	require.Equal(t, expectedNum, pieceReaderNumWriter(reader), "unexpected next piece number")
	require.Equal(t, makeLimit(expectedNum, expectedRev), limit, "unexpected next piece number limit")
	return func(uploaded bool) {
		if uploaded {
			done(hash(expectedNum), uploaded)
		} else {
			done(nil, false)
		}
	}
}

func requireDone(t *testing.T, manager *Manager) {
	reader, _, _, err := manager.NextPiece(context.Background())
	if err == nil {
		require.FailNowf(t, "expected done", "next piece %d", pieceReaderNumWriter(reader))
		return
	}
	require.True(t, errors.Is(err, ErrDone), "expected done but got %q", err)
}

func requireNextPieceAndFinish(t *testing.T, manager *Manager, expectedNum piecenum, expectedRev revision, uploaded bool) {
	requireNextPiece(t, manager, expectedNum, expectedRev)(uploaded)
}
