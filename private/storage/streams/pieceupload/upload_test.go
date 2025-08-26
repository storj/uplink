// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package pieceupload

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/pb"
	"storj.io/common/storj"
)

var (
	fakePrivateKey = mustNewPiecePrivateKey()
)

func TestUploadOne(t *testing.T) {
	for _, tc := range []struct {
		desc           string
		failPuts       int
		cancelLongTail bool
		cancelUpload   bool
		expectUploaded bool
		expectNum      int
		expectErr      string
	}{
		{
			// Basic success case: first piece upload succeeds immediately
			// Tests the happy path with no stall manager, no failures, no cancellations
			desc:           "first piece successful",
			expectUploaded: true,
			expectNum:      0,
		},
		{
			// Retry logic test: first piece fails, second succeeds
			// Tests that UploadOne properly retries after piece upload failures
			// failPuts=1 means fakePutter will fail the first call, succeed on second
			desc:           "second piece successful",
			failPuts:       1,
			expectUploaded: true,
			expectNum:      1,
		},
		{
			// Upload context cancellation test
			// Tests that when uploadCtx is cancelled, the function returns the context error
			// uploadCtx cancellation takes precedence over long tail cancellation
			desc:         "upload canceled",
			cancelUpload: true,
			expectErr:    "context canceled",
		},
		{
			// Long tail context cancellation test (no stall manager)
			// Tests that when longTailCtx is cancelled, the defer function converts error to nil
			// This simulates optimal threshold being reached in segment upload
			desc:           "long tail canceled",
			cancelLongTail: true,
			expectUploaded: false,
		},
		{
			// Manager failure test: piece manager exhausted
			// Tests that when the piece manager runs out of pieces (failPuts=2 exhausts the fake manager),
			// the error from manager.NextPiece() is returned directly
			desc:      "manager fails to return next piece",
			failPuts:  2,
			expectErr: "piece limit exchange failed: oh no",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			uploadCtx, uploadCancel := context.WithCancel(context.Background())
			t.Cleanup(uploadCancel)
			longTailCtx, longTailCancel := context.WithCancel(context.Background())
			t.Cleanup(longTailCancel)

			if tc.cancelUpload {
				uploadCancel()
			}
			if tc.cancelLongTail {
				longTailCancel()
			}

			manager := newManagerWithExchanger(2, failExchange{})
			putter := &fakePutter{t: t, failPuts: tc.failPuts}

			uploaded, _, _, err := UploadOne(longTailCtx, uploadCtx, manager, putter, fakePrivateKey, nil)
			if tc.expectErr != "" {
				require.ErrorContains(t, err, tc.expectErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectUploaded, uploaded)
			if tc.expectUploaded {
				assertResults(t, manager, revision{0}, makeResult(piecenum{tc.expectNum}, revision{0}))
			}
		})
	}
}

func TestUploadOneWithStall(t *testing.T) {
	// RUN_LONG_TESTS=TRUE go test -run TestUploadOneWithStall (make sure you're in the testsuite directory)
	if os.Getenv("RUN_LONG_TESTS") != "TRUE" {
		t.Skip("Skipping long test - set RUN_LONG_TESTS=TRUE to enable")
		return
	}
	for _, tc := range []struct {
		desc                  string
		testType              int
		failPuts              int
		cancelLongTail        bool
		cancelUpload          bool
		expectUploaded        bool
		expectNum             int
		expectErr             string
		stallManager          *StallManager
		stallTimeout          time.Duration
		delayedLongTailCancel time.Duration
	}{
		{
			// Stall manager basic success test
			// Tests that stall manager doesn't interfere with fast, successful uploads
			// No timeout is set, so stall manager waits indefinitely (no stall detection)
			// Expected: uploaded=true, piece number 0 (first attempt)
			desc:           "successful upload with stall manager",
			stallManager:   NewStallManager(),
			expectUploaded: true,
			expectNum:      0,
		},
		{
			// Stall detection timeout test
			// Tests that when stall manager timeout (10ms) expires before upload completes,
			// a StallDetectedError is returned. Uses slowPutter to ensure upload takes longer than timeout.
			// Expected: error="piece stall detected, context cancelled", uploaded=false
			desc:           "stall timeout triggers context cancellation",
			stallManager:   NewStallManager(),
			stallTimeout:   10 * time.Millisecond,
			expectUploaded: false,
			expectErr:      "piece stall detected, context cancelled",
		},
		{
			// Long tail cancellation with stall manager present
			// Tests that when longTailCtx is cancelled (optimal threshold reached),
			// the result is OptimalThresholdError -> converted to nil by defer function
			// Stall manager context is also cancelled, but longTailCtx.Err() != nil takes precedence
			// Expected: error=nil (converted by defer), uploaded=false
			desc:           "long tail cancellation with stall manager",
			cancelLongTail: true,
			stallManager:   NewStallManager(),
			expectUploaded: false,
		},
		{
			// Very aggressive stall detection test
			// Tests that even very short timeouts (1ms) work correctly
			// Uses slowPutter (50ms delay) to ensure stall timeout always triggers
			// Expected: error="stall detected", uploaded=false
			desc:           "stall manager with very short timeout",
			stallManager:   NewStallManager(),
			stallTimeout:   1 * time.Millisecond,
			expectUploaded: false,
			expectErr:      "piece stall detected, context cancelled",
		},
		{
			// Generous stall timeout test
			// Tests that when stall timeout (100ms) is longer than upload time,
			// the upload completes successfully without stall detection triggering
			// Uses fast fakePutter that completes immediately
			// Expected: uploaded=true, piece number 0 (first attempt)
			desc:           "stall manager with long timeout allows completion",
			stallManager:   NewStallManager(),
			stallTimeout:   100 * time.Millisecond,
			expectUploaded: true,
			expectNum:      0,
		},
		{
			// Context cancellation priority test
			// Tests that uploadCtx cancellation takes precedence over stall detection
			// Even with stall manager configured, uploadCtx.Err() is checked first
			// Expected: error="context canceled", uploaded=false
			desc:         "stall manager timeout vs upload context cancellation",
			cancelUpload: true,
			stallManager: NewStallManager(),
			stallTimeout: 50 * time.Millisecond,
			expectErr:    "context canceled",
		},
		{
			// Zero timeout edge case test
			// Tests that stallTimeout=0 means no timeout is set (stall manager waits indefinitely)
			// SetMaxDuration(0) is ignored, so no stall detection occurs
			// Expected: uploaded=true, piece number 0 (first attempt)
			desc:           "stall manager with zero timeout",
			stallManager:   NewStallManager(),
			stallTimeout:   0, // Zero timeout should not set duration
			expectUploaded: true,
			expectNum:      0,
		},
		{
			// Retry with stall manager test
			// Tests that stall manager doesn't interfere with retry logic
			// First piece fails, second succeeds, stall timeout is generous (100ms)
			// Verifies that stall detection and retry mechanism work together
			// Expected: uploaded=true, piece number 1 (second attempt)
			desc:           "multiple piece failures with stall manager",
			testType:       1,
			failPuts:       1,
			stallManager:   NewStallManager(),
			stallTimeout:   100 * time.Millisecond,
			expectUploaded: true,
			expectNum:      1,
		},
		{
			// Race condition test: stall vs long tail cancellation
			// Tests immediate longTailCtx cancellation vs short stall timeout (5ms)
			// Both contexts get cancelled, but defer function converts any error to nil when longTailCtx.Err() != nil
			// This simulates the race between optimal threshold and stall detection
			// Expected: error=nil (converted by defer), uploaded=false
			desc:           "stall timeout vs long tail cancellation race",
			cancelLongTail: true,
			stallManager:   NewStallManager(),
			stallTimeout:   5 * time.Millisecond,
			expectUploaded: false,
		},
		{
			// Delayed long tail cancellation test: stall wins
			// Tests scenario where stall timeout (10ms) triggers before delayed longTailCtx cancellation (200ms)
			// Uses slowPutter (100ms delay) to ensure upload takes longer than stall timeout
			// Stall manager context gets cancelled first, longTailCtx is still alive -> StallDetectedError
			// Expected: error="stall detected", uploaded=false
			desc:                  "long tail delayed cancellation with stall manager",
			testType:              2,
			stallManager:          NewStallManager(),
			stallTimeout:          10 * time.Millisecond,
			delayedLongTailCancel: 200 * time.Millisecond, // Cancel way after stall timeout
			expectUploaded:        false,
			expectErr:             "piece stall detected, context cancelled",
		},
		{
			// Delayed long tail cancellation test: long tail wins
			// Tests scenario where delayed longTailCtx cancellation (25ms) happens before stall timeout (50ms)
			// Uses slowPutter (100ms delay) to ensure upload takes longer than both delays
			// longTailCtx gets cancelled first -> OptimalThresholdError -> converted to nil by defer
			// Expected: error=nil (converted by defer), uploaded=false
			desc:                  "long tail cancellation just before stall timeout",
			testType:              3,
			stallManager:          NewStallManager(),
			stallTimeout:          50 * time.Millisecond,
			delayedLongTailCancel: 25 * time.Millisecond, // Cancel before stall timeout
			expectUploaded:        false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			uploadCtx, uploadCancel := context.WithCancel(context.Background())
			t.Cleanup(uploadCancel)
			longTailCtx, longTailCancel := context.WithCancel(context.Background())
			t.Cleanup(longTailCancel)

			if tc.cancelUpload {
				uploadCancel()
			}
			if tc.cancelLongTail {
				longTailCancel()
			}

			// Handle delayed long tail cancellation
			if tc.delayedLongTailCancel > 0 {
				go func() {
					time.Sleep(tc.delayedLongTailCancel)
					longTailCancel()
				}()
			}

			manager := newManagerWithExchanger(2, failExchange{})
			tc.stallManager.SetMaxDuration(tc.stallTimeout)
			var putter PiecePutter

			// Choose putter based on test requirements.
			switch {
			case tc.stallTimeout > 0 && tc.stallTimeout < 30*time.Millisecond:
				// Use slow putter for stall timeout tests (short timeouts).
				putter = &slowPutter{
					t:     t,
					delay: 100 * time.Millisecond, // Longer than stall timeout.
				}
			case tc.testType == 1: // multiple piece failures with stall manager.
				// Use fake putter with failures for retry test.
				putter = &fakePutter{t: t, failPuts: tc.failPuts}
			case tc.testType == 2: // long tail delayed cancellation with stall manager.
				// Use slow putter to ensure stall timeout can trigger.
				putter = &slowPutter{
					t:     t,
					delay: 100 * time.Millisecond, // Longer than stall timeout.
				}
			case tc.testType == 3: // long tail cancellation just before stall timeout.
				// Use slow putter to ensure upload takes longer than delay.
				putter = &slowPutter{
					t:     t,
					delay: 100 * time.Millisecond, // Longer than both delay and stall timeout.
				}
			default:
				// Use fast fake putter for most tests
				putter = &fakePutter{t: t, failPuts: tc.failPuts}
			}

			uploaded, _, _, err := UploadOne(longTailCtx, uploadCtx, manager, putter, fakePrivateKey, tc.stallManager)
			if tc.expectErr != "" {
				require.ErrorContains(t, err, tc.expectErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectUploaded, uploaded)
			if tc.expectUploaded {
				assertResults(t, manager, revision{0}, makeResult(piecenum{tc.expectNum}, revision{0}))
			}
		})
	}

}

func mustNewPiecePrivateKey() storj.PiecePrivateKey {
	pk, err := storj.PiecePrivateKeyFromBytes(bytes.Repeat([]byte{1}, 64))
	if err != nil {
		panic(err)
	}
	return pk
}

type fakePutter struct {
	t        *testing.T
	failPuts int
}

func (p *fakePutter) PutPiece(longTailCtx, uploadCtx context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (*pb.PieceHash, *struct{}, error) {
	assert.Equal(p.t, fakePrivateKey, privateKey, "private key was not passed correctly")

	num := pieceReaderNum(data)
	if p.failPuts > 0 {
		p.failPuts--
		return nil, nil, errs.New("put failed for piece: %d", num)
	}

	select {
	case <-uploadCtx.Done():
		return nil, nil, uploadCtx.Err()
	case <-longTailCtx.Done():
		return nil, nil, longTailCtx.Err()
	default:
		return hash(num), nil, nil
	}
}

// slowPutter simulates a slow upload for stall timeout testing
type slowPutter struct {
	t     *testing.T
	delay time.Duration
}

func (p *slowPutter) PutPiece(longTailCtx, uploadCtx context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (*pb.PieceHash, *struct{}, error) {
	assert.Equal(p.t, fakePrivateKey, privateKey, "private key was not passed correctly")

	// Simulate slow upload
	time.Sleep(p.delay)

	select {
	case <-uploadCtx.Done():
		return nil, nil, uploadCtx.Err()
	case <-longTailCtx.Done():
		return nil, nil, longTailCtx.Err()
	default:
		num := pieceReaderNum(data)
		return hash(num), nil, nil
	}
}
