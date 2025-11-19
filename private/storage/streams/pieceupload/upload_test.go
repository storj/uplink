// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package pieceupload

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
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

			uploaded, _, _, _, err := UploadOne(longTailCtx, uploadCtx, manager, putter, fakePrivateKey, nil)
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

// Run all stall detection tests:
// RUN_LONG_TESTS=TRUE go test -run TestStallDetection

// Run specific test groups:
// RUN_LONG_TESTS=TRUE go test -run TestStallDetection_BasicRetry
// RUN_LONG_TESTS=TRUE go test -run TestStallDetection_SuccessCases
// RUN_LONG_TESTS=TRUE go test -run TestStallDetection_ExhaustionCases
// RUN_LONG_TESTS=TRUE go test -run TestStallDetection_EdgeCases
// RUN_LONG_TESTS=TRUE go test -run TestStallDetection_LongTailInteraction

// Run a specific test case:
// RUN_LONG_TESTS=TRUE go test -run 'TestStallDetection_BasicRetry/stall_timeout_triggers_retry'

// skipIfNotLongTests skips the test if RUN_LONG_TESTS is not set.
func skipIfNotLongTests(t *testing.T) {
	if os.Getenv("RUN_LONG_TESTS") != "TRUE" {
		t.Skip("Skipping long test - set RUN_LONG_TESTS=TRUE to enable")
	}
}

// stallTestType defines the type of stall test to run, which determines
// which PiecePutter implementation to use.
type stallTestType int

const (
	stallTestDefault                stallTestType = iota // Fast putter, no special behavior
	stallTestMultipleFailures                            // Fake putter with failures for retry test
	stallTestLongTailDelayedCancel                       // Slow putter for long tail delayed cancellation
	stallTestLongTailBeforeTimeout                       // Slow putter for long tail cancellation before timeout
	stallTestSingleStall                                 // Mixed putter - first stalls, second succeeds
	stallTestMultipleStalls                              // Triple stall putter - first two stall, third succeeds
	stallTestNodeIDVerification                          // Node tracking putter - verify different nodes used
	stallTestMixedFailuresAndStalls                      // Mixed failure stall putter - error, stall, success
)

// stallTestCase defines the parameters for a stall detection test.
type stallTestCase struct {
	testType              stallTestType
	failPuts              int
	cancelLongTail        bool
	cancelUpload          bool
	expectUploaded        bool
	expectNum             int
	expectErr             string
	expectStallCount      int
	stallManager          *StallManager
	stallTimeout          time.Duration
	delayedLongTailCancel time.Duration
}

// runStallTest executes a stall detection test case.
func runStallTest(t *testing.T, tc stallTestCase) {
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

	// Choose manager size based on test requirements
	managerSize := 2
	if tc.testType == stallTestMultipleStalls || tc.testType == stallTestMixedFailuresAndStalls {
		managerSize = 3 // Need 3 pieces for multiple stall test and mixed failure/stall test
	}
	manager := newManagerWithExchanger(managerSize, failExchange{})
	tc.stallManager.SetMaxDuration(tc.stallTimeout)
	var putter PiecePutter

	// Choose putter based on test requirements.
	var mixedPtr *mixedPutter
	var triplePtr *tripleStallPutter
	var nodeTrackingPtr *nodeTrackingPutter
	var mixedFailureStallPtr *mixedFailureStallPutter
	switch tc.testType {
	case stallTestMixedFailuresAndStalls:
		// Use mixed failure stall putter for combined error + stall retry test.
		mixedFailureStallPtr = &mixedFailureStallPutter{
			t:     t,
			delay: 100 * time.Millisecond, // Longer than stall timeout.
		}
		putter = mixedFailureStallPtr
	case stallTestNodeIDVerification:
		// Use node tracking putter to verify different node IDs are used.
		nodeTrackingPtr = &nodeTrackingPutter{
			t:     t,
			delay: 100 * time.Millisecond, // Longer than stall timeout.
		}
		putter = nodeTrackingPtr
	case stallTestMultipleStalls:
		// Use triple stall putter for multiple retry test.
		triplePtr = &tripleStallPutter{
			t:     t,
			delay: 100 * time.Millisecond, // Longer than stall timeout.
		}
		putter = triplePtr
	case stallTestSingleStall:
		// Use mixed putter for stall retry test.
		mixedPtr = &mixedPutter{
			t:     t,
			delay: 100 * time.Millisecond, // Longer than stall timeout.
		}
		putter = mixedPtr
	case stallTestMultipleFailures:
		// Use fake putter with failures for retry test.
		putter = &fakePutter{t: t, failPuts: tc.failPuts}
	case stallTestLongTailDelayedCancel, stallTestLongTailBeforeTimeout:
		// Use slow putter to ensure stall timeout can trigger.
		putter = &slowPutter{
			t:     t,
			delay: 100 * time.Millisecond, // Longer than stall timeout.
		}
	default:
		// Use slow putter if timeout is very short (for exhaustion tests), otherwise fast putter
		if tc.stallTimeout > 0 && tc.stallTimeout < 30*time.Millisecond {
			putter = &slowPutter{
				t:     t,
				delay: 100 * time.Millisecond, // Longer than stall timeout.
			}
		} else {
			// Use fast fake putter for most tests
			putter = &fakePutter{t: t, failPuts: tc.failPuts}
		}
	}

	uploaded, _, _, actualStallCount, err := UploadOne(longTailCtx, uploadCtx, manager, putter, fakePrivateKey, tc.stallManager)
	if tc.expectErr != "" {
		require.ErrorContains(t, err, tc.expectErr)
		return
	}
	require.NoError(t, err)
	require.Equal(t, tc.expectUploaded, uploaded)
	if tc.expectUploaded {
		assertResults(t, manager, revision{0}, makeResult(piecenum{tc.expectNum}, revision{0}))
	}
	// Verify stall count matches expectations
	if tc.expectStallCount > 0 {
		require.Equal(t, tc.expectStallCount, actualStallCount, "stallCount should match expected number of stalls")
	}
	// For the retry tests, verify the putter was called the expected number of times
	if mixedPtr != nil {
		require.Equal(t, 2, mixedPtr.callCount, "mixedPutter should have been called twice: once for piece 0 (stalled), once for piece 1 (succeeded)")
	}
	if triplePtr != nil {
		require.Equal(t, 3, triplePtr.callCount, "tripleStallPutter should have been called three times: piece 0 (stalled), piece 1 (stalled), piece 2 (succeeded)")
	}
	if nodeTrackingPtr != nil {
		require.Equal(t, 2, nodeTrackingPtr.callCount, "nodeTrackingPutter should have been called twice")
		require.Len(t, nodeTrackingPtr.nodeIDs, 2, "should have captured two node IDs")
		require.NotEqual(t, nodeTrackingPtr.nodeIDs[0], nodeTrackingPtr.nodeIDs[1], "node IDs should be different (different storage nodes)")
	}
	if mixedFailureStallPtr != nil {
		require.Equal(t, 3, mixedFailureStallPtr.callCount, "mixedFailureStallPutter should have been called three times: piece 0 (error), piece 1 (stalled), piece 2 (succeeded)")
	}
}

// TestStallDetection_BasicRetry tests the core retry functionality when stalls are detected.
// These tests verify that the retry logic works as intended per the commit message.
func TestStallDetection_BasicRetry(t *testing.T) {
	skipIfNotLongTests(t)

	t.Run("stall timeout triggers retry on different node", func(t *testing.T) {
		// Tests that when stall manager timeout (10ms) expires before first upload completes,
		// UploadOne retries with the next node. First node is slow (stalls), second node is fast (succeeds).
		// Uses mixedPutter: first call (piece 0) stalls, second call (piece 1) succeeds immediately.
		// Expected: uploaded=true, piece number 1 (proves retry happened on different node), stallCount=1
		runStallTest(t, stallTestCase{
			testType:         stallTestSingleStall,
			stallManager:     NewStallManager(),
			stallTimeout:     10 * time.Millisecond,
			expectUploaded:   true,
			expectNum:        1, // Should succeed with piece 1 after piece 0 stalls
			expectStallCount: 1, // One stall occurred
		})
	})

	t.Run("multiple stalls before success", func(t *testing.T) {
		// Tests that retries can happen multiple times before succeeding.
		// First two calls stall, third call succeeds.
		// Uses tripleStallPutter: first two calls stall (100ms), third succeeds.
		// Expected: uploaded=true, piece number 2 (third piece after two stalls), stallCount=2
		runStallTest(t, stallTestCase{
			testType:         stallTestMultipleStalls,
			stallManager:     NewStallManager(),
			stallTimeout:     10 * time.Millisecond,
			expectUploaded:   true,
			expectNum:        2, // Should succeed with piece 2 after pieces 0 and 1 stall
			expectStallCount: 2, // Two stalls occurred
		})
	})

	t.Run("stall retry uses different node IDs", func(t *testing.T) {
		// Tests that retries actually use different node IDs (different storage nodes).
		// First call (piece 0, node ID based on piece 0) stalls, second call (piece 1, different node ID) succeeds.
		// Uses nodeTrackingPutter to capture and verify node IDs are different.
		// Expected: uploaded=true, piece number 1, stallCount=1, two different node IDs captured
		runStallTest(t, stallTestCase{
			testType:         stallTestNodeIDVerification,
			stallManager:     NewStallManager(),
			stallTimeout:     10 * time.Millisecond,
			expectUploaded:   true,
			expectNum:        1,
			expectStallCount: 1,
		})
	})

	t.Run("mixed failures and stalls before success", func(t *testing.T) {
		// Tests that the system can handle a combination of upload errors and stall detections.
		// First call: returns an error (piece upload fails)
		// Second call: stalls (takes longer than timeout)
		// Third call: succeeds
		// Uses mixedFailureStallPutter: error, then stall, then success.
		// Expected: uploaded=true, piece number 2 (third piece after error and stall), stallCount=1
		runStallTest(t, stallTestCase{
			testType:         stallTestMixedFailuresAndStalls,
			stallManager:     NewStallManager(),
			stallTimeout:     10 * time.Millisecond,
			expectUploaded:   true,
			expectNum:        2, // Should succeed with piece 2 after piece 0 error and piece 1 stall
			expectStallCount: 1, // One stall occurred (piece 0 was an error, not a stall)
		})
	})
}

// TestStallDetection_SuccessCases tests scenarios where stall detection is present but doesn't trigger.
func TestStallDetection_SuccessCases(t *testing.T) {
	skipIfNotLongTests(t)

	t.Run("successful upload with stall manager", func(t *testing.T) {
		// Tests that stall manager doesn't interfere with fast, successful uploads
		// No timeout is set, so stall manager waits indefinitely (no stall detection)
		// Expected: uploaded=true, piece number 0 (first attempt)
		runStallTest(t, stallTestCase{
			stallManager:   NewStallManager(),
			expectUploaded: true,
			expectNum:      0,
		})
	})

	t.Run("stall manager with long timeout allows completion", func(t *testing.T) {
		// Tests that when stall timeout (100ms) is longer than upload time,
		// the upload completes successfully without stall detection triggering
		// Uses fast fakePutter that completes immediately
		// Expected: uploaded=true, piece number 0 (first attempt)
		runStallTest(t, stallTestCase{
			stallManager:   NewStallManager(),
			stallTimeout:   100 * time.Millisecond,
			expectUploaded: true,
			expectNum:      0,
		})
	})

	t.Run("multiple piece failures with stall manager", func(t *testing.T) {
		// Tests that stall manager doesn't interfere with retry logic
		// First piece fails, second succeeds, stall timeout is generous (100ms)
		// Verifies that stall detection and retry mechanism work together
		// Expected: uploaded=true, piece number 1 (second attempt)
		runStallTest(t, stallTestCase{
			testType:       stallTestMultipleFailures,
			failPuts:       1,
			stallManager:   NewStallManager(),
			stallTimeout:   100 * time.Millisecond,
			expectUploaded: true,
			expectNum:      1,
		})
	})
}

// TestStallDetection_ExhaustionCases tests scenarios where all nodes stall or timeout.
func TestStallDetection_ExhaustionCases(t *testing.T) {
	skipIfNotLongTests(t)

	t.Run("all nodes stall exhausts manager", func(t *testing.T) {
		// Tests that when all nodes stall (both pieces take longer than timeout),
		// the manager runs out of pieces to try and returns an exchange error.
		// Uses slowPutter with 100ms delay for all pieces, timeout is 10ms.
		// After both pieces stall and retry, manager has no more pieces to exchange.
		// Expected: error="piece limit exchange failed", uploaded=false
		runStallTest(t, stallTestCase{
			stallManager:   NewStallManager(),
			stallTimeout:   10 * time.Millisecond,
			expectUploaded: false,
			expectErr:      "piece limit exchange failed",
		})
	})

	t.Run("stall manager with very short timeout", func(t *testing.T) {
		// Tests that even very short timeouts (1ms) work correctly.
		// Uses slowPutter (100ms delay) to ensure all pieces stall.
		// After all pieces stall and are retried, manager runs out of pieces.
		// Expected: error="piece limit exchange failed", uploaded=false
		runStallTest(t, stallTestCase{
			stallManager:   NewStallManager(),
			stallTimeout:   1 * time.Millisecond,
			expectUploaded: false,
			expectErr:      "piece limit exchange failed",
		})
	})
}

// TestStallDetection_EdgeCases tests edge cases like zero timeout and context cancellation.
func TestStallDetection_EdgeCases(t *testing.T) {
	skipIfNotLongTests(t)

	t.Run("stall manager with zero timeout", func(t *testing.T) {
		// Tests that stallTimeout=0 means no timeout is set (stall manager waits indefinitely)
		// SetMaxDuration(0) is ignored, so no stall detection occurs
		// Expected: uploaded=true, piece number 0 (first attempt)
		runStallTest(t, stallTestCase{
			stallManager:   NewStallManager(),
			stallTimeout:   0, // Zero timeout should not set duration
			expectUploaded: true,
			expectNum:      0,
		})
	})

	t.Run("stall manager timeout vs upload context cancellation", func(t *testing.T) {
		// Tests that uploadCtx cancellation takes precedence over stall detection
		// Even with stall manager configured, uploadCtx.Err() is checked first
		// Expected: error="context canceled", uploaded=false
		runStallTest(t, stallTestCase{
			cancelUpload: true,
			stallManager: NewStallManager(),
			stallTimeout: 50 * time.Millisecond,
			expectErr:    "context canceled",
		})
	})
}

// TestStallDetection_LongTailInteraction tests the interaction between stall detection and long-tail cancellation.
func TestStallDetection_LongTailInteraction(t *testing.T) {
	skipIfNotLongTests(t)

	t.Run("long tail cancellation with stall manager", func(t *testing.T) {
		// Tests that when longTailCtx is cancelled (optimal threshold reached),
		// the result is OptimalThresholdError -> converted to nil by defer function
		// Stall manager context is also cancelled, but longTailCtx.Err() != nil takes precedence
		// Expected: error=nil (converted by defer), uploaded=false
		runStallTest(t, stallTestCase{
			cancelLongTail: true,
			stallManager:   NewStallManager(),
			expectUploaded: false,
		})
	})

	t.Run("stall timeout vs long tail cancellation race", func(t *testing.T) {
		// Tests immediate longTailCtx cancellation vs short stall timeout (5ms)
		// Both contexts get cancelled, but defer function converts any error to nil when longTailCtx.Err() != nil
		// This simulates the race between optimal threshold and stall detection
		// Expected: error=nil (converted by defer), uploaded=false
		runStallTest(t, stallTestCase{
			cancelLongTail: true,
			stallManager:   NewStallManager(),
			stallTimeout:   5 * time.Millisecond,
			expectUploaded: false,
		})
	})

	t.Run("long tail delayed cancellation with stall manager", func(t *testing.T) {
		// Tests scenario where stall timeout (10ms) triggers before delayed longTailCtx cancellation (200ms).
		// Uses slowPutter (100ms delay) to ensure upload takes longer than stall timeout.
		// Stall manager cancels first piece, retry happens, then longTailCtx cancels during retry.
		// The longTailCtx cancellation during the retry converts the error to nil.
		// Expected: error=nil (converted by defer when longTailCtx.Err() != nil), uploaded=false, stallCount=1
		runStallTest(t, stallTestCase{
			testType:              stallTestLongTailDelayedCancel,
			stallManager:          NewStallManager(),
			stallTimeout:          10 * time.Millisecond,
			delayedLongTailCancel: 200 * time.Millisecond, // Cancel way after stall timeout
			expectUploaded:        false,
			expectStallCount:      1, // One stall occurred before retry was cancelled by optimal threshold
		})
	})

	t.Run("long tail cancellation just before stall timeout", func(t *testing.T) {
		// Tests scenario where delayed longTailCtx cancellation (25ms) happens before stall timeout (50ms)
		// Uses slowPutter (100ms delay) to ensure upload takes longer than both delays
		// longTailCtx gets cancelled first -> OptimalThresholdError -> converted to nil by defer
		// Expected: error=nil (converted by defer), uploaded=false
		runStallTest(t, stallTestCase{
			testType:              stallTestLongTailBeforeTimeout,
			stallManager:          NewStallManager(),
			stallTimeout:          50 * time.Millisecond,
			delayedLongTailCancel: 25 * time.Millisecond, // Cancel before stall timeout
			expectUploaded:        false,
		})
	})
}

// HELPER FUNCTIONS

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

// mixedPutter simulates first call being slow (stalls), subsequent calls being fast
// This tests that stall detection causes a retry on a different node (different piece number)
type mixedPutter struct {
	t         *testing.T
	callCount int
	delay     time.Duration
	mu        sync.Mutex
}

func (p *mixedPutter) PutPiece(longTailCtx, uploadCtx context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (*pb.PieceHash, *struct{}, error) {
	assert.Equal(p.t, fakePrivateKey, privateKey, "private key was not passed correctly")

	num := pieceReaderNum(data)

	// First call is slow (stalls), subsequent calls are fast
	p.mu.Lock()
	isFirstCall := p.callCount == 0
	p.callCount++
	p.mu.Unlock()

	if isFirstCall {
		// Simulate slow upload that will trigger stall detection
		time.Sleep(p.delay)
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

// tripleStallPutter simulates first two calls being slow (stalls), third call being fast
// This tests that multiple retries can happen before success
type tripleStallPutter struct {
	t         *testing.T
	callCount int
	delay     time.Duration
	mu        sync.Mutex
}

func (p *tripleStallPutter) PutPiece(longTailCtx, uploadCtx context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (*pb.PieceHash, *struct{}, error) {
	assert.Equal(p.t, fakePrivateKey, privateKey, "private key was not passed correctly")

	num := pieceReaderNum(data)

	// First two calls are slow (stall), third call is fast
	p.mu.Lock()
	currentCall := p.callCount
	p.callCount++
	p.mu.Unlock()

	if currentCall < 2 {
		// Simulate slow upload that will trigger stall detection
		time.Sleep(p.delay)
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

// nodeTrackingPutter tracks node IDs to verify retries use different nodes
// First call stalls, second call succeeds. Captures node IDs from limits.
type nodeTrackingPutter struct {
	t         *testing.T
	callCount int
	delay     time.Duration
	nodeIDs   []storj.NodeID
	mu        sync.Mutex
}

func (p *nodeTrackingPutter) PutPiece(longTailCtx, uploadCtx context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (*pb.PieceHash, *struct{}, error) {
	assert.Equal(p.t, fakePrivateKey, privateKey, "private key was not passed correctly")

	num := pieceReaderNum(data)

	// Capture the node ID from the limit
	p.mu.Lock()
	currentCall := p.callCount
	p.callCount++
	if limit.Limit != nil {
		p.nodeIDs = append(p.nodeIDs, limit.Limit.StorageNodeId)
	}
	p.mu.Unlock()

	// First call is slow (stalls), subsequent calls are fast
	if currentCall == 0 {
		// Simulate slow upload that will trigger stall detection
		time.Sleep(p.delay)
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

// mixedFailureStallPutter simulates first call returning an error, second call stalling, third call succeeding
// This tests that the system can handle a combination of upload errors and stall detections
type mixedFailureStallPutter struct {
	t         *testing.T
	callCount int
	delay     time.Duration
	mu        sync.Mutex
}

func (p *mixedFailureStallPutter) PutPiece(longTailCtx, uploadCtx context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (*pb.PieceHash, *struct{}, error) {
	assert.Equal(p.t, fakePrivateKey, privateKey, "private key was not passed correctly")

	num := pieceReaderNum(data)

	p.mu.Lock()
	currentCall := p.callCount
	p.callCount++
	p.mu.Unlock()

	// First call: return an error
	if currentCall == 0 {
		return nil, nil, errs.New("simulated upload error")
	}

	// Second call: stall (slow upload that will trigger stall detection)
	if currentCall == 1 {
		time.Sleep(p.delay)
	}

	// Third call and beyond: succeed immediately

	select {
	case <-uploadCtx.Done():
		return nil, nil, uploadCtx.Err()
	case <-longTailCtx.Done():
		return nil, nil, longTailCtx.Err()
	default:
		return hash(num), nil, nil
	}
}
