// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package eestream

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/infectious"
)

// TestReadStripesReleaseLeakOnError guards the fix for a refcount leak in
// ReadStripes: when ReadShare returns an error mid-loop after earlier pieces
// have already successfully Claim()ed their Batches, the earlier Claims'
// Release callbacks must still run. Previously those releases were discarded
// on the early-return path, leaving refCount > 0 forever.
//
// We provoke the error deterministically by sabotaging one piece's
// StreamingPiece.batches slot (swapping it to freedBatch) after data has
// been loaded but before ReadStripes runs its inner loop. This simulates a
// concurrent markCompleted racing the inner loop (which can happen because
// decodedReader.Close runs stripeReader.Close in a goroutine while
// ReadStripes may still be in flight).
func TestReadStripesReleaseLeakOnError(t *testing.T) {
	const shareSize = 1024
	const stripeSize = 2 * shareSize // required=2
	const totalStripes = 4
	const totalBytes = totalStripes * stripeSize

	fc, err := infectious.NewFEC(2, 4)
	require.NoError(t, err)
	es := NewRSScheme(fc, shareSize)

	// Generate deterministic input data.
	data := bytes.Repeat([]byte("A"), totalBytes)

	// Encode it up front by calling Encode directly stripe-by-stripe into per-piece buffers.
	perPiece := make(map[int]*bytes.Buffer, es.TotalCount())
	for i := 0; i < es.TotalCount(); i++ {
		perPiece[i] = &bytes.Buffer{}
	}
	for s := 0; s < totalStripes; s++ {
		stripe := data[s*stripeSize : (s+1)*stripeSize]
		err := es.Encode(stripe, func(num int, out []byte) {
			perPiece[num].Write(out)
		})
		require.NoError(t, err)
	}

	readers := make(map[int]io.ReadCloser, es.TotalCount())
	for i, buf := range perPiece {
		readers[i] = io.NopCloser(bytes.NewReader(buf.Bytes()))
	}

	sr := NewStripeReader(readers, es, totalStripes, false)

	// Wait until all pieces have received all stripes' shares (watermarks >= totalStripes).
	deadline := time.Now().Add(2 * time.Second)
	for {
		allReady := true
		for i := range sr.pieces {
			if sr.bundy.PieceSharesReceived(i) < int32(totalStripes) {
				allReady = false
				break
			}
		}
		if allReady {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pieces never filled")
		}
		time.Sleep(time.Millisecond)
	}

	// Sabotage the LAST piece (by slice index): replace its batches[0] with freedBatch
	// but do NOT Release the old batch through MarkCompleted logic. Instead, we will
	// manually keep the original batch alive so we can inspect refcount. The scenario
	// we want to simulate is "concurrent MarkCompleted swapped in freedBatch"; in that
	// real scenario MarkCompleted would also Release the old batch once, dropping
	// refCount by one.
	sabotageIdx := len(sr.pieces) - 1
	sabotagedBuffer := sr.pieces[sabotageIdx].buffer
	origSabotageBatch := sabotagedBuffer.batches[0].Load()
	require.NotNil(t, origSabotageBatch)
	require.True(t, origSabotageBatch != freedBatch)

	// Capture references to pre-existing batches on the OTHER pieces for leak inspection.
	origBatches := make(map[int]*Batch)
	for i := range sr.pieces {
		if i == sabotageIdx {
			continue
		}
		b := sr.pieces[i].buffer.batches[0].Load()
		require.NotNil(t, b, "piece %d batch[0] not hydrated", i)
		require.True(t, b != freedBatch)
		origBatches[i] = b
	}

	// Swap in freedBatch on the sabotaged piece. This mimics what MarkCompleted does
	// when it races ahead of the inner loop. Simulate MarkCompleted's Release call.
	sabotagedBuffer.batches[0].Store(freedBatch)
	origSabotageBatch.Release() // drop refCount 1 -> 0 (returns to pool)

	// Now call ReadStripes. We expect it to error out mid-loop because one of the
	// ready pieces will return "read completed buffer" from ReadShare.
	out := make([]byte, 0, stripeSize*totalStripes)
	_, _, err = sr.ReadStripes(context.Background(), 0, out)
	require.Error(t, err)
	t.Logf("got expected error: %v", err)

	// After the fix, pieces that successfully claimed their batch inside ReadStripes
	// have their Release callbacks run even on the early-return error path, so
	// batches[0].refCount should drop back to 1 (only the initial pool GetAndClaim).
	// Before the fix, refCount would be 2 because the mid-loop Claim inside ReadShare
	// was never released.
	for i, b := range origBatches {
		rc := b.refCount.Load()
		t.Logf("piece %d batch[0] refCount after errored ReadStripes: %d", i, rc)
		require.Equalf(t, int32(1), rc,
			"piece %d: releases collected before the mid-loop error must be drained", i)
	}

	require.NoError(t, sr.Close())
}
