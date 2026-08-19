// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package eestream_test

import (
	"bytes"
	"context"
	"io"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/testrand"
	"storj.io/infectious"
	"storj.io/uplink/private/eestream"
)

// TestRebuilderMatchesScheme checks that the planned rebuild produces exactly
// what the unplanned one does, for share sets that need no reconstruction, a
// few reconstructed shares, and only parity shares.
func TestRebuilderMatchesScheme(t *testing.T) {
	const (
		required = 4
		total    = 10
		shareLen = 256
	)

	fc, err := infectious.NewFEC(required, total)
	require.NoError(t, err)
	es := eestream.NewRSScheme(fc, shareLen)

	planner, ok := es.(eestream.RebuilderScheme)
	require.True(t, ok, "rsScheme must implement RebuilderScheme")

	stripe := testrand.Bytes(required * shareLen)
	all := make([]infectious.Share, 0, total)
	require.NoError(t, es.Encode(stripe, func(num int, data []byte) {
		all = append(all, infectious.Share{Number: num, Data: append([]byte(nil), data...)})
	}))

	for _, tt := range []struct {
		name string
		nums []int
	}{
		{"all data shares", []int{0, 1, 2, 3}},
		{"one parity share", []int{0, 1, 2, 7}},
		{"only parity shares", []int{4, 6, 8, 9}},
		{"more than required", []int{0, 2, 3, 5, 9}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			shares := make([]infectious.Share, 0, len(tt.nums))
			for _, num := range tt.nums {
				shares = append(shares, all[num])
			}

			want := make([]byte, required*shareLen)
			require.NoError(t, es.Rebuild(shares, func(r infectious.Share) {
				copy(want[r.Number*len(r.Data):], r.Data)
			}))
			require.Equal(t, stripe, want, "unplanned rebuild must return the original stripe")

			rebuilder, err := planner.NewRebuilder(tt.nums)
			require.NoError(t, err)

			// the same rebuilder is reused across stripes, so run it twice.
			for range 2 {
				got := make([]byte, required*shareLen)
				require.NoError(t, rebuilder.Rebuild(shares, func(r infectious.Share) {
					copy(got[r.Number*len(r.Data):], r.Data)
				}))
				require.Equal(t, want, got)
			}
		})
	}
}

func TestRebuilderRejectsBadShareNumbers(t *testing.T) {
	fc, err := infectious.NewFEC(4, 10)
	require.NoError(t, err)
	planner, ok := eestream.NewRSScheme(fc, 256).(eestream.RebuilderScheme)
	require.True(t, ok)

	_, err = planner.NewRebuilder([]int{0, 1})
	assert.Error(t, err, "fewer shares than required")

	_, err = planner.NewRebuilder([]int{0, 1, 2, 10})
	assert.Error(t, err, "share number out of range")

	_, err = planner.NewRebuilder([]int{0, 1, 2, 2})
	assert.Error(t, err, "duplicate share number")
}

// TestDecodeWithMissingShares decodes a stream where the available shares
// force reconstruction on every stripe, which is the case the planned rebuild
// exists for.
func TestDecodeWithMissingShares(t *testing.T) {
	ctx := context.Background()

	const (
		required = 4
		total    = 10
		blockLen = 1024
		dataLen  = 64 * 1024
	)

	data := testrand.Bytes(dataLen)

	fc, err := infectious.NewFEC(required, total)
	require.NoError(t, err)
	rs, err := eestream.NewRedundancyStrategy(eestream.NewRSScheme(fc, blockLen), 0, 0)
	require.NoError(t, err)

	for _, tt := range []struct {
		name  string
		avail []int
	}{
		{"data shares only", []int{0, 1, 2, 3}},
		{"parity shares only", []int{5, 6, 8, 9}},
		{"mixed, one extra", []int{1, 3, 4, 7, 9}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			readers, err := eestream.EncodeReader2(ctx, bytes.NewReader(data), rs)
			require.NoError(t, err)

			readerMap := make(map[int]io.ReadCloser, len(tt.avail))
			for i, reader := range readers {
				// unused readers still have to be drained, otherwise the
				// encoder blocks on them.
				if !slices.Contains(tt.avail, i) {
					go func() { _, _ = io.Copy(io.Discard, reader) }()
					continue
				}
				readerMap[i] = reader
			}

			ctx, cancel := context.WithCancel(ctx)
			decoder := eestream.DecodeReaders2(ctx, cancel, readerMap, rs, dataLen, 0, false)
			defer func() { assert.NoError(t, decoder.Close()) }()

			got, err := io.ReadAll(decoder)
			require.NoError(t, err)
			assert.Equal(t, data, got)
		})
	}
}

// BenchmarkRebuildStripes rebuilds many stripes from one set of shares, the
// way a segment download does, with and without planning the rebuild.
func BenchmarkRebuildStripes(b *testing.B) {
	const (
		required = 29
		total    = 80
		shareLen = 256
	)

	fc, err := infectious.NewFEC(required, total)
	require.NoError(b, err)
	es := eestream.NewRSScheme(fc, shareLen)
	planner := es.(eestream.RebuilderScheme)

	stripe := testrand.Bytes(required * shareLen)
	all := make([]infectious.Share, 0, total)
	require.NoError(b, es.Encode(stripe, func(num int, data []byte) {
		all = append(all, infectious.Share{Number: num, Data: append([]byte(nil), data...)})
	}))

	// a realistic download: the pieces come from arbitrary nodes, so most of
	// the data shares have to be reconstructed from parity.
	nums := make([]int, 0, required)
	shares := make([]infectious.Share, 0, required)
	for i := total - required; i < total; i++ {
		nums = append(nums, i)
		shares = append(shares, all[i])
	}

	out := make([]byte, required*shareLen)
	write := func(r infectious.Share) {
		copy(out[r.Number*len(r.Data):], r.Data)
	}

	b.Run("unplanned", func(b *testing.B) {
		b.SetBytes(required * shareLen)
		b.ReportAllocs()
		for range b.N {
			if err := es.Rebuild(shares, write); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("planned", func(b *testing.B) {
		rebuilder, err := planner.NewRebuilder(nums)
		require.NoError(b, err)

		b.SetBytes(required * shareLen)
		b.ReportAllocs()
		for range b.N {
			if err := rebuilder.Rebuild(shares, write); err != nil {
				b.Fatal(err)
			}
		}
	})
}
