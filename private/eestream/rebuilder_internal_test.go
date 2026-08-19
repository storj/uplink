// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package eestream

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/infectious"
)

// TestRebuilderFor checks that stripes are rebuilt through a planned
// rebuilder, that the plan is reused while the ready set is unchanged, and
// that it is replanned when the ready set changes.
func TestRebuilderFor(t *testing.T) {
	const (
		required = 4
		total    = 10
		shareLen = 256
	)

	fc, err := infectious.NewFEC(required, total)
	require.NoError(t, err)

	// the download path hands the StripeReader a RedundancyStrategy, which
	// embeds the scheme rather than being one, so test through it.
	scheme, err := NewRedundancyStrategy(NewRSScheme(fc, shareLen), 0, 0)
	require.NoError(t, err)

	readers := map[int]io.ReadCloser{}
	for i := range total {
		readers[i] = io.NopCloser(io.LimitReader(nullReader{}, shareLen))
	}
	s := NewStripeReader(readers, scheme, 1, false)
	defer func() { _ = s.Close() }()

	// pieces are indexed in map iteration order, so find the indexes of the
	// share numbers this test wants rather than assuming they line up.
	indexOf := map[int]int{}
	for idx := range s.pieces {
		indexOf[s.pieces[idx].shareNum] = idx
	}
	ready := func(shareNums ...int) []int {
		idxs := make([]int, 0, len(shareNums))
		for _, num := range shareNums {
			idxs = append(idxs, indexOf[num])
		}
		return idxs
	}

	first := s.rebuilderFor(ready(0, 1, 2, 3))
	require.NotEqual(t, Rebuilder(s.scheme), first, "scheme must support planned rebuilds")

	assert.Same(t, first, s.rebuilderFor(ready(0, 1, 2, 3)), "unchanged ready set must reuse the plan")

	second := s.rebuilderFor(ready(0, 1, 2, 9))
	require.NotNil(t, second)
	assert.NotSame(t, first, second, "changed ready set must be replanned")

	// too few shares cannot be planned, so the scheme itself is the fallback.
	assert.Equal(t, Rebuilder(s.scheme), s.rebuilderFor(ready(0, 1)))
	assert.Nil(t, s.rebuilder, "a failed plan must not be cached")
}

// TestRebuilderForUnplannableScheme checks the fallback for schemes that
// cannot plan rebuilds.
func TestRebuilderForUnplannableScheme(t *testing.T) {
	fc, err := infectious.NewFEC(2, 4)
	require.NoError(t, err)

	s := NewStripeReader(map[int]io.ReadCloser{
		0: io.NopCloser(io.LimitReader(nullReader{}, 256)),
		1: io.NopCloser(io.LimitReader(nullReader{}, 256)),
	}, unplannableScheme{NewRSScheme(fc, 256)}, 1, false)
	defer func() { _ = s.Close() }()

	assert.Equal(t, Rebuilder(s.scheme), s.rebuilderFor([]int{0, 1}))
}

// unplannableScheme hides the underlying scheme's RebuilderScheme
// implementation.
type unplannableScheme struct{ ErasureScheme }

type nullReader struct{}

func (nullReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
