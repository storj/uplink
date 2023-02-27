// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package segmentupload

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodedReader(t *testing.T) {
	rs := mustNewRedundancyStrategy()

	t.Run("produces valid pieces", func(t *testing.T) {
		expected := bytes.Repeat([]byte{1}, rs.StripeSize())

		pieces := map[int][]byte{}
		for i := 0; i < rs.TotalCount(); i++ {
			r := NewEncodedReader(bytes.NewReader(expected), rs, i)
			piece, err := io.ReadAll(r)
			require.NoError(t, err)
			pieces[i] = piece
		}

		actual, err := rs.Decode(nil, pieces)
		require.NoError(t, err)

		require.Equal(t, expected, actual)
	})

	t.Run("data not padded correctly", func(t *testing.T) {
		shortData := bytes.Repeat([]byte{1}, rs.StripeSize()-1)
		r := NewEncodedReader(bytes.NewReader(shortData), rs, 0)
		_, err := io.ReadAll(r)
		require.EqualError(t, err, "unexpected EOF")
		// reading again should produce the same error
		_, err = io.ReadAll(r)
		require.EqualError(t, err, "unexpected EOF")
	})

	t.Run("negative piece number", func(t *testing.T) {
		data := bytes.Repeat([]byte{1}, rs.StripeSize())
		r := NewEncodedReader(bytes.NewReader(data), rs, -1)
		_, err := io.ReadAll(r)
		require.EqualError(t, err, "num must be non-negative")
		// reading again should produce the same error
		_, err = io.ReadAll(r)
		require.EqualError(t, err, "num must be non-negative")
	})

	t.Run("out of range piece number", func(t *testing.T) {
		data := bytes.Repeat([]byte{1}, rs.StripeSize())
		r := NewEncodedReader(bytes.NewReader(data), rs, rs.TotalCount())
		_, err := io.ReadAll(r)
		require.EqualError(t, err, "num must be less than 4")
		// reading again should produce the same error
		_, err = io.ReadAll(r)
		require.EqualError(t, err, "num must be less than 4")
	})
}
