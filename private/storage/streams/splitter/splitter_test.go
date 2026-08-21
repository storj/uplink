// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package splitter

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/storj"
)

func TestSplitter(t *testing.T) {
	ctx := context.Background()

	opts := Options{
		Split:   20,
		Minimum: 10,
		Params: storj.EncryptionParameters{
			CipherSuite: storj.EncAESGCM,
			BlockSize:   21, // 16 bytes of padding + 5 bytes of plaintext
		},
		Key:        new(storj.Key),
		PartNumber: 1,
	}

	canceled := errs.New("canceled")

	type result struct {
		kind     string
		plain    int64
		enc      int64
		err      error
		position int
	}

	type test struct {
		name    string
		write   int64
		finish  error
		results []result
	}

	readResult := func(splitter *Splitter) (res result, ok bool) {
		seg, err := splitter.Next(ctx)
		if err != nil {
			return result{"error", 0, 0, err, 0}, false
		} else if seg == nil {
			return result{"done", 0, 0, nil, 0}, false
		}

		_ = seg.Begin() // ensure this can even be called

		n, err := io.Copy(io.Discard, seg.Reader())
		seg.DoneReading(nil)

		if err != nil {
			return result{"seg_error", 0, 0, err, 0}, false
		}

		data, err := seg.EncryptETag([]byte("some etag")) // ensure this can even be called
		require.NoError(t, err)
		require.NotNil(t, data)

		info := seg.Finalize()
		require.Equal(t, n, info.EncryptedSize)

		return result{
			kind:     map[bool]string{true: "inline", false: "remote"}[seg.Inline()],
			plain:    info.PlainSize,
			enc:      info.EncryptedSize,
			err:      err,
			position: int(seg.Position().Index),
		}, true
	}

	cases := []test{
		{"Basic", 45, nil, []result{
			{"remote", 20, 21 * (4 + 1), nil, 0},
			{"remote", 20, 21 * (4 + 1), nil, 1},
			{"inline", 5, 5 + 16, nil, 2},
			{"done", 0, 0, nil, 0},
		}},

		{"Aligned", 40, nil, []result{
			{"remote", 20, 21 * (4 + 1), nil, 0},
			{"remote", 20, 21 * (4 + 1), nil, 1},
			{"done", 0, 0, nil, 0},
		}},

		{"Inline", 5, nil, []result{
			{"inline", 5, 5 + 16, nil, 0},
			{"done", 0, 0, nil, 0},
		}},

		{"Inline_Aligned", 10, nil, []result{
			{"inline", 10, 10 + 16, nil, 0},
			{"done", 0, 0, nil, 0},
		}},

		{"Zero", 0, nil, []result{
			{"inline", 0, 0, nil, 0},
			{"done", 0, 0, nil, 0},
		}},

		{"Error_Inline", 45, canceled, []result{
			{"remote", 20, 21 * (4 + 1), nil, 0},
			{"remote", 20, 21 * (4 + 1), nil, 1},
			{"error", 0, 0, canceled, 0},
		}},

		{"Error_Buffer", 55, canceled, []result{
			{"remote", 20, 21 * (4 + 1), nil, 0},
			{"remote", 20, 21 * (4 + 1), nil, 1},
			{"seg_error", 0, 0, canceled, 0},
		}},

		{"Error_Aligned", 30, canceled, []result{
			{"remote", 20, 21 * (4 + 1), nil, 0},
			{"error", 0, 0, canceled, 0},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			splitter, err := New(opts)
			require.NoError(t, err)

			go func() {
				n, err := io.CopyN(randomWriter{splitter}, emptyReader{}, tc.write)
				splitter.Finish(tc.finish)
				if n != tc.write || err != nil {
					panic(fmt.Sprintln("not enough bytes written or error:", n, tc.write, err))
				}
			}()

			var results []result
			for {
				res, ok := readResult(splitter)
				results = append(results, res)
				if !ok {
					break
				}
			}
			require.Equal(t, tc.results, results)
		})
	}
}

func TestSplitterWriteAhead(t *testing.T) {
	newOpts := func() Options {
		return Options{
			Split:   20,
			Minimum: 10,
			Params: storj.EncryptionParameters{
				CipherSuite: storj.EncAESGCM,
				BlockSize:   21,
			},
			Key: new(storj.Key),
		}
	}

	t.Run("defaults when unset", func(t *testing.T) {
		s, err := New(newOpts())
		require.NoError(t, err)
		require.EqualValues(t, defaultWriteAhead, s.opts.WriteAhead)
	})

	t.Run("honors an explicit value", func(t *testing.T) {
		opts := newOpts()
		opts.WriteAhead = 8 << 20

		s, err := New(opts)
		require.NoError(t, err)
		require.EqualValues(t, 8<<20, s.opts.WriteAhead)
	})

	// Minimum decides whether a segment is stored inline and has nothing to do
	// with the write ahead window. Tying the two together starves the erasure
	// piece readers, which consume a stripe at a time, so the default has to
	// stay well clear of any plausible stripe size.
	t.Run("is independent of Minimum", func(t *testing.T) {
		opts := newOpts()
		opts.WriteAhead = 8 << 20

		s, err := New(opts)
		require.NoError(t, err)
		require.EqualValues(t, 8<<20, s.opts.WriteAhead)
		require.EqualValues(t, 10, s.opts.Minimum)

		const maxPlausibleStripeSize = 64 << 10
		require.Greater(t, int64(defaultWriteAhead), int64(maxPlausibleStripeSize))
	})
}
