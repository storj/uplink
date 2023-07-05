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

	"storj.io/common/memory"
	"storj.io/uplink/private/storage/streams/buffer"
)

func discard(ch buffer.Chunker) (n int64, err error) {
	for {
		buf, err := ch.Chunk(4096)
		n += int64(len(buf))
		if err == io.EOF {
			return n, nil
		} else if err != nil {
			return n, err
		}
	}
}

func TestBaseSplitter(t *testing.T) {
	ctx := context.Background()

	const (
		split   = 20
		minimum = 10
	)

	canceled := errs.New("canceled")

	type result struct {
		kind   string
		amount int64
		err    error
	}

	readResult := func(splitter *baseSplitter) (res result, ok bool) {
		buf := buffer.New(buffer.NewMemoryBackend(splitter.split), 10)

		inline, eof, err := splitter.Next(ctx, buf)
		if err != nil {
			return result{"error", 0, err}, false
		} else if eof {
			return result{"done", 0, nil}, false
		}

		if inline != nil {
			return result{"inline", int64(len(inline)), nil}, true
		}

		amount, err := discard(buf.Reader())
		buf.DoneReading(nil)

		// we get a nondeterministic number of bytes read if there was an error so
		// zero it so that the tests never fail
		if err != nil {
			amount = 0
		}

		return result{"buffer", amount, err}, err == nil
	}

	type test struct {
		name    string
		write   int64
		finish  error
		results []result
	}

	var cases = []test{
		{"Basic", 45, nil, []result{
			{"buffer", 20, nil},
			{"buffer", 20, nil},
			{"inline", 5, nil},
			{"done", 0, nil},
		}},

		{"Aligned", 40, nil, []result{
			{"buffer", 20, nil},
			{"buffer", 20, nil},
			{"done", 0, nil},
		}},

		{"Inline", 5, nil, []result{
			{"inline", 5, nil},
			{"done", 0, nil},
		}},

		{"Inline_Aligned", 10, nil, []result{
			{"inline", 10, nil},
			{"done", 0, nil},
		}},

		{"Zero", 0, nil, []result{
			{"inline", 0, nil},
			{"done", 0, nil},
		}},

		{"Error_Inline", 45, canceled, []result{
			{"buffer", 20, nil},
			{"buffer", 20, nil},
			{"error", 0, canceled},
		}},

		{"Error_Buffer", 55, canceled, []result{
			{"buffer", 20, nil},
			{"buffer", 20, nil},
			{"buffer", 0, canceled},
		}},

		{"Error_Aligned", 30, canceled, []result{
			{"buffer", 20, nil},
			{"error", 0, canceled},
		}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			splitter := newBaseSplitter(split, minimum)
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

func BenchmarkBaseSplitter(b *testing.B) {
	ctx := context.Background()

	const (
		minimum = 4 << 10
		split   = 64 << 20
	)

	run := func(b *testing.B, size int) {
		b.SetBytes(int64(size))
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			splitter := newBaseSplitter(split, minimum)

			go func() {
				_, _ = io.Copy(splitter, &emptyLimitReader{size})
				splitter.Finish(nil)
			}()

			for {
				buf := buffer.New(buffer.NewMemoryBackend(minimum), minimum)
				inline, eof, err := splitter.Next(ctx, buf)
				require.NoError(b, err)
				if eof {
					buf.DoneReading(nil)
					buf.DoneWriting(nil)
					break
				}
				if inline == nil {
					_, _ = discard(buf.Reader())
					buf.DoneReading(nil)
				}
			}
		}
	}

	sizes := []memory.Size{
		1 * memory.KiB,
		4 * memory.KiB,
		256 * memory.KiB,
		1 * memory.MiB,
		4 * memory.MiB,
		16 * memory.MiB,
		64 * memory.MiB,
		256 * memory.MiB,
		512 * memory.MiB,
		1 * memory.GiB,
	}

	for _, size := range sizes {
		b.Run(size.String(), func(b *testing.B) {
			run(b, size.Int())
		})
	}
}
