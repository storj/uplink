/*
 *   Copyright (c) 2024 Storj Labs, Inc.
 *   All rights reserved.
 *   See LICENSE for copying information.
 */

package buffer

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"

	"storj.io/common/testrand"
)

func FuzzChunkBackend(f *testing.F) {
	f.Add(0)
	f.Add(1)
	f.Add(chunkSize - 1)
	f.Add(chunkSize)
	f.Add(chunkSize + 1)
	f.Add(chunkSize*2 - 1)
	f.Add(chunkSize * 2)
	f.Add(chunkSize*2 + 1)

	const (
		fuzzCap = chunkSize*2 + 1
	)

	data := testrand.BytesInt(fuzzCap)
	f.Fuzz(func(t *testing.T, amount int) {
		if amount < 0 || amount > len(data) {
			return
		}

		amount64 := int64(amount)

		// cap the data to the fuzz amount
		data := data[:amount]
		backend := NewChunkBackend(amount64)

		n, err := io.Copy(backend, bytes.NewReader(data))
		require.NoError(t, err)
		require.Equal(t, amount64, n)
		require.NoError(t, iotest.TestReader(io.NewSectionReader(backend, 0, amount64), data))
	})
}

func TestChunkBackend(t *testing.T) {
	requireWrite := func(tb testing.TB, w io.Writer, data []byte) {
		n, err := w.Write(data)
		require.NoError(t, err)
		require.Equal(t, n, len(data))
	}

	requireReaderAtHas := func(tb testing.TB, ra io.ReaderAt, pieces ...[]byte) {
		var data []byte
		for _, piece := range pieces {
			data = append(data, piece...)
		}
		require.NoError(t, iotest.TestReader(io.NewSectionReader(ra, 0, int64(len(data))), data))
	}

	double := testrand.BytesInt(chunkSize * 2)

	t.Run("writes spanning chunk boundaries", func(t *testing.T) {
		backend := NewChunkBackend(standardMaxEncryptedSegmentSize)
		half := double[:len(double)/4]
		whole := double[:len(double)/2]
		requireWrite(t, backend, half)
		requireWrite(t, backend, whole)
		requireWrite(t, backend, double)
		requireWrite(t, backend, half)
		requireWrite(t, backend, whole)
		requireWrite(t, backend, double)
		requireReaderAtHas(t, backend, half, whole, double, half, whole, double)
	})

	t.Run("read spanning chunk boundaries", func(t *testing.T) {
		backend := NewChunkBackend(standardMaxEncryptedSegmentSize)
		requireWrite(t, backend, double)
		buf := make([]byte, len(double))
		n, err := backend.ReadAt(buf, 0)
		require.NoError(t, err)
		require.Equal(t, n, len(buf))
		require.Equal(t, buf, double)
	})

	t.Run("exceed cap", func(t *testing.T) {
		backend := NewChunkBackend(1)
		_, err := backend.Write(double[:2])
		require.ErrorIs(t, err, io.ErrShortWrite)
	})
}

func TestBackendCloseConcurrency(t *testing.T) {
	// A failed or canceled upload closes the backend to interrupt an
	// in-flight Write (see Buffer.DoneWriting), so Close must be safe to
	// call concurrently with Write and ReadAt.
	backends := map[string]func() Backend{
		"memory": func() Backend { return NewMemoryBackend(chunkSize * 3) },
		"chunks": func() Backend { return NewChunkBackend(chunkSize * 3) },
	}

	for name, newBackend := range backends {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				backend := newBackend()

				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					data := make([]byte, 1024)
					for {
						if _, err := backend.Write(data); err != nil {
							return
						}
					}
				}()
				go func() {
					defer wg.Done()
					data := make([]byte, 1024)
					for {
						if _, err := backend.ReadAt(data, 0); errors.Is(err, io.ErrClosedPipe) {
							return
						}
					}
				}()

				require.NoError(t, backend.Close())
				wg.Wait()
			}
		})
	}
}

func TestChunksNeeded(t *testing.T) {
	require.Equal(t, int64(0), chunksNeeded(0))
	require.Equal(t, int64(1), chunksNeeded(1))
	require.Equal(t, int64(1), chunksNeeded(chunkSize-1))
	require.Equal(t, int64(1), chunksNeeded(chunkSize))
	require.Equal(t, int64(2), chunksNeeded(chunkSize+1))
	require.Equal(t, int64(2), chunksNeeded(chunkSize*2-1))
	require.Equal(t, int64(2), chunksNeeded(chunkSize*2))
	require.Equal(t, int64(3), chunksNeeded(chunkSize*2+1))
}

func BenchmarkBackends(b *testing.B) {
	buf := make([]byte, standardMaxEncryptedSegmentSize)
	_, err := rand.Read(buf)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	b.Run("memory", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			backend := NewMemoryBackend(standardMaxEncryptedSegmentSize)
			_, err := io.Copy(backend, bytes.NewReader(buf))
			require.NoError(b, err)
			require.NoError(b, backend.Close())
		}
	})

	b.Run("chunks", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			backend := NewChunkBackend(standardMaxEncryptedSegmentSize)
			_, err := io.Copy(backend, bytes.NewReader(buf))
			require.NoError(b, err)
			require.NoError(b, backend.Close())
		}
	})
}
