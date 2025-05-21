// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package buffer

import (
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	buf := New(NewMemoryBackend(25), 10)
	r1 := buf.Reader()
	r2 := buf.Reader()

	requireRead := func(r io.Reader, n int, err error) {
		t.Helper()
		m, gerr := r.Read(make([]byte, n))
		require.Equal(t, err, gerr)
		require.Equal(t, n, m)
	}

	requireWrite := func(w io.Writer, n int) {
		t.Helper()
		m, err := w.Write(make([]byte, n))
		require.NoError(t, err)
		require.Equal(t, n, m)
	}

	requireWrite(buf, 10)
	requireRead(r1, 5, nil)
	requireWrite(buf, 5)
	requireRead(r2, 15, nil)
	requireWrite(buf, 10)
	requireRead(r1, 20, nil)
	requireRead(r2, 10, nil)

	buf.DoneWriting(nil)

	requireRead(r1, 0, io.EOF)
	requireRead(r2, 0, io.EOF)

	buf.DoneReading(nil)
}

func TestBufferSimpleConcurrent(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		const amount = 10*1024 + 10
		testBufferSimpleConcurrent(t, NewMemoryBackend(amount), amount)
	})

	t.Run("chunked", func(t *testing.T) {
		const amount = chunkSize + 1
		testBufferSimpleConcurrent(t, NewChunkBackend(amount), amount)
	})
}

func testBufferSimpleConcurrent(t *testing.T, backend Backend, amount int64) {
	buf := New(backend, 2)
	defer buf.DoneReading(nil)

	r := buf.Reader()

	go func() {
		var tmp [1]byte
		for range amount {
			_, _ = buf.Write(tmp[:])
		}
		buf.DoneWriting(nil)
	}()

	var tmp [1]byte
	for {
		_, err := r.Read(tmp[:])
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
}

type eternalReader struct{}

func (eternalReader) Read(p []byte) (int, error) { return len(p), nil }

func TestWriteBufferConcurrent(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		const amount = 10*1024 + 10
		testWriteBufferConcurrent(t, NewMemoryBackend(amount), amount)
	})

	t.Run("chunked", func(t *testing.T) {
		const amount = 10*1024 + 10
		testWriteBufferConcurrent(t, NewChunkBackend(amount), amount)
	})
}

func testWriteBufferConcurrent(t *testing.T, backend Backend, amount int64) {
	type result struct {
		n   int64
		err error
	}
	wrap := func(n int64, err error) result { return result{n, err} }

	results := make(chan result)
	buf := New(backend, 1024)
	defer buf.DoneReading(nil)

	go func() {
		results <- wrap(io.CopyN(buf, eternalReader{}, amount))
		buf.DoneWriting(nil)
	}()

	go func() { results <- wrap(io.Copy(io.Discard, iotest.OneByteReader(buf.Reader()))) }()
	go func() { results <- wrap(io.Copy(io.Discard, iotest.HalfReader(buf.Reader()))) }()
	for range 10 {
		go func() { results <- wrap(io.Copy(io.Discard, buf.Reader())) }()
	}

	for range 13 {
		require.Equal(t, result{amount, nil}, <-results)
	}
}
