// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package buffer

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	buf := New(NewMemoryBackend(25), 10)
	r1 := buf.Reader()
	r2 := buf.Reader()

	requireRead := func(r Chunker, n int, err error) {
		t.Helper()
		m, gerr := r.Chunk(n)
		require.Equal(t, err, gerr)
		require.Equal(t, n, len(m))
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
	buf := New(NewMemoryBackend(2048), 2)
	r := buf.Reader()

	go func() {
		var tmp [1]byte
		for i := 0; i < 1024; i++ {
			_, _ = buf.Write(tmp[:])
		}
		buf.DoneWriting(nil)
	}()

	for {
		_, err := r.Chunk(1)
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
	const amount = 10*1024 + 10

	type result struct {
		n   int64
		err error
	}
	wrap := func(n int64, err error) result { return result{n, err} }

	results := make(chan result)
	buf := New(NewMemoryBackend(amount), 1024)
	defer buf.DoneReading(nil)

	go func() {
		results <- wrap(io.CopyN(buf, eternalReader{}, amount))
		buf.DoneWriting(nil)
	}()

	for i := 0; i < 10; i++ {
		go func() { results <- wrap(discard(buf.Reader())) }()
	}

	for i := 0; i < 11; i++ {
		require.Equal(t, result{amount, nil}, <-results)
	}
}

func discard(ch Chunker) (n int64, err error) {
	for {
		buf, err := ch.Chunk(10240)
		n += int64(len(buf))
		if err == io.EOF {
			return n, nil
		} else if err != nil {
			return n, err
		}
	}
}
