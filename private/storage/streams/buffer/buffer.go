// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package buffer

import (
	"io"
	"sync"
)

// Buffer lets one write to some Backend and lazily create readers from it.
type Buffer struct {
	mu      sync.Mutex
	cursor  *Cursor
	backend Backend
	wrote   int64
}

// New constructs a Buffer using the underlying Backend and allowing
// the writer to write writeAhead extra bytes in front of the most advanced reader.
func New(backend Backend, writeAhead int64) *Buffer {
	return &Buffer{
		cursor:  NewCursor(writeAhead),
		backend: backend,
	}
}

// DoneReading signals to the Buffer that no more Read calls are coming and
// that Write calls should return the provided error. The first call that
// causes both DoneReading and DoneWriting to have been called closes the
// underlying backend.
func (w *Buffer) DoneReading(err error) {
	if w.cursor.DoneReading(err) {
		_ = w.backend.Close()
	}
}

// DoneWriting signals to the Buffer that no more Write calls are coming and
// that Read calls should return the provided error. The first call that
// causes both DoneReading and DoneWriting to have been called closes the
// underlying backend.
func (w *Buffer) DoneWriting(err error) {
	if w.cursor.DoneWriting(err) {
		_ = w.backend.Close()
	}
}

// Write appends the bytes in p to the Buffer. It blocks until the furthest
// advanced reader is less than the write ahead amount.
func (w *Buffer) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for len(p) > 0 {
		m, ok, err := w.cursor.WaitWrite(w.wrote + int64(len(p)))
		if err != nil {
			return n, err
		} else if !ok {
			return n, nil
		}

		var nn int
		nn, err = w.backend.Write(p[:m-w.wrote])
		n += nn
		p = p[nn:]
		w.wrote += int64(nn)
		w.cursor.WroteTo(w.wrote)

		if err != nil {
			w.cursor.DoneWriting(err)
			return n, err
		}
	}
	return n, nil
}

// bufferReader is the concrete implementation returned from the Buffer.Reader method.
type bufferReader struct {
	mu     sync.Mutex
	cursor *Cursor
	buffer Backend
	read   int64
}

// Reader returns a fresh io.Reader that can be used to read all of the previously
// and future written bytes to the Buffer.
func (w *Buffer) Reader() Chunker {
	return &bufferReader{
		cursor: w.cursor,
		buffer: w.backend,
	}
}

type Chunker interface {
	Chunk(n int) ([]byte, error)
}

func (br *bufferReader) Chunk(n int) ([]byte, error) {
	br.mu.Lock()
	defer br.mu.Unlock()

	m, ok, err := br.cursor.WaitRead(br.read + int64(n))
	if err != nil {
		return nil, err
	} else if m == br.read {
		return nil, io.EOF
	}

	buf, err := br.buffer.ChunkAt(n, br.read)
	br.read += int64(len(buf))
	br.cursor.ReadTo(br.read)

	if err != nil {
		return buf, err
	} else if !ok {
		return buf, io.EOF
	}
	return buf, nil
}
