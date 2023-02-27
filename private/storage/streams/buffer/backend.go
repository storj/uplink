// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package buffer

import (
	"io"
	"sync/atomic"
)

// Backend is a backing store of bytes for a Backend.
type Backend interface {
	io.Writer
	io.ReaderAt
	io.Closer
}

// NewMemoryBackend returns a MemoryBackend with the provided initial
// capacity. It implements the Backend interface.
func NewMemoryBackend(cap int) *MemoryBackend {
	buf := make([]byte, 0, cap)
	mbuf := new(MemoryBackend)
	mbuf.buf.Store(buf)
	return mbuf
}

// MemoryBackend implements the Backend interface backed by a slice.
type MemoryBackend struct {
	buf atomic.Value // []byte
}

// Reset clears the length and allows the capacity to be reused.
func (u *MemoryBackend) Reset() {
	buf, _ := u.buf.Load().([]byte)
	u.buf.Store(buf[:0])
}

// Write appends the data to the buffer.
func (u *MemoryBackend) Write(p []byte) (n int, err error) {
	buf, _ := u.buf.Load().([]byte)
	buf = append(buf, p...)
	u.buf.Store(buf)
	return len(p), nil
}

// ReadAt reads into the provided buffer p starting at off.
func (u *MemoryBackend) ReadAt(p []byte, off int64) (n int, err error) {
	buf, _ := u.buf.Load().([]byte)
	if off >= int64(len(buf)) {
		return 0, io.EOF
	}
	n = copy(p, buf[off:])
	return n, nil
}

// Close is a no-op for a memory backend.
func (u *MemoryBackend) Close() error {
	return nil
}
