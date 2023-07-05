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
	io.Closer
	ChunkAt(n int, off int64) ([]byte, error)
}

// NewMemoryBackend returns a MemoryBackend with the provided initial
// capacity. It implements the Backend interface.
func NewMemoryBackend(cap int64) *MemoryBackend {
	return &MemoryBackend{
		buf: make([]byte, cap),
	}
}

// MemoryBackend implements the Backend interface backed by a slice.
type MemoryBackend struct {
	buf    []byte
	len    int64
	closed bool
}

// Write appends the data to the buffer.
func (u *MemoryBackend) Write(p []byte) (n int, err error) {
	if u.closed {
		return 0, io.ErrClosedPipe
	}
	l := atomic.LoadInt64(&u.len)
	n = copy(u.buf[l:], p)
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	atomic.AddInt64(&u.len, int64(n))
	return n, nil
}

func (u *MemoryBackend) ChunkAt(n int, off int64) ([]byte, error) {
	if u.closed {
		return nil, io.ErrClosedPipe
	}
	l := atomic.LoadInt64(&u.len)
	if off < 0 || off >= l {
		return nil, io.EOF
	}
	end := off + int64(n)
	if end > l {
		end = l
	}
	return u.buf[off:end], nil
}

// ReadAt reads into the provided buffer p starting at off.
func (u *MemoryBackend) ReadAt(p []byte, off int64) (n int, err error) {
	if u.closed {
		return 0, io.ErrClosedPipe
	}
	l := atomic.LoadInt64(&u.len)
	if off < 0 || off >= l {
		return 0, io.EOF
	}
	return copy(p, u.buf[off:l]), nil
}

// Close releases memory and causes future calls to ReadAt and Write to fail.
func (u *MemoryBackend) Close() error {
	u.buf = nil
	u.closed = true
	return nil
}
