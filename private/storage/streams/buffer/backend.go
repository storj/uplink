// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package buffer

import (
	"io"
	"sync"
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
	return &MemoryBackend{
		buf: make([]byte, 0, cap),
	}
}

// MemoryBackend implements the Backend interface backed by a slice.
type MemoryBackend struct {
	mu     sync.Mutex
	buf    []byte
	closed bool
}

// Write appends the data to the buffer.
func (u *MemoryBackend) Write(p []byte) (n int, err error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.closed {
		return 0, io.ErrClosedPipe
	}
	u.buf = append(u.buf, p...)

	return len(p), nil
}

// ReadAt reads into the provided buffer p starting at off.
func (u *MemoryBackend) ReadAt(p []byte, off int64) (n int, err error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.closed {
		return 0, io.ErrClosedPipe
	} else if off >= int64(len(u.buf)) {
		return 0, io.EOF
	}
	n = copy(p, u.buf[off:])

	return n, nil
}

// Close releases memory and causes future calls to ReadAt and Write to fail.
func (u *MemoryBackend) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.buf = nil
	u.closed = true
	return nil
}
