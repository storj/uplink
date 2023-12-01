// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package buffer

import (
	"io"
	"sync"
	"sync/atomic"
)

// Backend is a backing store of bytes for a Backend.
type Backend interface {
	io.Writer
	io.ReaderAt
	io.Closer
}

const (
	standardMaxEncryptedSegmentSize = 67254016
)

var (
	standardPool = sync.Pool{
		New: func() interface{} {
			// TODO: this pool approach is a bit of a bandaid - it would be good to
			// rework this logic to not require this large allocation at all.
			return new([standardMaxEncryptedSegmentSize]byte)
		},
	}
)

// NewMemoryBackend returns a MemoryBackend with the provided initial
// capacity. It implements the Backend interface.
func NewMemoryBackend(cap int64) (rv *MemoryBackend) {
	rv = &MemoryBackend{}
	if cap == standardMaxEncryptedSegmentSize {
		rv.buf = standardPool.Get().(*[standardMaxEncryptedSegmentSize]byte)[:]
	} else {
		rv.buf = make([]byte, cap)
	}
	return rv
}

// MemoryBackend implements the Backend interface backed by a slice.
type MemoryBackend struct {
	len    int64
	buf    []byte
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
	buf := u.buf
	u.buf = nil
	u.closed = true
	if len(buf) == standardMaxEncryptedSegmentSize {
		standardPool.Put((*[standardMaxEncryptedSegmentSize]byte)(buf))
	}
	return nil
}
