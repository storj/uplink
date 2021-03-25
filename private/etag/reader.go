// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package etag

import (
	"hash"
	"io"

	"github.com/zeebo/errs"
)

// Reader that calculates ETag from content.
//
// CurrentETag returns the ETag calculated from the content that is already read.
type Reader interface {
	io.Reader
	CurrentETag() []byte
}

// HashReader implements the etag.Reader interface by reading from an io.Reader
// and calculating the ETag with a hash.Hash.
type HashReader struct {
	io.Reader
	h hash.Hash
}

// NewHashReader returns a new HashReader reading from r and calculating the ETag with h.
func NewHashReader(r io.Reader, h hash.Hash) *HashReader {
	return &HashReader{
		Reader: r,
		h:      h,
	}
}

func (r *HashReader) Read(b []byte) (n int, err error) {
	n, err = r.Reader.Read(b)
	_, hashErr := r.h.Write(b[:n])
	return n, errs.Combine(err, hashErr)
}

// CurrentETag returns the ETag for the content that has already been read from
// the reader.
func (r *HashReader) CurrentETag() []byte {
	return r.h.Sum(nil)
}
