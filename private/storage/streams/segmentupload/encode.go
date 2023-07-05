// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package segmentupload

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/zeebo/errs"

	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/storage/streams/buffer"
)

// EncodedReader provides a redundant piece for given reader.
type EncodedReader struct {
	r         io.Reader
	rs        eestream.RedundancyStrategy
	num       int
	stripeBuf []byte
	shareBuf  []byte
	available int
	err       error
}

// NewEncodedReader provides a reader that returns a redundant piece for the
// reader using the given redundancy strategy and piece number.
func NewEncodedReader(r io.Reader, rs eestream.RedundancyStrategy, num int) *EncodedReader {
	return &EncodedReader{
		r:         r,
		rs:        rs,
		num:       num,
		stripeBuf: make([]byte, rs.StripeSize()),
		shareBuf:  make([]byte, rs.ErasureShareSize()),
	}
}

// Read reads the redundant piece data.
func (er *EncodedReader) Read(p []byte) (n int, err error) {
	// No need to trace this function because it's very fast and called many times.
	if er.err != nil {
		return 0, er.err
	}

	for len(p) > 0 {
		if er.available == 0 {
			// take the next stripe from the segment buffer
			_, err := io.ReadFull(er.r, er.stripeBuf)
			if errors.Is(err, io.EOF) {
				er.err = io.EOF
				break
			} else if err != nil {
				er.err = errs.Wrap(err)
				return 0, er.err
			}

			// encode the num-th erasure share
			err = er.rs.EncodeSingle(er.stripeBuf, er.shareBuf, er.num)
			if err != nil {
				er.err = err
				return 0, err
			}

			er.available = len(er.shareBuf)
		}

		off := len(er.shareBuf) - er.available
		nc := copy(p, er.shareBuf[off:])
		p = p[nc:]
		er.available -= nc
		n += nc
	}

	return n, er.err
}

// EncodedChunker provides a redundant piece for given reader.
type EncodedChunker struct {
	r   buffer.Chunker
	rs  eestream.RedundancyStrategy
	num int

	err  error
	done bool
}

// NewEncodedChunker provides a reader that returns a redundant piece for the
// reader using the given redundancy strategy and piece number.
func NewEncodedChunker(r buffer.Chunker, rs eestream.RedundancyStrategy, num int) *EncodedChunker {
	return &EncodedChunker{
		r:   r,
		rs:  rs,
		num: num,
	}
}

// Read reads the redundant piece data.
func (ec *EncodedChunker) WriteTo(w io.Writer) (n int64, err error) {
	share := make([]byte, ec.rs.ErasureShareSize())
	stripe := ec.rs.StripeSize()

	for {
		if ec.err != nil {
			return n, ec.err
		}
		if ec.done {
			return n, nil
		}

		buf, err := ec.r.Chunk(stripe)
		if len(buf) != stripe {
			if err == io.EOF {
				tmp := make([]byte, stripe)
				copy(tmp, buf)
				copy(tmp[len(buf):], makePadding(n+int64(len(buf)), stripe))
				buf = tmp
				ec.done = true
			} else if err != nil {
				return n, errs.Wrap(err)
			} else {
				return n, errs.New("invalid chunk size")
			}
		}

		err = ec.rs.EncodeSingle(buf, share, ec.num)
		if err != nil {
			ec.err = err
			return n, err
		}

		nn, err := w.Write(share)
		n += int64(nn)
		if err != nil {
			ec.err = err
			return n, err
		}
	}
}

// makePadding calculates how many bytes of padding are needed to fill
// an encryption block then creates a slice of zero bytes that size.
// The last byte of the padding slice contains the count of the total padding bytes added.
func makePadding(dataLen int64, blockSize int) []byte {
	amount := dataLen + 4
	r := amount % int64(blockSize)
	padding := 4
	if r > 0 {
		padding += blockSize - int(r)
	}
	paddingBytes := bytes.Repeat([]byte{0}, padding)
	binary.BigEndian.PutUint32(paddingBytes[padding-4:], uint32(padding))
	return paddingBytes
}
