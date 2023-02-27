// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package splitter

import (
	"io"
	"math/rand"
)

var emptyLimitReaderBuf [4096]byte

type emptyLimitReader struct{ n int }

func (e *emptyLimitReader) Read(p []byte) (n int, err error) {
	if e.n == 0 {
		return 0, io.EOF
	}
	if e.n < len(p) {
		p = p[:e.n]
	}
	for i := range p {
		p[i] = 0
	}
	e.n -= len(p)
	return len(p), nil
}

func (e *emptyLimitReader) WriteTo(w io.Writer) (n int64, err error) {
	for e.n > 0 {
		buf := emptyLimitReaderBuf[:]
		if e.n < len(buf) {
			buf = buf[:e.n]
		}

		nn, err := w.Write(buf)
		n += int64(nn)
		e.n -= nn

		if err != nil {
			return n, err
		}
	}

	return n, err
}

type emptyReader struct{}

func (emptyReader) Read(p []byte) (int, error) { return len(p), nil }

type randomWriter struct{ io.Writer }

func (r randomWriter) Write(p []byte) (n int, err error) {
	for len(p) > 0 {
		nn, err := r.Writer.Write(p[:rand.Intn(len(p)+1)])
		n += nn
		p = p[nn:]

		if err != nil {
			return n, err
		}
	}

	return n, nil
}
