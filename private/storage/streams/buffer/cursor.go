// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package buffer

import (
	"sync"

	"github.com/zeebo/errs"
)

// Cursor keeps track of how many bytes have been written and the furthest advanced
// reader, letting one wait until space or bytes are available.
type Cursor struct {
	writeAhead int64

	mu   *sync.Mutex
	cond *sync.Cond

	doneReading bool
	doneWriting bool

	readErr  error
	writeErr error

	maxRead int64
	written int64
}

// NewCursor constructs a new cursor that keeps track of reads and writes
// into some buffer, allowing one to wait until enough data has been read or written.
func NewCursor(writeAhead int64) *Cursor {
	mu := new(sync.Mutex)
	return &Cursor{
		writeAhead: writeAhead,

		mu:   mu,
		cond: sync.NewCond(mu),
	}
}

// WaitRead blocks until the writer is done or until at least n bytes have been written.
// It returns min(n, w.written) letting the caller know the largest offset that can be read.
// The ok boolean is true if there are more bytes to be read. If writing is done with an
// error, then 0 and that error are returned. If writing is done with no error and the requested
// amount is at least the amount written, it returns the written amount, false, and nil.
func (w *Cursor) WaitRead(n int64) (m int64, ok bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.doneReading {
		return 0, false, errs.New("WaitRead called after DoneReading")
	}

	for {
		switch {
		// first, return any write error if there is one.
		case w.writeErr != nil:
			return 0, false, w.writeErr

		// next, return io.EOF when fully read.
		case n >= w.written && w.doneWriting:
			return w.written, false, nil

		// next, allow reading up to the written amount.
		case n <= w.written:
			return n, true, nil

		// next, if maxRead is not yet caught up to written, allow reads to proceed up to written.
		case w.maxRead < w.written:
			return w.written, true, nil

		// finally, if more is requested, allow at most the written amount.
		case w.doneWriting:
			return w.written, true, nil
		}

		w.cond.Wait()
	}
}

// WaitWrite blocks until the readers are done or until the furthest advanced reader is
// within the writeAhead of the writer. It returns the largest offset that can be written.
// The ok boolean is true if there are readers waiting for more bytes. If reading is done
// with an error, then 0 and that error are returned. If reading is done with no error, then
// it returns the amount written, false, and nil.
func (w *Cursor) WaitWrite(n int64) (m int64, ok bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.doneWriting {
		return 0, false, errs.New("WaitWrite called after DoneWriting")
	}

	for {
		switch {
		// first, return any read error if there is one.
		case w.readErr != nil:
			return 0, false, w.readErr

		// next, don't allow more writes if the reader is done.
		case w.doneReading:
			return w.written, false, nil

		// next, allow when enough behind the furthest advanced reader.
		case n <= w.maxRead+w.writeAhead:
			return n, true, nil

		// finally, only allow up to a maximum amount ahead of the furthest reader.
		case w.written < w.maxRead+w.writeAhead:
			return w.maxRead + w.writeAhead, true, nil
		}

		w.cond.Wait()
	}
}

// DoneWriting signals that no more Write calls will happen. It returns true
// the first time DoneWriting and DoneReading have both been called.
func (w *Cursor) DoneWriting(err error) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.doneWriting {
		w.doneWriting = true
		w.writeErr = err
		w.cond.Broadcast()

		return w.doneReading
	}

	return false
}

// DoneReading signals that no more Read calls will happen. It returns true
// the first time DoneWriting and DoneReading have both been called.
func (w *Cursor) DoneReading(err error) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.doneReading {
		w.doneReading = true
		w.readErr = err
		w.cond.Broadcast()

		return w.doneWriting
	}

	return false
}

// ReadTo reports to the cursor that some reader read up to byte offset n.
func (w *Cursor) ReadTo(n int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if n > w.maxRead {
		w.maxRead = n
		w.cond.Broadcast()
	}
}

// WroteTo reports to the cursor that the writer wrote up to byte offset n.
func (w *Cursor) WroteTo(n int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if n > w.written {
		w.written = n
		w.cond.Broadcast()
	}
}
