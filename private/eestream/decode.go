// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package eestream

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/errs2"
	"storj.io/common/ranger"
	"storj.io/common/readcloser"
)

type decodedReader struct {
	ctx               context.Context
	cancel            context.CancelFunc
	readers           map[int]io.ReadCloser
	scheme            ErasureScheme
	stripeReader      *StripeReader
	outbuf, outbufmem []byte
	err               error
	currentStripe     int64
	expectedStripes   int64
	close             sync.Once
	closeErr          error
}

// DecodeReaders2 takes a map of readers and an ErasureScheme returning a
// combined Reader.
//
// rs is a map of erasure piece numbers to erasure piece streams.
// expectedSize is the number of bytes expected to be returned by the Reader.
// mbm is the maximum memory (in bytes) to be allocated for read buffers. If
// set to 0, the minimum possible memory will be used.
// if forceErrorDetection is set to true then k+1 pieces will be always
// required for decoding, so corrupted pieces can be detected.
func DecodeReaders2(ctx context.Context, cancel func(), rs map[int]io.ReadCloser, es ErasureScheme, expectedSize int64, mbm int, forceErrorDetection bool) io.ReadCloser {
	defer mon.Task()(&ctx)(nil)
	if expectedSize < 0 {
		return readcloser.FatalReadCloser(Error.New("negative expected size"))
	}
	if expectedSize%int64(es.StripeSize()) != 0 {
		return readcloser.FatalReadCloser(
			Error.New("expected size (%d) not a factor decoded block size (%d)",
				expectedSize, es.StripeSize()))
	}
	if err := checkMBM(mbm); err != nil {
		return readcloser.FatalReadCloser(err)
	}
	expectedStripes := expectedSize / int64(es.StripeSize())
	dr := &decodedReader{
		readers:         rs,
		scheme:          es,
		outbufmem:       make([]byte, 0, 32*1024),
		expectedStripes: expectedStripes,
	}

	dr.stripeReader = NewStripeReader(rs, es, int(expectedStripes), forceErrorDetection)

	dr.ctx, dr.cancel = ctx, cancel
	// Kick off a goroutine to watch for context cancelation.
	go func() {
		<-dr.ctx.Done()
		_ = dr.Close()
	}()
	return dr
}

func (dr *decodedReader) Read(p []byte) (n int, err error) {
	ctx := dr.ctx

	if len(dr.outbuf) == 0 {
		// if the output buffer is empty, let's fill it again
		// if we've already had an error, fail
		if dr.err != nil {
			return 0, dr.err
		}
		// return EOF is the expected stripes were read
		if dr.currentStripe >= dr.expectedStripes {
			dr.err = io.EOF
			return 0, dr.err
		}
		// read the input buffers of the next stripe - may also decode it
		var newStripes int
		dr.outbuf, newStripes, dr.err = dr.stripeReader.ReadStripes(ctx, dr.currentStripe, dr.outbufmem)
		dr.currentStripe += int64(newStripes)
		if dr.err != nil {
			return 0, dr.err
		}
	}

	n = copy(p, dr.outbuf)
	dr.outbuf = dr.outbuf[n:]
	return n, nil
}

func (dr *decodedReader) Close() (err error) {
	ctx := dr.ctx
	defer mon.Task()(&ctx)(&err)
	errorThreshold := len(dr.readers) - dr.scheme.RequiredCount()
	var closeGroup errs2.Group
	// avoid double close of readers
	dr.close.Do(func() {
		for _, r := range dr.readers {
			r := r
			closeGroup.Go(func() error {
				return errs2.IgnoreCanceled(r.Close())
			})
		}

		// close the stripe reader
		closeGroup.Go(dr.stripeReader.Close)

		// We'll add a separate cancellation, just in case the Closing for some reason
		// does not finish in time. However, we don't want to call `dr.cancel` them
		// immediately, because this would not allow the connections to be pooled.
		ctxDelay, ctxDelayCancel := context.WithTimeout(ctx, time.Millisecond)
		defer ctxDelayCancel()
		go func() {
			<-ctxDelay.Done()
			dr.cancel()
		}()

		allErrors := closeGroup.Wait()
		errorThreshold -= len(allErrors)
		dr.closeErr = errs.Combine(allErrors...)
	})
	// ensure readers are definitely closed.
	dr.cancel()

	// TODO this is workaround, we need reorganize to return multiple errors or divide into fatal, non fatal
	if errorThreshold < 0 {
		return dr.closeErr
	}
	if dr.closeErr != nil {
		mon.Event("close failed")
	}
	return nil
}

type decodedRanger struct {
	es                  ErasureScheme
	rrs                 map[int]ranger.Ranger
	inSize              int64
	mbm                 int // max buffer memory
	forceErrorDetection bool
}

// Decode takes a map of Rangers and an ErasureScheme and returns a combined
// Ranger.
//
// rrs is a map of erasure piece numbers to erasure piece rangers.
// mbm is the maximum memory (in bytes) to be allocated for read buffers. If
// set to 0, the minimum possible memory will be used.
// if forceErrorDetection is set to true then k+1 pieces will be always
// required for decoding, so corrupted pieces can be detected.
func Decode(rrs map[int]ranger.Ranger, es ErasureScheme, mbm int, forceErrorDetection bool) (ranger.Ranger, error) {
	if err := checkMBM(mbm); err != nil {
		return nil, err
	}
	if len(rrs) < es.RequiredCount() {
		return nil, Error.New("not enough readers to reconstruct data!")
	}
	size := int64(-1)
	for _, rr := range rrs {
		if size == -1 {
			size = rr.Size()
		} else if size != rr.Size() {
			return nil, Error.New("decode failure: range reader sizes don't all match")
		}
	}
	if size == -1 {
		return ranger.ByteRanger(nil), nil
	}
	if size%int64(es.ErasureShareSize()) != 0 {
		return nil, Error.New("invalid erasure decoder and range reader combo. "+
			"range reader size (%d) must be a multiple of erasure encoder block size (%d)",
			size, es.ErasureShareSize())
	}
	return &decodedRanger{
		es:                  es,
		rrs:                 rrs,
		inSize:              size,
		mbm:                 mbm,
		forceErrorDetection: forceErrorDetection,
	}, nil
}

func (dr *decodedRanger) Size() int64 {
	blocks := dr.inSize / int64(dr.es.ErasureShareSize())
	return blocks * int64(dr.es.StripeSize())
}

func (dr *decodedRanger) Range(ctx context.Context, offset, length int64) (_ io.ReadCloser, err error) {
	defer mon.Task()(&ctx)(&err)

	ctx, cancel := context.WithCancel(ctx)
	// offset and length might not be block-aligned. figure out which
	// blocks contain this request
	firstBlock, blockCount := encryption.CalcEncompassingBlocks(offset, length, dr.es.StripeSize())

	// go ask for ranges for all those block boundaries
	readers := make(map[int]io.ReadCloser, len(dr.rrs))
	for i, rr := range dr.rrs {
		r, err := rr.Range(ctx, firstBlock*int64(dr.es.ErasureShareSize()), blockCount*int64(dr.es.ErasureShareSize()))
		if err != nil {
			readers[i] = readcloser.FatalReadCloser(err)
		} else {
			readers[i] = r
		}
	}

	// decode from all those ranges
	r := DecodeReaders2(ctx, cancel, readers, dr.es, blockCount*int64(dr.es.StripeSize()), dr.mbm, dr.forceErrorDetection)
	// offset might start a few bytes in, potentially discard the initial bytes
	_, err = io.CopyN(io.Discard, r, offset-firstBlock*int64(dr.es.StripeSize()))
	if err != nil {
		return nil, Error.Wrap(err)
	}
	// length might not have included all of the blocks, limit what we return
	return readcloser.LimitReadCloser(r, length), nil
}

func checkMBM(mbm int) error {
	if mbm < 0 {
		return Error.New("negative max buffer memory")
	}
	return nil
}
