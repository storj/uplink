// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package piecestore

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/pb"
)

// fakeDownloadStream is a downloadStream whose Close returns a configurable
// error, emulating a storage node that has already torn down the connection.
type fakeDownloadStream struct {
	closeErr error
}

func (f *fakeDownloadStream) Close() error                             { return f.closeErr }
func (f *fakeDownloadStream) Send(*pb.PieceDownloadRequest) error      { return nil }
func (f *fakeDownloadStream) Recv() (*pb.PieceDownloadResponse, error) { return nil, io.EOF }

func newTestDownload(stream downloadStream) *Download {
	return &Download{
		stream:    stream,
		cancelCtx: func(error) {},
		limit:     &pb.OrderLimit{},
	}
}

// TestDownloadCloseSuppressesBenignStreamError reproduces the issue where
// closing a partially read download surfaced the stream-close handshake error
// (e.g. "use of closed network connection") even though the bytes that were
// read are correct and the caller is intentionally abandoning the download.
func TestDownloadCloseSuppressesBenignStreamError(t *testing.T) {
	// the kind of transport error a storage node teardown produces when the
	// uplink performs the mutual close handshake on an abandoned stream.
	benignCloseErr := errs.New("write tcp 127.0.0.1:1234->127.0.0.1:5678: use of closed network connection")

	t.Run("partial download", func(t *testing.T) {
		download := newTestDownload(&fakeDownloadStream{closeErr: benignCloseErr})
		download.downloadSize = 100
		download.read.Store(10) // only part of the piece was read

		// Closing a correctly-but-partially read download must not surface the
		// benign stream-close error. Before the fix, this returned the error
		// because read (10) < downloadSize (100).
		require.NoError(t, download.Close())
	})

	t.Run("full download", func(t *testing.T) {
		download := newTestDownload(&fakeDownloadStream{closeErr: benignCloseErr})
		download.downloadSize = 100
		download.read.Store(100) // everything was read

		require.NoError(t, download.Close())
	})

	t.Run("real error is still reported", func(t *testing.T) {
		realErr := errs.New("piece data corrupted")
		download := newTestDownload(&fakeDownloadStream{closeErr: benignCloseErr})
		download.downloadSize = 100
		download.read.Store(10)

		// when there is an actual error to report, it must still propagate,
		// even alongside a benign stream-close error.
		download.closeWithError(realErr)
		download.cancelCtx(context.Canceled)
		require.ErrorIs(t, download.closingError.Get(), realErr)
	})
}
