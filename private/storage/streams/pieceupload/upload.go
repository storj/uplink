// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package pieceupload

import (
	"context"
	"io"

	"github.com/spacemonkeygo/monkit/v3"

	"storj.io/common/pb"
	"storj.io/common/storj"
)

var mon = monkit.Package()

// PiecePutter puts pieces.
type PiecePutter interface {
	// PutPiece puts a piece using the given limit and private key. The
	// operation can be cancelled using the longTailCtx or uploadCtx is
	// cancelled.
	PutPiece(longTailCtx, uploadCtx context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (hash *pb.PieceHash, deprecated *struct{}, err error)
}

// UploadOne uploads one piece from the manager using the given private key. If
// it fails, it will attempt to upload another until either the upload context,
// or the long tail context is cancelled.
func UploadOne(longTailCtx, uploadCtx context.Context, manager *Manager, putter PiecePutter, privateKey storj.PiecePrivateKey) (_ bool, err error) {
	defer mon.Task()(&longTailCtx)(&err)

	// If the long tail context is cancelled, then return a nil error.
	defer func() {
		if longTailCtx.Err() != nil {
			err = nil
		}
	}()

	for {
		piece, limit, done, err := manager.NextPiece(longTailCtx)
		if err != nil {
			return false, err
		}

		hash, _, err := putter.PutPiece(longTailCtx, uploadCtx, limit, privateKey, io.NopCloser(piece))
		done(hash, err == nil)
		if err == nil {
			return true, nil
		}

		if err := uploadCtx.Err(); err != nil {
			return false, err
		}

		if longTailCtx.Err() != nil {
			// If this context is done but the uploadCtx context isn't, then the
			// download was cancelled for long tail optimization purposes. This
			// is expected. Return that there was no error but that the upload
			// did not complete.
			return false, nil
		}
	}
}
