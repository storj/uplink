// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package pieceupload

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/pb"
	"storj.io/common/storj"
)

var (
	fakePrivateKey = mustNewPiecePrivateKey()
)

func TestUploadOne(t *testing.T) {
	for _, tc := range []struct {
		desc           string
		failPuts       int
		cancelLongTail bool
		cancelUpload   bool
		expectUploaded bool
		expectNum      int
		expectErr      string
	}{
		{
			desc:           "first piece successful",
			expectUploaded: true,
			expectNum:      0,
		},
		{
			desc:           "second piece successful",
			failPuts:       1,
			expectUploaded: true,
			expectNum:      1,
		},
		{
			desc:         "upload canceled",
			cancelUpload: true,
			expectErr:    "context canceled",
		},
		{
			desc:           "long tail canceled",
			cancelLongTail: true,
			expectUploaded: false,
		},
		{
			desc:      "manager fails to return next piece",
			failPuts:  2,
			expectErr: "piece limit exchange failed: oh no",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			uploadCtx, uploadCancel := context.WithCancel(context.Background())
			t.Cleanup(uploadCancel)
			longTailCtx, longTailCancel := context.WithCancel(context.Background())
			t.Cleanup(longTailCancel)

			if tc.cancelUpload {
				uploadCancel()
			}
			if tc.cancelLongTail {
				longTailCancel()
			}

			manager := newManagerWithExchanger(2, failExchange{})
			putter := &fakePutter{t: t, failPuts: tc.failPuts}
			uploaded, err := UploadOne(longTailCtx, uploadCtx, manager, putter, fakePrivateKey, NewStallManager())
			if tc.expectErr != "" {
				require.EqualError(t, err, tc.expectErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectUploaded, uploaded)
			if tc.expectUploaded {
				assertResults(t, manager, revision{0}, makeResult(piecenum{tc.expectNum}, revision{0}))
			}
		})
	}
}

type fakePutter struct {
	t        *testing.T
	failPuts int
}

func (p *fakePutter) PutPiece(longTailCtx, uploadCtx context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (*pb.PieceHash, *struct{}, error) {
	assert.Equal(p.t, fakePrivateKey, privateKey, "private key was not passed correctly")

	num := pieceReaderNum(data)
	if p.failPuts > 0 {
		p.failPuts--
		return nil, nil, errs.New("put failed for piece: %d", num)
	}

	select {
	case <-uploadCtx.Done():
		return nil, nil, uploadCtx.Err()
	case <-longTailCtx.Done():
		return nil, nil, longTailCtx.Err()
	default:
		return hash(num), nil, nil
	}
}

func mustNewPiecePrivateKey() storj.PiecePrivateKey {
	pk, err := storj.PiecePrivateKeyFromBytes(bytes.Repeat([]byte{1}, 64))
	if err != nil {
		panic(err)
	}
	return pk
}
