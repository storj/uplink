// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package ecclient

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/pb"
	"storj.io/common/ranger"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/piecestore"
)

var mon = monkit.Package()

// GetOptions is a struct of options for Get.
type GetOptions struct {
	ErrorDetection bool
}

// Client defines an interface for storing erasure coded data to piece store nodes.
type Client interface {
	Get(ctx context.Context, limits []*pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, es eestream.ErasureScheme, size int64, opts GetOptions) (ranger.Ranger, error)
	// PutPiece uploads a single erasure share to the node addressed by the
	// limit. Segment-level uploads are driven by private/storage/streams,
	// which uses this to satisfy pieceupload.PiecePutter.
	PutPiece(ctx, parent context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (hash *pb.PieceHash, err error)
}

type dialPiecestoreFunc func(context.Context, storj.NodeURL) (*piecestore.Client, error)

type ecClient struct {
	dialer      rpc.Dialer
	memoryLimit int
}

// New creates a client from the given dialer and max buffer memory.
func New(dialer rpc.Dialer, memoryLimit int) Client {
	return &ecClient{
		dialer:      dialer,
		memoryLimit: memoryLimit,
	}
}

func (ec *ecClient) dialPiecestore(ctx context.Context, n storj.NodeURL) (*piecestore.Client, error) {
	hashAlgo := piecestore.GetPieceHashAlgo(ctx)
	client, err := piecestore.DialReplaySafe(ctx, ec.dialer, n, piecestore.DefaultConfig)
	if err != nil {
		return client, err
	}
	client.UploadHashAlgo = hashAlgo
	return client, nil
}

func (ec *ecClient) PutPiece(ctx, parent context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (hash *pb.PieceHash, err error) {
	if limit == nil {
		defer mon.Task()(&ctx, "node: nil")(&err)
		defer func() { err = errs.Combine(err, data.Close()) }()
		_, _ = io.Copy(io.Discard, data)
		return nil, nil
	}

	storageNodeID := limit.GetLimit().StorageNodeId
	defer mon.Task()(&ctx, "node: "+storageNodeID.String()[0:8])(&err)
	defer func() { err = errs.Combine(err, data.Close()) }()

	ps, err := ec.dialPiecestore(ctx, limitToNodeURL(limit))
	if err != nil {
		return nil, Error.New("failed to dial (node:%v): %w", storageNodeID, err)
	}
	defer func() { err = errs.Combine(err, ps.Close()) }()

	hash, err = ps.UploadReader(ctx, limit.GetLimit(), privateKey, data)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			// Canceled context means the piece upload was interrupted by user or due
			// to slow connection. No error logging for this case.
			if errors.Is(parent.Err(), context.Canceled) {
				err = Error.New("upload canceled by user: %w", err)
			} else {
				err = Error.New("upload cut due to slow connection (node:%v): %w", storageNodeID, err)
			}

			// make sure context.Canceled is the primary error in the error chain
			// for later errors.Is/errs2.IsCanceled checking
			err = errs.Combine(context.Canceled, err)
		} else {
			nodeAddress := ""
			if limit.GetStorageNodeAddress() != nil {
				nodeAddress = limit.GetStorageNodeAddress().GetAddress()
			}
			err = Error.New("upload failed (node:%v, address:%v): %w", storageNodeID, nodeAddress, err)
		}

		return nil, err
	}

	return hash, nil
}

func (ec *ecClient) Get(ctx context.Context, limits []*pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, es eestream.ErasureScheme, size int64, opts GetOptions) (rr ranger.Ranger, err error) {
	defer mon.Task()(&ctx)(&err)

	if len(limits) != es.TotalCount() {
		return nil, Error.New("size of limits slice (%d) does not match total count (%d) of erasure scheme", len(limits), es.TotalCount())
	}

	if nonNilCount(limits) < es.RequiredCount() {
		return nil, Error.New("number of non-nil limits (%d) is less than required count (%d) of erasure scheme", nonNilCount(limits), es.RequiredCount())
	}

	paddedSize := calcPadded(size, es.StripeSize())
	pieceSize := paddedSize / int64(es.RequiredCount())

	rrs := map[int]ranger.Ranger{}
	for i, addressedLimit := range limits {
		if addressedLimit == nil {
			continue
		}

		rrs[i] = &lazyPieceRanger{
			dialPiecestore: ec.dialPiecestore,
			limit:          addressedLimit,
			privateKey:     privateKey,
			size:           pieceSize,
		}
	}

	rr, err = eestream.Decode(rrs, es, ec.memoryLimit, opts.ErrorDetection)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	ranger, err := encryption.Unpad(rr, int(paddedSize-size))
	return ranger, Error.Wrap(err)
}

func calcPadded(size int64, blockSize int) int64 {
	mod := size % int64(blockSize)
	if mod == 0 {
		return size
	}
	return size + int64(blockSize) - mod
}

type lazyPieceRanger struct {
	dialPiecestore dialPiecestoreFunc
	limit          *pb.AddressedOrderLimit
	privateKey     storj.PiecePrivateKey
	size           int64
}

// Size implements Ranger.Size.
func (lr *lazyPieceRanger) Size() int64 {
	return lr.size
}

// Range implements Ranger.Range to be lazily connected.
func (lr *lazyPieceRanger) Range(ctx context.Context, offset, length int64) (_ io.ReadCloser, err error) {
	defer mon.Task()(&ctx)(&err)

	ctx, cancel := context.WithCancel(ctx)

	return &lazyPieceReader{
		ranger: lr,
		ctx:    ctx,
		cancel: cancel,
		offset: offset,
		length: length,
	}, nil
}

type lazyPieceReader struct {
	ranger *lazyPieceRanger
	ctx    context.Context
	cancel func()
	offset int64
	length int64

	mu       sync.Mutex
	isClosed bool
	download *piecestore.Download
	client   *piecestore.Client
}

func (lr *lazyPieceReader) Read(data []byte) (_ int, err error) {
	if err := lr.dial(); err != nil {
		return 0, err
	}
	return lr.download.Read(data)
}

func (lr *lazyPieceReader) dial() error {
	lr.mu.Lock()
	if lr.isClosed {
		lr.mu.Unlock()
		return io.EOF
	}
	if lr.download != nil {
		lr.mu.Unlock()
		return nil
	}
	lr.mu.Unlock()

	client, downloader, err := lr.ranger.dial(lr.ctx, lr.offset, lr.length)
	if err != nil {
		return Error.Wrap(err)
	}

	lr.mu.Lock()
	defer lr.mu.Unlock()

	if lr.isClosed {
		// Close tried to cancel the dialing, however failed to do so.
		lr.cancel()
		_ = downloader.Close()
		_ = client.Close()
		return io.ErrClosedPipe
	}

	lr.download = downloader
	lr.client = client

	return nil
}

func limitToNodeURL(limit *pb.AddressedOrderLimit) storj.NodeURL {
	return (&pb.Node{
		Id:      limit.GetLimit().StorageNodeId,
		Address: limit.GetStorageNodeAddress(),
	}).NodeURL()
}

var monLazyPieceRangerDialTask = mon.Task()

func (lr *lazyPieceRanger) dial(ctx context.Context, offset, length int64) (_ *piecestore.Client, _ *piecestore.Download, err error) {
	defer monLazyPieceRangerDialTask(&ctx)(&err)

	ps, err := lr.dialPiecestore(ctx, limitToNodeURL(lr.limit))
	if err != nil {
		return nil, nil, err
	}

	download, err := ps.Download(ctx, lr.limit.GetLimit(), lr.privateKey, offset, length)
	if err != nil {
		return nil, nil, errs.Combine(err, ps.Close())
	}
	return ps, download, nil
}

func (lr *lazyPieceReader) Close() (err error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	if lr.isClosed {
		return nil
	}
	lr.isClosed = true

	if lr.download != nil {
		err = errs.Combine(err, lr.download.Close())
	}
	if lr.client != nil {
		err = errs.Combine(err, lr.client.Close())
	}

	lr.cancel()
	return err
}

func nonNilCount(limits []*pb.AddressedOrderLimit) int {
	total := 0
	for _, limit := range limits {
		if limit != nil {
			total++
		}
	}
	return total
}
