package stub

import (
	"context"
	"fmt"
	"storj.io/common/pb"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"storj.io/drpc"
	"sync"
	"time"
)

type pieceStoreUploadStub struct {
	node   *nodeStub
	closed chan struct{}
	once   sync.Once
	key    storj.PiecePrivateKey
}

func NewPieceStoreUploadStub(node *nodeStub) *pieceStoreUploadStub {
	_, key, err := storj.NewPieceKey()
	if err != nil {
		panic(err)
	}

	return &pieceStoreUploadStub{
		key:    key,
		node:   node,
		closed: make(chan struct{}),
	}
}

func (p pieceStoreUploadStub) Close() error {
	p.once.Do(func() {
		close(p.closed)
	})
	return nil
}

func (p pieceStoreUploadStub) Closed() <-chan struct{} {
	return p.closed
}

func (p pieceStoreUploadStub) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	//TODO implement me
	panic("implement me")
}

func (p pieceStoreUploadStub) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	return &pieceStoreUploadStream{
		key:      p.key,
		rpc:      rpc,
		ctx:      ctx,
		node:     p.node,
		requests: make(chan drpc.Message, 1000),
	}, nil
}

type pieceStoreUploadStream struct {
	ctx      context.Context
	node     *nodeStub
	rpc      string
	requests chan drpc.Message
	key      storj.PiecePrivateKey
	closed   bool
}

func (p *pieceStoreUploadStream) Context() context.Context {
	return p.ctx
}

func (p *pieceStoreUploadStream) MsgSend(msg drpc.Message, enc drpc.Encoding) error {
	switch m := msg.(type) {
	case *pb.PieceUploadRequest:
		if m.Done == nil {
			return nil
		}
	case *pb.PieceDownloadRequest:
		fmt.Println("Piece download request")
	default:
		panic(fmt.Sprintf("%T is not supported", m))
	}
	p.requests <- msg
	return nil
}

func (p *pieceStoreUploadStream) MsgRecv(msg drpc.Message, enc drpc.Encoding) error {
	request := <-p.requests

	switch m := request.(type) {
	case *pb.PieceUploadRequest:
		response := msg.(*pb.PieceUploadResponse)
		if m.Done != nil {
			signer := signing.SignerFromFullIdentity(p.node.Identity)
			m.Done.Timestamp = time.Now()
			hash, err := signing.SignPieceHash(p.ctx, signer, m.Done)
			if err != nil {
				return err
			}
			response.Done = hash
		}
	case *pb.PieceDownloadRequest:
		offset := int64(0)
		response := msg.(*pb.PieceDownloadResponse)
		if m.Chunk == nil {
			fmt.Println(m)
		} else {
			offset = m.Chunk.Offset
		}
		response.Chunk = &pb.PieceDownloadResponse_Chunk{
			Offset: offset,
			Data:   []byte{1, 2, 3, 4},
		}

	default:
		panic(fmt.Sprintf("%T is not supported", m))
	}
	return nil
}

func (p *pieceStoreUploadStream) CloseSend() error {
	if !p.closed {
		close(p.requests)
	}
	p.closed = true
	return nil
}

func (p *pieceStoreUploadStream) Close() error {
	if !p.closed {
		close(p.requests)
	}
	p.closed = true
	return nil
}

var _ drpc.Conn = &pieceStoreUploadStub{}
