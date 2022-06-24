package stub

import (
	"bytes"
	"context"
	"fmt"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/drpc"
	"sync"
)

type DownloadState struct {
	initialOffset int64
	maxPaidOffset int64
	position      int64
}

type pieceStoreDownloadStub struct {
	node          *nodeStub
	closed        chan struct{}
	once          sync.Once
	key           storj.PiecePrivateKey
	pieceSet      map[int][]byte
	pieceTemplate []byte
}

type BufferWithClose struct {
	buffer *bytes.Buffer
}

func (c *BufferWithClose) Read(p []byte) (n int, err error) {
	return c.buffer.Read(p)
}

func (c *BufferWithClose) Close() error {
	return nil
}

func NewPieceStoreDownloadStub(node *nodeStub, template []byte) (*pieceStoreDownloadStub, error) {
	_, key, err := storj.NewPieceKey()
	if err != nil {
		panic(err)
	}

	return &pieceStoreDownloadStub{
		pieceTemplate: template,
		key:           key,
		node:          node,
		closed:        make(chan struct{}),
	}, nil
}

func (p *pieceStoreDownloadStub) Close() error {
	p.once.Do(func() {
		close(p.closed)
	})
	return nil
}

func (p *pieceStoreDownloadStub) Closed() <-chan struct{} {
	return p.closed
}

func (p *pieceStoreDownloadStub) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	panic("implement me")
}

func (p *pieceStoreDownloadStub) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	return &pieceStoreDownloadStream{
		key:           p.key,
		rpc:           rpc,
		ctx:           ctx,
		node:          p.node,
		requests:      make(chan drpc.Message, 1000),
		downloads:     make(map[string]*DownloadState),
		pieceTemplate: p.pieceTemplate,
	}, nil
}

type pieceStoreDownloadStream struct {
	ctx           context.Context
	node          *nodeStub
	rpc           string
	requests      chan drpc.Message
	key           storj.PiecePrivateKey
	downloads     map[string]*DownloadState
	pieceTemplate []byte
}

func (p *pieceStoreDownloadStream) Context() context.Context {
	return p.ctx
}

func (p *pieceStoreDownloadStream) MsgSend(msg drpc.Message, enc drpc.Encoding) error {
	switch m := msg.(type) {
	case *pb.PieceDownloadRequest:
		key := p.getStreamKey(m)
		downloadState, found := p.downloads[key]
		if !found {
			downloadState = &DownloadState{}
			p.downloads[key] = downloadState
		}
		if m.Order != nil {
			downloadState.maxPaidOffset = downloadState.initialOffset + m.Order.Amount
		}
		if m.Chunk != nil {
			downloadState.initialOffset = m.Chunk.Offset
			downloadState.position = m.Chunk.Offset
			return nil
		}

	default:
		panic(fmt.Sprintf("%T is not supported", m))
	}
	p.requests <- msg
	return nil
}

func (p *pieceStoreDownloadStream) getStreamKey(m *pb.PieceDownloadRequest) string {
	key := ""
	if m.Limit != nil {
		key = m.Limit.SerialNumber.String()
	}
	if m.Order != nil {
		key = m.Order.SerialNumber.String()
	}
	return key
}

func (p *pieceStoreDownloadStream) MsgRecv(msg drpc.Message, enc drpc.Encoding) error {
	request := <-p.requests

	switch m := request.(type) {
	case *pb.PieceDownloadRequest:
		response := msg.(*pb.PieceDownloadResponse)
		key := p.getStreamKey(m)
		downloadState := p.downloads[key]
		remaining := downloadState.maxPaidOffset - downloadState.position
		if remaining > 0 {
			response.Chunk = &pb.PieceDownloadResponse_Chunk{
				Offset: downloadState.position,
				Data:   p.pieceTemplate[downloadState.position : downloadState.position+remaining],
			}
			downloadState.position += remaining
		}
	default:
		panic(fmt.Sprintf("%T is not supported", m))
	}
	return nil
}

func (p *pieceStoreDownloadStream) CloseSend() error {
	close(p.requests)
	return nil
}

func (p *pieceStoreDownloadStream) Close() error {
	fmt.Println("Close")
	close(p.requests)
	return nil
}

var _ drpc.Conn = &pieceStoreDownloadStub{}
