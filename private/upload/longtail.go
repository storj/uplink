package upload

import (
	"context"
	"fmt"
	"github.com/zeebo/errs"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream"
	"time"
)

type PieceLayer interface {
	BeginPieceUpload(ctx context.Context, limit *pb.OrderLimit, address *pb.NodeAddress, privateKey storj.PiecePrivateKey) error
	WritePieceUpload(ctx context.Context, data []byte) error
	CommitPieceUpload(ctx context.Context) (*pb.SegmentPieceUploadResult, error)
}

type LongTailRouter struct {
	CreateOutput       func() PieceLayer
	control            map[int]chan interface{}
	hashes             chan *pb.SegmentPieceUploadResult
	redundancyStrategy eestream.RedundancyStrategy
}

type beginPiece struct {
	piecePrivateKey storj.PiecePrivateKey
	limit           *pb.AddressedOrderLimit
}

type commit struct{}

func (l *LongTailRouter) BeginSegment(ctx context.Context, piecePrivateKey storj.PiecePrivateKey, limits []*pb.AddressedOrderLimit, redundancyStrategy eestream.RedundancyStrategy) error {
	//need buffer here to avoid blocking
	l.hashes = make(chan *pb.SegmentPieceUploadResult)
	l.control = make(map[int]chan interface{})
	l.redundancyStrategy = redundancyStrategy
	//at first time we create the outputs for each node
	if l.control == nil || len(l.control) == 0 {
		for i := 0; i < len(limits); i++ {
			c := make(chan interface{})
			l.control[i] = c

			go func() {
				out := l.CreateOutput()
				for {
					select {
					case msg, ok := <-c:
						//channel is closed
						if !ok {
							return
						}
						switch m := msg.(type) {
						case beginPiece:
							err := out.BeginPieceUpload(ctx, m.limit.Limit, m.limit.StorageNodeAddress, m.piecePrivateKey)
							if err != nil {
								fmt.Println(err)
							}
						case []byte:
							err := out.WritePieceUpload(ctx, m)
							if err != nil {
								fmt.Println(err)
							}

						case commit:
							hash, err := out.CommitPieceUpload(ctx)
							if err != nil {
								fmt.Println(err)
							}
							l.hashes <- hash

						}
					case <-ctx.Done():
						//return
						//context is cancelled
					}
				}
			}()
			c <- beginPiece{
				piecePrivateKey: piecePrivateKey,
				limit:           limits[i],
			}

		}
	}
	return nil
}

func (l *LongTailRouter) CommitSegment(ctx context.Context) ([]*pb.SegmentPieceUploadResult, error) {
	for _, c := range l.control {
		c <- commit{}
	}

	hashes := make([]*pb.SegmentPieceUploadResult, 0)
	timeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for i := 0; i < l.redundancyStrategy.RequiredCount(); i++ {
		select {
		case hash := <-l.hashes:
			hashes = append(hashes, hash)
		case <-timeout.Done():
			return nil, errs.New("request is timed out")
		}

	}
	for _, c := range l.control {
		close(c)
	}
	l.control = nil
	return hashes, nil
}

func (l *LongTailRouter) StartPieceUpload(ctx context.Context, ecShareIndex int, data []byte) error {
	l.control[ecShareIndex] <- data
	return nil
}

var _ ErasureEncodedLayer = &LongTailRouter{}
