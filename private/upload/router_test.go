package upload

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/vivint/infectious"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream"
	"testing"
)

var result []*pb.SegmentPieceUploadResult

func BenchmarkLongtail(b *testing.B) {

	ctx := context.Background()
	_, key, err := storj.NewPieceKey()
	require.NoError(b, err)

	fec, err := infectious.NewFEC(3, 10)
	require.NoError(b, err)
	es := eestream.NewRSScheme(fec, 1024)
	strategy, err := eestream.NewRedundancyStrategy(es, 8, 9)
	require.NoError(b, err)
	data := make([]byte, 0)

	limits := make([]*pb.AddressedOrderLimit, es.TotalCount())
	for j := 0; j < es.TotalCount(); j++ {
		limits[j] = &pb.AddressedOrderLimit{}
	}

	b.Run("longtail", func(b *testing.B) {
		c := LongTailRouter{
			CreateOutput: func() PieceLayer {
				return &NOOPPieceLayer{}
			},
		}

		for i := 0; i < b.N; i++ {
			simulateUpload(b, &c, ctx, key, limits, strategy, es, data)
		}
	})

	b.Run("seq", func(b *testing.B) {
		c := SequentialRouter{
			CreateOutput: func() PieceLayer {
				return &NOOPPieceLayer{}
			},
		}

		for i := 0; i < b.N; i++ {
			simulateUpload(b, &c, ctx, key, limits, strategy, es, data)
		}
	})

}

func simulateUpload(b *testing.B, c ErasureEncodedLayer, ctx context.Context, key storj.PiecePrivateKey, limits []*pb.AddressedOrderLimit, strategy eestream.RedundancyStrategy, es eestream.ErasureScheme, data []byte) {
	err := c.BeginSegment(ctx, key, limits, strategy)
	require.NoError(b, err)
	for chunks := 0; chunks < 100; chunks++ {
		for j := 0; j < es.TotalCount(); j++ {
			err := c.StartPieceUpload(ctx, j, data)
			require.NoError(b, err)
		}
	}
	result, err = c.CommitSegment(ctx)
	require.NoError(b, err)
}

type NOOPPieceLayer struct {
}

func (n *NOOPPieceLayer) BeginPieceUpload(ctx context.Context, limit *pb.OrderLimit, address *pb.NodeAddress, privateKey storj.PiecePrivateKey) error {
	return nil
}

func (n *NOOPPieceLayer) WritePieceUpload(ctx context.Context, data []byte) error {
	return nil
}

func (n *NOOPPieceLayer) CommitPieceUpload(ctx context.Context) (*pb.SegmentPieceUploadResult, error) {
	return nil, nil
}

var _ PieceLayer = (*NOOPPieceLayer)(nil)
