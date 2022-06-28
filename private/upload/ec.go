package upload

import (
	"context"
	"github.com/vivint/infectious"
	"github.com/zeebo/errs"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream"
)

type ECWriter struct {
	fec      *infectious.FEC
	output   ErasureEncodedLayer
	strategy eestream.RedundancyStrategy
}

type ErasureEncodedLayer interface {
	BeginSegment(ctx context.Context, piecePrivateKey storj.PiecePrivateKey, limits []*pb.AddressedOrderLimit, redundancyStrategy eestream.RedundancyStrategy) error
	CommitSegment(ctx context.Context) ([]*pb.SegmentPieceUploadResult, error)
	StartPieceUpload(ctx context.Context, ecShareIndex int, data []byte) error
}

var _ EncryptedObjectLayer = &ECWriter{}

func NewECWRiter(output ErasureEncodedLayer, redundancyStrategy eestream.RedundancyStrategy) (*ECWriter, error) {
	fec, err := infectious.NewFEC(redundancyStrategy.RequiredCount(), redundancyStrategy.TotalCount())
	if err != nil {
		return nil, errs.Wrap(err)
	}
	return &ECWriter{
		output:   output,
		fec:      fec,
		strategy: redundancyStrategy,
	}, nil
}

func (w *ECWriter) BeginSegment(ctx context.Context, piecePrivateKey storj.PiecePrivateKey, limits []*pb.AddressedOrderLimit) (err error) {
	return w.output.BeginSegment(ctx, piecePrivateKey, limits, w.strategy)
}

func (w *ECWriter) CommitSegment(ctx context.Context) ([]*pb.SegmentPieceUploadResult, error) {
	return w.output.CommitSegment(ctx)
}

func (w *ECWriter) EncryptedWrite(ctx context.Context, i int, bytes []byte) error {
	return w.upload(ctx, bytes)
}

func (w *ECWriter) EncryptedCommit(ctx context.Context) error {
	return nil
}

func (w *ECWriter) upload(ctx context.Context, buffer []byte) error {
	intErrors := []error{}
	err := w.fec.Encode(buffer, func(share infectious.Share) {
		err := w.output.StartPieceUpload(ctx, share.Number, share.Data)
		if err != nil {
			intErrors = append(intErrors, err)
		}
	})
	if err != nil {
		return err
	}
	if len(intErrors) > 0 {
		return errs.Combine(intErrors...)
	}

	//TODO: we may introduce an other method here to make it possible to wait until one chunk is uploaded.
	return nil

}

type StartPieceUpload struct {
	Number int
	Data   []byte
}

func (w *StartPieceUpload) String() string {
	return "StartPieceUpload"
}

type WaitForPieces struct {
}

func (w *WaitForPieces) String() string {
	return "WaitForPieces"
}
