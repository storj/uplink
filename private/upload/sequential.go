package upload

import (
	"context"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream"
)

type SequentialRouter struct {
	CreateOutput func() PieceLayer
	outputs      []PieceLayer
}

func (l *SequentialRouter) BeginSegment(ctx context.Context, piecePrivateKey storj.PiecePrivateKey, limits []*pb.AddressedOrderLimit, redundancyStrategy eestream.RedundancyStrategy) error {
	//at first time we create the outputs for each node
	if l.outputs == nil || len(l.outputs) == 0 {
		for i := 0; i < len(limits); i++ {
			l.outputs = append(l.outputs, l.CreateOutput())
		}
	}
	for ix, limit := range limits {
		//TODO this supposed to be done in async
		err := l.outputs[ix].BeginPieceUpload(ctx, limit.Limit, limit.StorageNodeAddress, piecePrivateKey)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *SequentialRouter) CommitSegment(ctx context.Context) ([]*pb.SegmentPieceUploadResult, error) {
	var uploadResults []*pb.SegmentPieceUploadResult
	for _, output := range l.outputs {
		uploadResult, err := output.CommitPieceUpload(ctx)
		if err != nil {
			return uploadResults, err
		}
		//todo return the hashes to upper layers
		uploadResults = append(uploadResults, uploadResult)
	}
	return uploadResults, nil
}

func (l *SequentialRouter) StartPieceUpload(ctx context.Context, ecShareIndex int, data []byte) error {
	//TODO: do this in async way
	//TODO: what if we hove not enough worker?
	return l.outputs[ecShareIndex].WritePieceUpload(ctx, data)
}

var _ ErasureEncodedLayer = &SequentialRouter{}
