package upload

import (
	"bytes"
	"context"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

type PieceCache struct {
	Buffer *bytes.Buffer
	Output HashedPieceLayer
}

func (pc *PieceCache) HashCalculated(ctx context.Context, hash []byte) error {
	return pc.Output.HashCalculated(ctx, hash)
}

//TODO: this caching is required to avoid too frequent signing. Can be avoided if we separated the signing and data sending.
func (pc *PieceCache) BeginPieceUpload(ctx context.Context, limit *pb.OrderLimit, address *pb.NodeAddress, privateKey storj.PiecePrivateKey) error {
	return pc.Output.BeginPieceUpload(ctx, limit, address, privateKey)
}

func (pc *PieceCache) WritePieceUpload(ctx context.Context, data []byte) error {
	if pc.Buffer.Len() < 262144 {
		pc.Buffer.Write(data)
		return nil
	}
	err := pc.Output.WritePieceUpload(ctx, pc.Buffer.Bytes())
	pc.Buffer.Reset()
	return err

}

func (pc *PieceCache) CommitPieceUpload(ctx context.Context) (*pb.SegmentPieceUploadResult, error) {
	return pc.Output.CommitPieceUpload(ctx)
}

var _ HashedPieceLayer = &PieceCache{}
