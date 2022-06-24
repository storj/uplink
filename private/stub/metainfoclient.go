package stub

import (
	"context"
	"fmt"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/drpc"
)

type MetaInfoClientStub struct {
	Nodes stubNodes
}

func (m MetaInfoClientStub) GetObject(ctx context.Context, in *pb.ObjectGetRequest) (*pb.ObjectGetResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) ListObjects(ctx context.Context, in *pb.ObjectListRequest) (*pb.ObjectListResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) BeginDeleteObject(ctx context.Context, in *pb.ObjectBeginDeleteRequest) (*pb.ObjectBeginDeleteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) FinishDeleteObject(ctx context.Context, in *pb.ObjectFinishDeleteRequest) (*pb.ObjectFinishDeleteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) GetObjectIPs(ctx context.Context, in *pb.ObjectGetIPsRequest) (*pb.ObjectGetIPsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) ListPendingObjectStreams(ctx context.Context, in *pb.ObjectListPendingStreamsRequest) (*pb.ObjectListPendingStreamsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) DownloadObject(ctx context.Context, in *pb.ObjectDownloadRequest) (*pb.ObjectDownloadResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) UpdateObjectMetadata(ctx context.Context, in *pb.ObjectUpdateMetadataRequest) (*pb.ObjectUpdateMetadataResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) BeginCopyObject(ctx context.Context, in *pb.ObjectBeginCopyRequest) (*pb.ObjectBeginCopyResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) FinishCopyObject(ctx context.Context, in *pb.ObjectFinishCopyRequest) (*pb.ObjectFinishCopyResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) DRPCConn() drpc.Conn {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) CreateBucket(ctx context.Context, in *pb.BucketCreateRequest) (*pb.BucketCreateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) GetBucket(ctx context.Context, in *pb.BucketGetRequest) (*pb.BucketGetResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) DeleteBucket(ctx context.Context, in *pb.BucketDeleteRequest) (*pb.BucketDeleteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) ListBuckets(ctx context.Context, in *pb.BucketListRequest) (*pb.BucketListResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) BeginObject(ctx context.Context, in *pb.ObjectBeginRequest) (*pb.ObjectBeginResponse, error) {
	fmt.Println("begin object")
	return &pb.ObjectBeginResponse{
		Bucket:        in.Bucket,
		EncryptedPath: in.EncryptedPath,
	}, nil
}

func (m MetaInfoClientStub) CommitObject(ctx context.Context, in *pb.ObjectCommitRequest) (*pb.ObjectCommitResponse, error) {
	fmt.Println("commit object")
	return &pb.ObjectCommitResponse{}, nil
}

func (m MetaInfoClientStub) BeginSegment(ctx context.Context, in *pb.SegmentBeginRequest) (*pb.SegmentBeginResponse, error) {
	limits := []*pb.AddressedOrderLimit{}
	for i := 0; i < 20; i++ {
		limits = append(limits, &pb.AddressedOrderLimit{
			Limit: &pb.OrderLimit{
				Limit: 1024,
			},
			StorageNodeAddress: &pb.NodeAddress{
				Transport: pb.NodeTransport_TCP_TLS_GRPC,
				Address:   m.Nodes[i].Address,
			},
		})
	}

	_, key, err := storj.NewPieceKey()
	if err != nil {
		return nil, err
	}
	return &pb.SegmentBeginResponse{
		SegmentId:       pb.SegmentID{},
		AddressedLimits: limits,
		PrivateKey:      key,
		RedundancyScheme: &pb.RedundancyScheme{
			Type:             pb.RedundancyScheme_RS,
			MinReq:           10,
			Total:            20,
			RepairThreshold:  11,
			SuccessThreshold: 20,
			ErasureShareSize: 1024,
		},
	}, nil
}

func (m MetaInfoClientStub) CommitSegment(ctx context.Context, in *pb.SegmentCommitRequest) (*pb.SegmentCommitResponse, error) {
	return &pb.SegmentCommitResponse{
		SuccessfulPieces: int32(len(in.UploadResult)),
	}, nil
}

func (m MetaInfoClientStub) MakeInlineSegment(ctx context.Context, in *pb.SegmentMakeInlineRequest) (*pb.SegmentMakeInlineResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) BeginDeleteSegment(ctx context.Context, in *pb.SegmentBeginDeleteRequest) (*pb.SegmentBeginDeleteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) FinishDeleteSegment(ctx context.Context, in *pb.SegmentFinishDeleteRequest) (*pb.SegmentFinishDeleteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) ListSegments(ctx context.Context, in *pb.SegmentListRequest) (*pb.SegmentListResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) DownloadSegment(ctx context.Context, in *pb.SegmentDownloadRequest) (*pb.SegmentDownloadResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) DeletePart(ctx context.Context, in *pb.PartDeleteRequest) (*pb.PartDeleteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) Batch(ctx context.Context, in *pb.BatchRequest) (*pb.BatchResponse, error) {
	response := &pb.BatchResponse{
		Responses: []*pb.BatchResponseItem{},
	}
	for _, r := range in.GetRequests() {
		switch e := r.Request.(type) {
		case *pb.BatchRequestItem_ObjectBegin:
			resp, err := m.BeginObject(ctx, e.ObjectBegin)
			if err != nil {
				return nil, err
			}
			response.Responses = append(response.Responses, &pb.BatchResponseItem{
				Response: &pb.BatchResponseItem_ObjectBegin{
					ObjectBegin: resp,
				},
			})
		case *pb.BatchRequestItem_SegmentBegin:
			resp, err := m.BeginSegment(ctx, e.SegmentBegin)
			if err != nil {
				return nil, err
			}
			response.Responses = append(response.Responses, &pb.BatchResponseItem{
				Response: &pb.BatchResponseItem_SegmentBegin{
					SegmentBegin: resp,
				},
			})
		case *pb.BatchRequestItem_SegmentCommit:
			resp, err := m.CommitSegment(ctx, e.SegmentCommit)
			if err != nil {
				return nil, err
			}
			response.Responses = append(response.Responses, &pb.BatchResponseItem{
				Response: &pb.BatchResponseItem_SegmentCommit{
					SegmentCommit: resp,
				},
			})
		case *pb.BatchRequestItem_ObjectCommit:
			resp, err := m.CommitObject(ctx, e.ObjectCommit)
			if err != nil {
				return nil, err
			}
			response.Responses = append(response.Responses, &pb.BatchResponseItem{
				Response: &pb.BatchResponseItem_ObjectCommit{
					ObjectCommit: resp,
				},
			})
		default:
			panic(fmt.Sprintf("%T is not supported", e))
		}
	}
	return response, nil
}

func (m MetaInfoClientStub) ProjectInfo(ctx context.Context, in *pb.ProjectInfoRequest) (*pb.ProjectInfoResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) RevokeAPIKey(ctx context.Context, in *pb.RevokeAPIKeyRequest) (*pb.RevokeAPIKeyResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) BeginMoveObject(ctx context.Context, in *pb.ObjectBeginMoveRequest) (*pb.ObjectBeginMoveResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m MetaInfoClientStub) FinishMoveObject(ctx context.Context, in *pb.ObjectFinishMoveRequest) (*pb.ObjectFinishMoveResponse, error) {
	//TODO implement me
	panic("implement me")
}

var _ pb.DRPCMetainfoClient = &MetaInfoClientStub{}
