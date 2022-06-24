package stub

import (
	"context"
	"crypto/tls"
	"github.com/stretchr/testify/require"
	"storj.io/common/encryption"
	"storj.io/common/identity/testidentity"
	"storj.io/common/macaroon"
	"storj.io/common/pb"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/rpc/rpcpool"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/drpc"
	"storj.io/uplink/private/ecclient"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams"
	"storj.io/uplink/private/stream"
	"testing"
	"time"
)

func TestDownload(t *testing.T) {
	nodes, err := NewStubNodes(20)
	require.NoError(t, err)

	encryptionParameters := storj.EncryptionParameters{
		CipherSuite: storj.EncAESGCM,
		BlockSize:   256,
	}

	redundancyScheme := storj.RedundancyScheme{
		Algorithm:      storj.ReedSolomon,
		ShareSize:      1024,
		RepairShares:   14,
		RequiredShares: 10,
		OptimalShares:  18,
		TotalShares:    20,
	}

	size := 1024
	generated, err := GenerateData(size)
	require.NoError(t, err)

	ctx := rpcpool.WithDialerWrapper(testcontext.New(t), func(ctx context.Context, address string, dialer rpcpool.Dialer) rpcpool.Dialer {
		return func(context.Context) (drpc.Conn, *tls.ConnectionState, error) {
			node, err := nodes.GetByAddress(address)
			if err != nil {
				return nil, nil, err
			}
			return node.CreateDownloadConnection(generated[int(node.Index)])
		}
	})

	encryptionStore := encryption.NewStore()
	encryptionStore.SetDefaultKey(&storj.Key{})
	encryptionStore.SetDefaultPathCipher(storj.EncAESGCM)

	apiKey, err := macaroon.NewAPIKey([]byte{})
	require.NoError(t, err)

	uplinkIdent, err := testidentity.PregeneratedIdentity(1, storj.LatestIDVersion())
	require.NoError(t, err)

	options, err := tlsopts.NewOptions(uplinkIdent, tlsopts.Config{}, nil)
	require.NoError(t, err)

	dialer := rpc.NewDefaultDialer(options)
	ec := ecclient.New(dialer, 1024)
	metaClient := metaclient.NewClient(MetaInfoClientStub{
		Nodes: nodes,
	}, apiKey, "")

	streamStore, err := streams.NewStreamStore(metaClient, ec, 64*1024*1024, encryptionStore, encryptionParameters, 10)
	require.NoError(t, err)

	dataSize := 1024

	downloadInfo := metaclient.DownloadInfo{
		Object: storj.Object{
			Bucket: storj.Bucket{
				Name: "bucket1",
			},
			Path: "key1",
			Stream: storj.Stream{
				ID:                   storj.StreamID{},
				Size:                 int64(dataSize),
				FixedSegmentSize:     64 * 1024 * 1024,
				EncryptionParameters: encryptionParameters,
			},
		},
		DownloadedSegments: make([]metaclient.DownloadSegmentWithRSResponse, 0),
		ListSegments: metaclient.ListSegmentsResponse{
			Items:                make([]metaclient.SegmentListItem, 0),
			More:                 false,
			EncryptionParameters: encryptionParameters,
		},
	}

	var orderLimits []*pb.AddressedOrderLimit
	for i := int16(0); i < redundancyScheme.TotalShares; i++ {
		id := storj.PieceID{}
		id[0] = byte(i)
		orderLimits = append(orderLimits, &pb.AddressedOrderLimit{
			StorageNodeAddress: &pb.NodeAddress{
				Address:   "10.10.10.10:1234",
				Transport: pb.NodeTransport_TCP_TLS_GRPC,
			},
			Limit: &pb.OrderLimit{
				PieceId: id,
			},
		})

	}
	_, privateKey, err := storj.NewPieceKey()
	require.NoError(t, err)

	downloadInfo.DownloadedSegments = append(downloadInfo.DownloadedSegments, metaclient.DownloadSegmentWithRSResponse{
		Info: metaclient.SegmentDownloadInfo{
			SegmentID:     storj.SegmentID{},
			PlainOffset:   0,
			PlainSize:     int64(size),
			EncryptedSize: int64(size),
			Position: &storj.SegmentPosition{
				PartNumber: 0,
				Index:      0,
			},
			RedundancyScheme:  redundancyScheme,
			SegmentEncryption: storj.SegmentEncryption{},
			PiecePrivateKey:   privateKey,
		},
		Limits: orderLimits,
	})
	downloadInfo.ListSegments.Items = append(downloadInfo.ListSegments.Items, metaclient.SegmentListItem{
		Position: metaclient.SegmentPosition{
			PartNumber: 0,
			Index:      0,
		},
		PlainSize:   64 * 1024 * 1024,
		PlainOffset: 0,
		CreatedAt:   time.Now(),
	})

	download := stream.NewDownload(ctx, downloadInfo, streamStore)
	defer func() {
		err = download.Close()
		require.NoError(t, err)
	}()

	var bytes = make([]byte, 20000)
	for i := 0; i < 1_000_000; i++ {
		_, err = download.Read(bytes)
		require.NoError(t, err)
	}

}
