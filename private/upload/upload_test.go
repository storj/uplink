package upload

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"github.com/stretchr/testify/require"
	"storj.io/common/encryption"
	"storj.io/common/identity/testidentity"
	"storj.io/common/macaroon"
	"storj.io/common/memory"
	"storj.io/common/pb"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/rpc/rpcpool"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/drpc"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/stub"
	"testing"
)

func TestNg(t *testing.T) {
	t.Skip()
	nodes, err := stub.NewStubNodes(20)
	require.NoError(t, err)

	ctx := rpcpool.WithDialerWrapper(testcontext.New(t), func(ctx context.Context, address string, dialer rpcpool.Dialer) rpcpool.Dialer {
		return func(context.Context) (drpc.Conn, *tls.ConnectionState, error) {
			node, err := nodes.GetByAddress(address)
			if err != nil {
				return nil, nil, err
			}
			return node.CreateUploadConnection()
		}
	})

	apiKey, err := macaroon.NewAPIKey([]byte{})
	require.NoError(t, err)

	rsScheme := &pb.RedundancyScheme{
		Type:             pb.RedundancyScheme_RS,
		MinReq:           10,
		Total:            20,
		RepairThreshold:  11,
		SuccessThreshold: 20,
		ErasureShareSize: 1024,
	}

	encParam := storj.EncryptionParameters{
		CipherSuite: storj.EncAESGCM,
		BlockSize:   rsScheme.MinReq * rsScheme.ErasureShareSize,
	}

	contentKey := storj.Key{}
	_, err = rand.Read(contentKey[:])
	require.NoError(t, err)

	contentNonce := storj.Nonce{}
	_, err = encryption.Increment(&contentNonce, int64(0)+1)
	require.NoError(t, err)

	transformer, err := encryption.NewEncrypter(encParam.CipherSuite, &contentKey, &contentNonce, int(encParam.BlockSize))
	require.NoError(t, err)

	plainBlockSize := int(rsScheme.MinReq*rsScheme.ErasureShareSize) - transformer.OutBlockSize() + transformer.InBlockSize()
	fmt.Println(plainBlockSize)

	uplinkIdent, err := testidentity.PregeneratedIdentity(1, storj.LatestIDVersion())
	require.NoError(t, err)

	options, err := tlsopts.NewOptions(uplinkIdent, tlsopts.Config{}, nil)
	require.NoError(t, err)

	dialer := rpc.NewDefaultDialer(options)

	pieceLayer := func() PieceLayer {
		return &NoHash{
			Output: &PieceCache{
				Buffer: bytes.NewBuffer(make([]byte, 262144)),
				Output: &PieceWriter{
					Dialer:         dialer,
					AllocationStep: 65536,
					MaximumStep:    262144,
				},
			},
		}
	}
	longtail := SequentialRouter{
		CreateOutput: pieceLayer,
	}

	strategy, err := eestream.NewRedundancyStrategyFromProto(rsScheme)
	require.NoError(t, err)

	ec, err := NewECWRiter(&longtail, strategy)
	require.NoError(t, err)

	metaClient := metaclient.NewClient(stub.MetaInfoClientStub{
		Nodes: nodes,
	}, apiKey, "")

	meta := &ObjectMeta{
		Output:     ec,
		Metaclient: metaClient,
	}

	encrypter := &Encrypter{
		Output:             meta,
		EncryptedBlockSize: int(rsScheme.MinReq * rsScheme.ErasureShareSize),
	}

	keys := &KeyDerivation{
		Output: encrypter,
		Cipher: encParam.CipherSuite,
	}

	segmenter := &Segmenter{
		SegmentSize: 64 * memory.MiB.Int() / plainBlockSize * plainBlockSize,
		ChunkSize:   plainBlockSize,
		Output:      keys,
		ObjectInfo: func() *StartObject {
			return &StartObject{
				EncryptionParams: encParam,
			}
		},
	}

	padding := &Padding{
		ChunkSize: plainBlockSize,
		Output:    segmenter,
	}

	c, err := NewChunkedWriter(ctx, padding, plainBlockSize)

	bytes := make([]byte, c.size)
	k := int64(0)
	for i := 0; i < 1_000_000; i++ {
		_, err = c.Write(bytes)
		k += int64(len(bytes))
		require.NoError(t, err)
	}
	fmt.Println(k)

	require.NoError(t, c.Close())

}
