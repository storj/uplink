package stub

import (
	"context"
	"crypto/tls"
	"github.com/stretchr/testify/require"
	"storj.io/common/encryption"
	"storj.io/common/identity/testidentity"
	"storj.io/common/macaroon"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/rpc/rpcpool"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/drpc"
	"storj.io/uplink/private/ecclient"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams"
	stream2 "storj.io/uplink/private/stream"
	"testing"
)

func TestUpload(t *testing.T) {
	nodes, err := NewStubNodes(20)
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

	db := metaclient.New(nil, nil)
	object, err := db.CreateObject(ctx, "bucket1", "key1", &metaclient.CreateObject{})
	require.NoError(t, err)

	stream, err := object.CreateStream(ctx)
	require.NoError(t, err)

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
	streamStore, err := streams.NewStreamStore(metaClient, ec, 64*1024*1024, encryptionStore, storj.EncryptionParameters{
		CipherSuite: storj.EncAESGCM,
		BlockSize:   11 * 256,
	}, 10)
	require.NoError(t, err)

	upload := stream2.NewUpload(ctx, stream, streamStore)
	defer func() {
		err = upload.Close()
		require.NoError(t, err)
	}()

	bytes := make([]byte, 10224)
	for i := 0; i < 10224; i++ {
		bytes[i] = 12
	}
	max := 1_000_000
	for i := 0; i < max; i++ {
		_, err = upload.Write(bytes)
		require.NoError(t, err)
	}

}
