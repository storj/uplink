package main

import (
	"context"
	"crypto/tls"
	"github.com/pkg/profile"
	"io/ioutil"
	"log"
	"os"
	"storj.io/common/encryption"
	"storj.io/common/fpath"
	"storj.io/common/identity/testidentity"
	"storj.io/common/macaroon"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/rpc/rpcpool"
	"storj.io/common/storj"
	"storj.io/drpc"
	"storj.io/uplink/private/ecclient"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams"
	"storj.io/uplink/private/stream"
	"storj.io/uplink/private/stub"
)

func main() {
	hashType := os.Getenv("STORJ_HASH")
	if hashType == "" {
		hashType = "default"
	}
	tmpDir, _ := ioutil.TempDir("", "uplink-"+hashType+"-*")

	defer profile.Start(profile.ProfilePath(tmpDir)).Stop()
	err := run()
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func run() error {

	nodes, err := stub.NewStubNodes(20)
	if err != nil {
		return err
	}

	ctx := rpcpool.WithDialerWrapper(fpath.WithTempData(context.Background(), "", true), func(ctx context.Context, address string, dialer rpcpool.Dialer) rpcpool.Dialer {
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
	if err != nil {
		return err
	}

	objectStream, err := object.CreateStream(ctx)
	if err != nil {
		return err
	}
	encryptionStore := encryption.NewStore()
	encryptionStore.SetDefaultKey(&storj.Key{})
	encryptionStore.SetDefaultPathCipher(storj.EncAESGCM)

	apiKey, err := macaroon.NewAPIKey([]byte{})
	if err != nil {
		return err
	}
	uplinkIdent, err := testidentity.PregeneratedIdentity(1, storj.LatestIDVersion())
	if err != nil {
		return err
	}
	options, err := tlsopts.NewOptions(uplinkIdent, tlsopts.Config{}, nil)
	if err != nil {
		return err
	}
	dialer := rpc.NewDefaultDialer(options)
	ec := ecclient.New(dialer, 1024)
	metaClient := metaclient.NewClient(stub.MetaInfoClientStub{
		Nodes: nodes,
	}, apiKey, "")
	streamStore, err := streams.NewStreamStore(metaClient, ec, 64*1024*1024, encryptionStore, storj.EncryptionParameters{
		CipherSuite: storj.EncAESGCM,
		BlockSize:   11 * 256,
	}, 10)
	if err != nil {
		return err
	}
	upload := stream.NewUpload(ctx, objectStream, streamStore)
	defer func() {
		_ = upload.Close()
	}()
	bytes := make([]byte, 20000)
	for i := 0; i < 20000; i++ {
		bytes[i] = 12
	}
	for i := 0; i < 1_000_000; i++ {
		_, err = upload.Write(bytes)
		if err != nil {
			return err
		}
	}
	return nil
}
