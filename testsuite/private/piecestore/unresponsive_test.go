// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information

package piecestore_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"storj.io/common/identity/testidentity"
	"storj.io/common/pb"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/storj/private/revocation"
	"storj.io/storj/private/server"
	"storj.io/uplink/private/piecestore"
)

type piecestoreMock struct {
	log       *zap.Logger
	completed chan struct{}
}

var _ pb.DRPCPiecestoreServer = &piecestoreMock{}

func (mock *piecestoreMock) Upload(server pb.DRPCPiecestore_UploadStream) error {
	return mock.stall(server.Context())
}

func (mock *piecestoreMock) Download(server pb.DRPCPiecestore_DownloadStream) error {
	return mock.stall(server.Context())
}

func (mock *piecestoreMock) Delete(ctx context.Context, delete *pb.PieceDeleteRequest) (_ *pb.PieceDeleteResponse, err error) {
	return nil, nil
}

func (mock *piecestoreMock) DeletePieces(ctx context.Context, delete *pb.DeletePiecesRequest) (_ *pb.DeletePiecesResponse, err error) {
	return nil, nil
}

func (mock *piecestoreMock) Retain(ctx context.Context, retain *pb.RetainRequest) (_ *pb.RetainResponse, err error) {
	return nil, nil
}

func (mock *piecestoreMock) RetainBig(stream pb.DRPCPiecestore_RetainBigStream) error {
	return nil
}

func (mock *piecestoreMock) RestoreTrash(context.Context, *pb.RestoreTrashRequest) (*pb.RestoreTrashResponse, error) {
	return nil, nil
}

func (mock *piecestoreMock) Exists(context.Context, *pb.ExistsRequest) (*pb.ExistsResponse, error) {
	return nil, nil
}

func (mock *piecestoreMock) stall(ctx context.Context) error {
	mock.log.Info("stall started")
	defer func() { mock.completed <- struct{}{} }()

	timoutTicker := time.NewTicker(60 * time.Second)
	defer timoutTicker.Stop()

	select {
	case <-timoutTicker.C:
		mock.log.Error("stall timed out")
		return nil
	case <-ctx.Done():
		mock.log.Info("stall cancelled")
		return nil
	}
}

func TestUnresponsiveStoragenode(t *testing.T) {
	ctx := testcontext.New(t)
	log := zaptest.NewLogger(t)

	/* setup a mock piecestore */

	serverTLSConfig := tlsopts.Config{
		RevocationDBURL:    "bolt://" + ctx.File("fakestoragenode", "revocation.db"),
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "*",
	}

	revocationDB, err := revocation.OpenDBFromCfg(ctx, serverTLSConfig)
	require.NoError(t, err)
	ctx.Check(revocationDB.Close)

	storagenodeIdent, err := testidentity.PregeneratedIdentity(1, storj.LatestIDVersion())
	require.NoError(t, err)

	serverOptions, err := tlsopts.NewOptions(storagenodeIdent, serverTLSConfig, revocationDB)
	require.NoError(t, err)

	server, err := server.New(log.Named("storagenode"), serverOptions,
		server.Config{
			Address:        "127.0.0.1:0",
			PrivateAddress: "127.0.0.1:0",
		})
	require.NoError(t, err)
	defer ctx.Check(server.Close)

	mockPiecestore := &piecestoreMock{
		log:       log.Named("piecestore-mock"),
		completed: make(chan struct{}, 2),
	}
	err = pb.DRPCRegisterPiecestore(server.DRPC(), mockPiecestore)
	require.NoError(t, err)

	subctx, subcancel := context.WithCancel(ctx)
	defer subcancel()
	ctx.Go(func() error {
		if err := server.Run(subctx); err != nil {
			return errs.Wrap(err)
		}
		return nil
	})

	// Create Uplink

	uplinkIdent, err := testidentity.PregeneratedIdentity(1, storj.LatestIDVersion())
	require.NoError(t, err)
	require.NotNil(t, uplinkIdent)

	uplinkOptions, err := tlsopts.NewOptions(uplinkIdent, tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "*",
	}, nil)
	require.NoError(t, err)

	uplinkDialer := rpc.NewDefaultDialer(uplinkOptions)

	piecestoreConfig := piecestore.DefaultConfig
	piecestoreConfig.MessageTimeout = 5 * time.Second

	// Start tests

	t.Run("Download", func(t *testing.T) {
		client, err := piecestore.Dial(ctx, uplinkDialer, storj.NodeURL{
			Address: server.Addr().String(),
			ID:      storagenodeIdent.ID,
		}, piecestoreConfig)
		require.NoError(t, err)
		defer ctx.Check(client.Close)

		_, privateKey, err := storj.NewPieceKey()
		require.NoError(t, err)
		download, err := client.Download(ctx, &pb.OrderLimit{}, privateKey, 0, 1024)
		require.NoError(t, err)
		defer func() { _ = download.Close() }()

		start := time.Now()
		_, err = io.ReadAll(download)
		require.Error(t, err)
		require.Contains(t, err.Error(), "message timeout")
		duration := time.Since(start)

		requireDurationWithin(t, piecestoreConfig.MessageTimeout, duration, 3*time.Second)
		/* TODO: currently the context cancellation doesn't propagate.
		select {
		case <-mockPiecestore.completed:
		case <-time.After(5 * time.Second):
			t.Fatal("expected piecestore to have exited")
		}
		*/
	})

	t.Run("Upload", func(t *testing.T) {
		client, err := piecestore.Dial(ctx, uplinkDialer, storj.NodeURL{
			Address: server.Addr().String(),
			ID:      storagenodeIdent.ID,
		}, piecestoreConfig)
		require.NoError(t, err)
		defer ctx.Check(client.Close)

		_, privateKey, err := storj.NewPieceKey()
		require.NoError(t, err)

		start := time.Now()
		_, err = client.UploadReader(ctx, &pb.OrderLimit{}, privateKey, bytes.NewBuffer([]byte{1, 2, 3, 4}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "message timeout")
		duration := time.Since(start)

		requireDurationWithin(t, piecestoreConfig.MessageTimeout, duration, 3*time.Second)

		/* TODO: currently the context cancellation doesn't propagate.
		select {
		case <-mockPiecestore.completed:
		case <-time.After(5 * time.Second):
			t.Fatal("expected piecestore to have exited")
		}
		*/
	})
}

func requireDurationWithin(t *testing.T, expected, actual, delta time.Duration) {
	t.Helper()
	if actual < expected-delta || actual > expected+delta {
		require.Failf(t, "duration outside of bounds", "Expected %v+-%v, but got %v", expected, delta, actual)
	}
}
