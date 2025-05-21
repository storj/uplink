// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package piecestore

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/pb"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
	"storj.io/drpc/drpctest"
)

func TestSendRetain(t *testing.T) {
	mock := &MockPieceStore{}
	c1, c2 := pipe()

	ctx := drpctest.NewTracker(t)
	mux := drpcmux.New()

	err := pb.DRPCRegisterPiecestore(mux, mock)
	require.NoError(t, err)

	srv := drpcserver.NewWithOptions(mux, drpcserver.Options{})
	ctx.Run(func(ctx context.Context) { _ = srv.ServeOne(ctx, c1) })
	conn := drpcconn.NewWithOptions(c2, drpcconn.Options{})

	c := Client{}
	c.client = pb.NewDRPCPiecestoreClient(conn)

	for _, size := range []int{10, 100, retainMessageLimit, retainMessageLimit + 1, retainMessageLimit - 1, retainMessageLimit*2 - 1, retainMessageLimit * 2, retainMessageLimit*2 + 1} {
		t.Run(fmt.Sprintf("with_size_%d", size), func(t *testing.T) {
			mock.lastReq = nil
			data := make([]byte, size)
			_, err := rand.Read(data)
			require.NoError(t, err)
			created := time.Now()
			err = c.Retain(ctx, &pb.RetainRequest{
				Filter:       data,
				CreationDate: created,
			})
			require.NoError(t, err)
			require.Eventuallyf(t, func() bool {
				return !mock.Unused()
			}, 10*time.Second, 100*time.Millisecond, "Message is not received")
			require.Equal(t, created.UTC(), mock.lastReq.CreationDate.UTC())
			require.Equal(t, data, mock.lastReq.Filter)
		})
	}
	t.Run("wrong hash", func(t *testing.T) {
		stream, err := c.client.RetainBig(ctx)
		require.NoError(t, err)
		err = stream.Send(&pb.RetainRequest{
			CreationDate: time.Now(),
			Filter:       []byte{1, 2, 3, 4},
			Hash:         make([]byte, 4),
		})
		require.NoError(t, err)
		_, err = stream.CloseAndRecv()
		require.ErrorContains(t, err, "Hash mismatch")
	})
}

func TestCompatibility(t *testing.T) {
	mock := &MockPieceStore{}
	c1, c2 := pipe()

	ctx := drpctest.NewTracker(t)
	mux := drpcmux.New()

	// this is a tricky server which excludes RetainBig from the available RPC methods
	// emulating servers before RetaingBig is implemented
	err := mux.Register(mock, LegacyPieceStoreDescription{})
	require.NoError(t, err)

	srv := drpcserver.NewWithOptions(mux, drpcserver.Options{})
	ctx.Run(func(ctx context.Context) { _ = srv.ServeOne(ctx, c1) })
	conn := drpcconn.NewWithOptions(c2, drpcconn.Options{})

	c := Client{}
	c.client = pb.NewDRPCPiecestoreClient(conn)
	t.Run("small filter", func(t *testing.T) {
		mock.lastReq = nil

		data := make([]byte, 10)
		_, err := rand.Read(data)
		require.NoError(t, err)

		created := time.Now()
		err = c.Retain(ctx, &pb.RetainRequest{
			Filter:       data,
			CreationDate: created,
		})

		// should work, with calling the original Retain
		require.NoError(t, err)
		require.Eventuallyf(t, func() bool {
			return !mock.Unused()
		}, 10*time.Second, 100*time.Millisecond, "Message is not received")
		require.Equal(t, created.UTC(), mock.lastReq.CreationDate.UTC())
		require.Equal(t, data, mock.lastReq.Filter)

	})
	t.Run("big filter", func(t *testing.T) {
		mock.lastReq = nil

		data := make([]byte, 10*1024*1024)
		_, err := rand.Read(data)
		require.NoError(t, err)

		created := time.Now()
		err = c.Retain(ctx, &pb.RetainRequest{
			Filter:       data,
			CreationDate: created,
		})

		// workaround couldn't work as message is too big
		require.Error(t, err)

	})
}

// MockPieceStore is a partial DRPC Piecestore implementation.
type MockPieceStore struct {
	mu      sync.Mutex
	lastReq *pb.RetainRequest
	pb.DRPCPiecestoreUnimplementedServer
}

// Retain implements pb.DRPCPiecestoreServer.
func (s *MockPieceStore) Retain(ctx context.Context, req *pb.RetainRequest) (*pb.RetainResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReq = req
	return &pb.RetainResponse{}, nil
}

func (s *MockPieceStore) Unused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastReq == nil
}

// RetainBig implements pb.DRPCPiecestoreServer.
func (s *MockPieceStore) RetainBig(stream pb.DRPCPiecestore_RetainBigStream) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lastReq, err := RetainRequestFromStream(stream)
	if err != nil {
		return err
	}
	s.lastReq = &lastReq
	return err
}

func pipe() (drpc.Transport, drpc.Transport) {
	type rwc struct {
		io.Reader
		io.Writer
		io.Closer
	}
	c1r, c1w := io.Pipe()
	c2r, c2w := io.Pipe()

	return rwc{c1r, c2w, c2w}, rwc{c2r, c1w, c1w}
}

// LegacyPieceStoreDescription is like the existing pb.DRPCPiecestoreDescription, but the RetainBig method is filtered out.
type LegacyPieceStoreDescription struct {
	Current pb.DRPCPiecestoreDescription
}

var _ drpc.Description = LegacyPieceStoreDescription{}

// NumMethods implements drpc.Description.
func (l LegacyPieceStoreDescription) NumMethods() int {
	return l.Current.NumMethods() - 1
}

// Method implements drpc.Description.
func (l LegacyPieceStoreDescription) Method(n int) (rpc string, encoding drpc.Encoding, receiver drpc.Receiver, method any, ok bool) {
	index := 0
	for i := 0; i < l.Current.NumMethods(); i++ {
		rpc, encoding, receiver, method, ok := l.Current.Method(i)
		if strings.Contains(rpc, "RetainBig") {
			continue
		}
		if index == n {
			return rpc, encoding, receiver, method, ok
		}
		index++
	}
	panic("index was too hight")
}
