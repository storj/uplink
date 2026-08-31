// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package piecestore

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/common/testrand"
)

// fakeUploadStream is an uploadStream that reassembles the chunks it is sent.
// It copies the chunk data, which is what the real stream effectively does when
// it marshals the request, so that reusing the caller's buffer is safe.
type fakeUploadStream struct {
	received bytes.Buffer
}

func (f *fakeUploadStream) Context() context.Context { return context.Background() }
func (f *fakeUploadStream) Close() error             { return nil }

func (f *fakeUploadStream) Send(req *pb.PieceUploadRequest) error {
	if req.Chunk != nil {
		f.received.Write(req.Chunk.Data)
	}
	return nil
}

func (f *fakeUploadStream) CloseAndRecv() (*pb.PieceUploadResponse, error) { return nil, nil }

func newTestUpload(stream uploadStream, key storj.PiecePrivateKey, bufferSize int64) *upload {
	config := DefaultConfig
	config.UploadBufferSize = bufferSize

	return &upload{
		client:        &Client{config: config},
		limit:         &pb.OrderLimit{Limit: 1 << 40},
		privateKey:    key,
		stream:        stream,
		hash:          pb.NewHashFromAlgorithm(pb.PieceHashAlgorithm_BLAKE3),
		hashAlgorithm: pb.PieceHashAlgorithm_BLAKE3,
		orderStep:     config.InitialStep,
	}
}

// TestUploadWrite covers the buffer reuse in write: the backing array comes
// from a pool, so a stale buffer from a previous upload must not leak into the
// data sent to the storage node.
func TestUploadWrite(t *testing.T) {
	_, key, err := storj.NewPieceKey()
	require.NoError(t, err)

	for _, bufferSize := range []int64{pooledUploadBufferSize, 5000} {
		// several sequential uploads, so that later ones get a dirty buffer
		// back from the pool.
		for _, size := range []int64{0, 1, 1000, pooledUploadBufferSize, pooledUploadBufferSize + 1, 3*pooledUploadBufferSize - 7} {
			data := testrand.BytesInt(int(size))
			stream := &fakeUploadStream{}
			up := newTestUpload(stream, key, bufferSize)

			// commit fails because the fake stream returns no piece hash; the
			// data sent up to that point is still what we want to check.
			_, err := up.write(context.Background(), bytes.NewReader(data))
			require.Error(t, err)

			require.True(t, bytes.Equal(data, stream.received.Bytes()),
				"buffer size %d, data size %d: sent data does not match", bufferSize, size)
			require.Equal(t, size, up.offset, "buffer size %d, data size %d", bufferSize, size)
		}
	}
}

func TestUploadBuffer(t *testing.T) {
	for _, size := range []int64{pooledUploadBufferSize, 5000} {
		buf := getUploadBuffer(size)
		require.Len(t, buf, int(size))
		putUploadBuffer(buf)
	}
}

func BenchmarkUploadWrite(b *testing.B) {
	_, key, err := storj.NewPieceKey()
	require.NoError(b, err)

	// pieces are a segment divided by the number of erasure shares, so the
	// smaller sizes are what uploads of small objects look like.
	for _, pieceSize := range []memory.Size{4 * memory.KiB, 64 * memory.KiB, memory.MiB} {
		b.Run(pieceSize.String(), func(b *testing.B) {
			data := testrand.BytesInt(pieceSize.Int())
			stream := &fakeUploadStream{}
			b.SetBytes(pieceSize.Int64())

			for b.Loop() {
				stream.received.Reset()
				up := newTestUpload(stream, key, DefaultConfig.UploadBufferSize)
				_, _ = up.write(context.Background(), bytes.NewReader(data))
			}
		})
	}
}
