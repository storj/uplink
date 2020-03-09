// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package piecestore

import (
	"context"
	"errors"
	"hash"
	"io"

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/pkcrypto"
	"storj.io/common/signing"
	"storj.io/common/storj"
)

var mon = monkit.Package()

// Upload implements uploading to the storage node.
type upload struct {
	client     *Client
	limit      *pb.OrderLimit
	privateKey storj.PiecePrivateKey
	peer       *identity.PeerIdentity
	stream     uploadStream

	hash           hash.Hash // TODO: use concrete implementation
	offset         int64
	allocationStep int64

	// when there's a send error then it will automatically close
	finished bool
}

type uploadStream interface {
	Context() context.Context
	CloseSend() error
	Send(*pb.PieceUploadRequest) error
	CloseAndRecv() (*pb.PieceUploadResponse, error)
}

// UploadReader uploads to the storage node.
func (client *Client) UploadReader(ctx context.Context, limit *pb.OrderLimit, piecePrivateKey storj.PiecePrivateKey, data io.Reader) (hash *pb.PieceHash, err error) {
	defer mon.Task()(&ctx, "node: "+limit.StorageNodeId.String()[0:8])(&err)

	peer, err := client.conn.PeerIdentity()
	if err != nil {
		return nil, ErrInternal.Wrap(err)
	}

	stream, err := client.client.Upload(ctx)
	if err != nil {
		return nil, err
	}

	err = stream.Send(&pb.PieceUploadRequest{
		Limit: limit,
	})
	if err != nil {
		_, closeErr := stream.CloseAndRecv()
		switch {
		case !errors.Is(err, io.EOF) && closeErr != nil:
			err = ErrProtocol.Wrap(errs.Combine(err, closeErr))
		case closeErr != nil:
			err = ErrProtocol.Wrap(closeErr)
		}

		return nil, err
	}

	upload := &upload{
		client:     client,
		limit:      limit,
		privateKey: piecePrivateKey,
		peer:       peer,
		stream:     stream,

		hash:           pkcrypto.NewHash(),
		offset:         0,
		allocationStep: client.config.InitialStep,
	}

	return upload.write(ctx, data)
}

// write sends all data to the storagenode allocating as necessary.
func (client *upload) write(ctx context.Context, data io.Reader) (hash *pb.PieceHash, err error) {
	defer mon.Task()(&ctx, "node: "+client.peer.ID.String()[0:8])(&err)

	defer func() {
		if err != nil {
			err = errs.Combine(err, client.cancel(ctx))
			return
		}
	}()

	// write the hash of the data sent to the server
	data = io.TeeReader(data, client.hash)

	backingArray := make([]byte, client.client.config.MaximumStep)

	done := false
	for !done {
		// try our best to read up to the next allocation step
		sendData := backingArray[:client.allocationStep]
		n, readErr := io.ReadFull(cancelingReader(ctx, data), sendData)
		if readErr != nil {
			if !errors.Is(readErr, io.ErrUnexpectedEOF) && !errors.Is(readErr, io.EOF) {
				return nil, ErrInternal.Wrap(readErr)
			}
			done = true
		}
		if n <= 0 {
			continue
		}
		sendData = sendData[:n]

		// create a signed order for the next chunk
		order, err := signing.SignUplinkOrder(ctx, client.privateKey, &pb.Order{
			SerialNumber: client.limit.SerialNumber,
			Amount:       client.offset + int64(len(sendData)),
		})
		if err != nil {
			return nil, ErrInternal.Wrap(err)
		}

		// send signed order + data
		err = client.stream.Send(&pb.PieceUploadRequest{
			Order: order,
			Chunk: &pb.PieceUploadRequest_Chunk{
				Offset: client.offset,
				Data:   sendData,
			},
		})
		if err != nil {
			_, closeErr := client.stream.CloseAndRecv()
			switch {
			case !errors.Is(err, io.EOF) && closeErr != nil:
				err = ErrProtocol.Wrap(errs.Combine(err, closeErr))
			case closeErr != nil:
				err = ErrProtocol.Wrap(closeErr)
			}

			return nil, err
		}

		// update our offset
		client.offset += int64(len(sendData))

		// update allocation step, incrementally building trust
		client.allocationStep = client.client.nextAllocationStep(client.allocationStep)
	}

	return client.commit(ctx)
}

// cancel cancels the uploading.
func (client *upload) cancel(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)
	if client.finished {
		return io.EOF
	}
	client.finished = true
	return Error.Wrap(client.stream.CloseSend())
}

// commit finishes uploading by sending the piece-hash and retrieving the piece-hash.
func (client *upload) commit(ctx context.Context) (_ *pb.PieceHash, err error) {
	defer mon.Task()(&ctx, "node: "+client.peer.ID.String()[0:8])(&err)
	if client.finished {
		return nil, io.EOF
	}
	client.finished = true

	// sign the hash for storage node
	uplinkHash, err := signing.SignUplinkPieceHash(ctx, client.privateKey, &pb.PieceHash{
		PieceId:   client.limit.PieceId,
		PieceSize: client.offset,
		Hash:      client.hash.Sum(nil),
		Timestamp: client.limit.OrderCreation,
	})
	if err != nil {
		// failed to sign, let's close the sending side, no need to wait for a response
		closeErr := client.stream.CloseSend()
		// closeErr being io.EOF doesn't inform us about anything
		return nil, Error.Wrap(errs.Combine(err, ignoreEOF(closeErr)))
	}

	// exchange signed piece hashes
	// 1. send our piece hash
	sendErr := client.stream.Send(&pb.PieceUploadRequest{
		Done: uplinkHash,
	})

	// 2. wait for a piece hash as a response
	response, closeErr := client.stream.CloseAndRecv()
	if response == nil || response.Done == nil {
		// combine all the errors from before
		// sendErr is io.EOF when failed to send, so don't care
		// closeErr is io.EOF when storage node closed before sending us a response
		return nil, errs.Combine(ErrProtocol.New("expected piece hash"), ignoreEOF(sendErr), ignoreEOF(closeErr))
	}

	// verification
	verifyErr := client.client.VerifyPieceHash(client.stream.Context(), client.peer, client.limit, response.Done, uplinkHash.Hash)

	// combine all the errors from before
	// sendErr is io.EOF when we failed to send
	// closeErr is io.EOF when storage node closed properly
	return response.Done, errs.Combine(verifyErr, ignoreEOF(sendErr), ignoreEOF(closeErr))
}

func cancelingReader(ctx context.Context, r io.Reader) io.Reader {
	return readerFunc(func(p []byte) (n int, err error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return r.Read(p)
		}
	})
}

type readerFunc func(p []byte) (n int, err error)

func (rf readerFunc) Read(p []byte) (n int, err error) { return rf(p) }
