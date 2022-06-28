package upload

import (
	"context"
	"errors"
	"github.com/zeebo/errs"
	"io"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"storj.io/uplink/private/piecestore"
)

type PieceWriter struct {
	Dialer rpc.Dialer

	//changing for each piece upload
	peerID         *identity.PeerIdentity
	privateKey     storj.PiecePrivateKey
	limit          *pb.OrderLimit
	address        *pb.NodeAddress
	stream         pb.DRPCPiecestore_UploadClient
	offset         int64
	AllocationStep int64
	finished       bool

	//hash, calculated by different level, received right before the PieceEnd
	hash        []byte
	client      pb.DRPCPiecestoreClient
	MaximumStep int64
}

var _ HashedPieceLayer = &PieceWriter{}

var Error = errs.Class("pieceupload")

var ErrProtocol = errs.Class("protocol")

// ErrInternal is an error class for internal errors.
var ErrInternal = errs.Class("internal")

func (pw *PieceWriter) HashCalculated(ctx context.Context, hash []byte) error {
	pw.hash = hash
	return nil
}

func (pw *PieceWriter) BeginPieceUpload(ctx context.Context, limit *pb.OrderLimit, address *pb.NodeAddress, privateKey storj.PiecePrivateKey) error {
	return pw.PutPiece(ctx, address, limit, privateKey)
}

func (pw *PieceWriter) WritePieceUpload(ctx context.Context, data []byte) error {
	return pw.write(ctx, data)
}

func (pw *PieceWriter) CommitPieceUpload(ctx context.Context) (*pb.SegmentPieceUploadResult, error) {
	hash, err := pw.commit(ctx)
	if err != nil {
		return nil, err
	}
	//TODO, who should fill the piece Num?
	return &pb.SegmentPieceUploadResult{
		NodeId: pw.limit.StorageNodeId,
		Hash:   hash,
	}, nil
}

func (pw *PieceWriter) PutPiece(ctx context.Context, address *pb.NodeAddress, limit *pb.OrderLimit, key storj.PiecePrivateKey) (err error) {
	nodeName := "nil"
	if pw.limit != nil {
		nodeName = pw.limit.StorageNodeId.String()[0:8]
	}
	defer mon.Task()(&ctx, "node: "+nodeName)(&err)
	pw.limit = limit
	pw.address = address
	pw.privateKey = key
	pw.finished = false

	storageNodeID := pw.limit.StorageNodeId

	conn, err := pw.Dialer.DialNodeURL(ctx, storj.NodeURL{
		ID:      storageNodeID,
		Address: pw.address.Address,
	})
	if err != nil {
		return Error.New("failed to dial (node:%v): %w", storageNodeID, err)
	}

	pw.client = pb.NewDRPCPiecestoreClient(conn)
	pw.peerID, err = conn.PeerIdentity()
	if err != nil {
		err = Error.New("failed getting peer identity (node:%v): %w", storageNodeID, err)
		return err
	}

	pw.stream, err = pw.client.Upload(ctx)
	if err != nil {
		return err
	}

	err = pw.stream.Send(&pb.PieceUploadRequest{
		Limit: pw.limit,
	})
	if err != nil {
		_, closeErr := pw.stream.CloseAndRecv()
		switch {
		case !errors.Is(err, io.EOF) && closeErr != nil:
			err = Error.Wrap(errs.Combine(err, closeErr))
		case closeErr != nil:
			err = Error.Wrap(closeErr)
		}

		return err
	}

	return nil
}

// write sends all data to the storagenode allocating as necessary.
func (pw *PieceWriter) write(ctx context.Context, sendData []byte) (err error) {
	defer mon.Task()(&ctx, "node: "+pw.peerID.ID.String()[0:8])(&err)

	// create a signed order for the next chunk
	order, err := signing.SignUplinkOrder(ctx, pw.privateKey, &pb.Order{
		SerialNumber: pw.limit.SerialNumber,
		Amount:       pw.offset + int64(len(sendData)),
	})
	if err != nil {
		return ErrInternal.Wrap(err)
	}

	// send signed order + data
	err = pw.stream.Send(&pb.PieceUploadRequest{
		Order: order,
		Chunk: &pb.PieceUploadRequest_Chunk{
			Offset: pw.offset,
			Data:   sendData,
		},
	})
	if err != nil {
		_, closeErr := pw.stream.CloseAndRecv()
		switch {
		case !errors.Is(err, io.EOF) && closeErr != nil:
			err = ErrProtocol.Wrap(errs.Combine(err, closeErr))
		case closeErr != nil:
			err = ErrProtocol.Wrap(closeErr)
		}

		return err
	}

	// update our offset
	pw.offset += int64(len(sendData))

	// update allocation step, incrementally building trust
	pw.AllocationStep = pw.nextAllocationStep(pw.AllocationStep)
	return nil
}

// next allocation step find the next trusted step.
func (pw *PieceWriter) nextAllocationStep(previous int64) int64 {
	// TODO: ensure that this is frame idependent
	next := previous * 3 / 2
	if next > pw.MaximumStep {
		next = pw.MaximumStep
	}
	return next
}

// commit finishes uploading by sending the piece-hash and retrieving the piece-hash.
func (pw *PieceWriter) commit(ctx context.Context) (_ *pb.PieceHash, err error) {
	defer mon.Task()(&ctx, "node: "+pw.peerID.ID.String()[0:8])(&err)
	if pw.finished {
		return nil, io.EOF
	}
	pw.finished = true

	// sign the hash for storage node
	uplinkHash, err := signing.SignUplinkPieceHash(ctx, pw.privateKey, &pb.PieceHash{
		PieceId:   pw.limit.PieceId,
		PieceSize: pw.offset,
		Hash:      pw.hash,
		Timestamp: pw.limit.OrderCreation,
	})
	if err != nil {
		// failed to sign, let's close, no need to wait for a response
		closeErr := pw.stream.Close()
		// closeErr being io.EOF doesn't inform us about anything
		return nil, Error.Wrap(errs.Combine(err, ignoreEOF(closeErr)))
	}

	// exchange signed piece hashes
	// 1. send our piece hash
	sendErr := pw.stream.Send(&pb.PieceUploadRequest{
		Done: uplinkHash,
	})

	// 2. wait for a piece hash as a response
	response, closeErr := pw.stream.CloseAndRecv()
	if response == nil || response.Done == nil {
		// combine all the errors from before
		// sendErr is io.EOF when failed to send, so don't care
		// closeErr is io.EOF when storage node closed before sending us a response
		return nil, errs.Combine(ErrProtocol.New("expected piece hash"), ignoreEOF(sendErr), ignoreEOF(closeErr))
	}

	// verification
	verifyErr := piecestore.VerifyPieceHash(ctx, pw.peerID, pw.limit, response.Done, uplinkHash.Hash)

	// combine all the errors from before
	// sendErr is io.EOF when we failed to send
	// closeErr is io.EOF when storage node closed properly
	return response.Done, errs.Combine(verifyErr, ignoreEOF(sendErr), ignoreEOF(closeErr))
}

// ignoreEOF is an utility func for ignoring EOF error, when it's not important.
func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
