package upload

import (
	"context"
	"fmt"
	"github.com/zeebo/errs"
	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams"
	"time"
)

type ObjectMeta struct {
	Output     EncryptedObjectLayer
	Metaclient *metaclient.Client
	encStore   *encryption.Store

	//requests created, but not yet send for better batching
	beginObjectReq *metaclient.BeginObjectParams
	commitSegment  *metaclient.CommitSegmentParams
	currentSegment metaclient.BeginSegmentResponse

	//TODO: get this from outside
	metadata streams.Metadata

	//changed for each segment
	streamID          storj.StreamID
	encryptedDataSize int64
	plainSize         int64
	encryptionParams  *EncryptionParams
	blockSize         int32
}

func (o *ObjectMeta) EncryptionParams(ctx context.Context, v EncryptionParams) error {
	o.encryptionParams = &v
	return nil
}

type StartObject struct {
	Bucket           string
	UnecryptedKey    string
	Expiration       time.Time
	EncryptionParams storj.EncryptionParameters
}

func (o *ObjectMeta) EncryptedWrite(ctx context.Context, i int, bytes []byte) error {
	return o.Output.EncryptedWrite(ctx, i, bytes)
}

func (o *ObjectMeta) EncryptedCommit(ctx context.Context) error {
	return o.Output.EncryptedCommit(ctx)
}

type EncryptedObjectLayer interface {
	BeginSegment(ctx context.Context, piecePrivateKey storj.PiecePrivateKey, limits []*pb.AddressedOrderLimit) error
	CommitSegment(ctx context.Context) ([]*pb.SegmentPieceUploadResult, error)
	EncryptedWrite(context.Context, int, []byte) error
	EncryptedCommit(context.Context) error
}

func (o *ObjectMeta) StartObject(ctx context.Context, info *StartObject) error {
	encPath, err := encryption.EncryptPathWithStoreCipher(info.Bucket, paths.NewUnencrypted(info.UnecryptedKey), o.encStore)
	if err != nil {
		return errs.Wrap(err)
	}

	o.beginObjectReq = &metaclient.BeginObjectParams{
		Bucket:               []byte(info.Bucket),
		EncryptedObjectKey:   []byte(encPath.Raw()),
		ExpiresAt:            info.Expiration,
		EncryptionParameters: info.EncryptionParams,
	}
	return nil
}

func (o *ObjectMeta) StartSegment(ctx context.Context, index int) error {
	beginSegment := &metaclient.BeginSegmentParams{
		//TODO: use real value
		MaxOrderLimit: 10000000,
		Position: metaclient.SegmentPosition{
			Index: int32(index),
		},
	}
	if o.beginObjectReq != nil {
		results, err := o.Metaclient.Batch(ctx, o.beginObjectReq, beginSegment)
		if err != nil {
			fmt.Printf("%++v", err)
			return errs.Wrap(err)
		}
		o.beginObjectReq = nil

		beginObject, err := results[0].BeginObject()
		if err != nil {
			return errs.Wrap(err)
		}
		o.streamID = beginObject.StreamID

		o.currentSegment, err = results[1].BeginSegment()
		if err != nil {
			return errs.Wrap(err)
		}
	}
	if o.commitSegment != nil {
		results, err := o.Metaclient.Batch(ctx, o.commitSegment, beginSegment)
		if err != nil {
			return errs.Wrap(err)
		}
		o.currentSegment, err = results[1].BeginSegment()
	}
	o.encryptedDataSize = 0
	o.plainSize = 0

	return o.Output.BeginSegment(ctx,
		o.currentSegment.PiecePrivateKey,
		o.currentSegment.Limits,
	)
}

var _ EncryptedWriterLayer = &ObjectMeta{}

func (o *ObjectMeta) EndSegment(ctx context.Context) error {
	results, err := o.Output.CommitSegment(ctx)
	if err != nil {
		return err
	}
	o.commitSegment = &metaclient.CommitSegmentParams{
		SegmentID:         o.currentSegment.SegmentID,
		SizeEncryptedData: o.encryptedDataSize,
		PlainSize:         o.plainSize,
		Encryption: metaclient.SegmentEncryption{
			EncryptedKeyNonce: *o.encryptionParams.encryptedKeyNonce,
			EncryptedKey:      *o.encryptionParams.encryptedKey,
		},
		//TODO
		UploadResult: results,
	}

	//we will execute it only later batching with the next request
	//but underlaying layer should have the opportunity to act now

	return err
}

func (o *ObjectMeta) EndObject(ctx context.Context) error {
	//TODO
	//metadataBytes, err := o.metadata.Metadata()
	//if err != nil {
	//	return errs.Wrap(err)
	//}
	metadataBytes := []byte{}

	// TODO: Do we still need to set SegmentsSize and LastSegmentSize
	// for backward compatibility with old uplinks?
	streamInfo, err := pb.Marshal(&pb.StreamInfo{
		//TODO: use the segment size
		//SegmentsSize:    s.segmentSize,
		LastSegmentSize: o.encryptedDataSize,
		Metadata:        metadataBytes,
	})
	if err != nil {
		return errs.Wrap(err)
	}

	// encrypt metadata with the content encryption key and zero nonce.
	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, o.encryptionParams.cipher, o.encryptionParams.contentKey, &storj.Nonce{})
	if err != nil {
		return errs.Wrap(err)
	}

	streamMeta := pb.StreamMeta{
		EncryptedStreamInfo: encryptedStreamInfo,
	}

	objectMetadata, err := pb.Marshal(&streamMeta)
	if err != nil {
		return errs.Wrap(err)
	}

	commitObject := &metaclient.CommitObjectParams{
		StreamID:          o.streamID,
		EncryptedMetadata: objectMetadata,
	}

	if o.commitSegment != nil {
		_, err := o.Metaclient.Batch(ctx, o.commitSegment, commitObject)
		if err != nil {
			return errs.Wrap(err)
		}
	}
	return nil
}
