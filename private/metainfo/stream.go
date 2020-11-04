// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import (
	"context"
	"errors"
	"time"

	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

var _ ReadOnlyStream = (*readonlyStream)(nil)

type readonlyStream struct {
	db *DB

	info storj.Object
}

func (stream *readonlyStream) Info() storj.Object { return stream.info }

func (stream *readonlyStream) SegmentsAt(ctx context.Context, byteOffset int64, limit int64) (infos []storj.Segment, more bool, err error) {
	defer mon.Task()(&ctx)(&err)

	if stream.info.FixedSegmentSize <= 0 {
		return nil, false, errors.New("not implemented")
	}

	index := byteOffset / stream.info.FixedSegmentSize
	return stream.Segments(ctx, index, limit)
}

func (stream *readonlyStream) segment(ctx context.Context, index int64) (segment storj.Segment, err error) {
	defer mon.Task()(&ctx)(&err)

	segment = storj.Segment{
		Index: index,
	}

	isLastSegment := segment.Index+1 == stream.info.SegmentCount
	if isLastSegment {
		index = -1
	}
	info, limits, err := stream.db.metainfo.DownloadSegment(ctx, DownloadSegmentParams{
		StreamID: stream.Info().ID,
		Position: storj.SegmentPosition{
			Index: int32(index),
		},
	})
	if err != nil {
		return segment, err
	}

	segment.Size = stream.info.Size
	segment.EncryptedKeyNonce = info.SegmentEncryption.EncryptedKeyNonce
	segment.EncryptedKey = info.SegmentEncryption.EncryptedKey

	streamKey, err := encryption.DeriveContentKey(stream.info.Bucket.Name, paths.NewUnencrypted(stream.info.Path), stream.db.encStore)
	if err != nil {
		return segment, err
	}

	contentKey, err := encryption.DecryptKey(segment.EncryptedKey, stream.info.EncryptionParameters.CipherSuite, streamKey, &segment.EncryptedKeyNonce)
	if err != nil {
		return segment, err
	}

	nonce := new(storj.Nonce)
	_, err = encryption.Increment(nonce, segment.Index+1)
	if err != nil {
		return segment, err
	}

	if len(info.EncryptedInlineData) != 0 || len(limits) == 0 {
		inline, err := encryption.Decrypt(info.EncryptedInlineData, stream.info.EncryptionParameters.CipherSuite, contentKey, nonce)
		if err != nil {
			return segment, err
		}
		segment.Inline = inline
	}

	return segment, nil
}

func (stream *readonlyStream) Segments(ctx context.Context, index int64, limit int64) (infos []storj.Segment, more bool, err error) {
	defer mon.Task()(&ctx)(&err)

	if index < 0 {
		return nil, false, errors.New("invalid argument")
	}
	if limit <= 0 {
		limit = defaultSegmentLimit
	}
	if index >= stream.info.SegmentCount {
		return nil, false, nil
	}

	infos = make([]storj.Segment, 0, limit)
	for ; index < stream.info.SegmentCount && limit > 0; index++ {
		limit--
		segment, err := stream.segment(ctx, index)
		if err != nil {
			return nil, false, err
		}
		infos = append(infos, segment)
	}

	more = index < stream.info.SegmentCount
	return infos, more, nil
}

// MutableStream is for manipulating stream information.
type MutableStream struct {
	db   *DB
	info storj.Object

	dynamic         bool
	dynamicMetadata SerializableMeta
	dynamicExpires  time.Time
}

// SerializableMeta is an interface for getting pb.SerializableMeta.
type SerializableMeta interface {
	Metadata() ([]byte, error)
}

// BucketName returns streams bucket name.
func (stream *MutableStream) BucketName() string { return stream.info.Bucket.Name }

// Path returns streams path.
func (stream *MutableStream) Path() string { return stream.info.Path }

// Info returns object info about the stream.
func (stream *MutableStream) Info() storj.Object { return stream.info }

// Expires returns stream expiration time.
func (stream *MutableStream) Expires() time.Time {
	if stream.dynamic {
		return stream.dynamicExpires
	}
	return stream.info.Expires
}

// Metadata returns metadata associated with the stream.
func (stream *MutableStream) Metadata() ([]byte, error) {
	if stream.dynamic {
		return stream.dynamicMetadata.Metadata()
	}

	if stream.info.ContentType != "" {
		if stream.info.Metadata == nil {
			stream.info.Metadata = make(map[string]string)
			stream.info.Metadata[contentTypeKey] = stream.info.ContentType
		} else if _, found := stream.info.Metadata[contentTypeKey]; !found {
			stream.info.Metadata[contentTypeKey] = stream.info.ContentType
		}
	}
	if stream.info.Metadata == nil {
		return []byte{}, nil
	}
	return pb.Marshal(&pb.SerializableMeta{
		UserDefined: stream.info.Metadata,
	})
}
