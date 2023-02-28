// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package streams

import (
	"context"
	"crypto/rand"
	"io"
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams/pieceupload"
	"storj.io/uplink/private/storage/streams/segmentupload"
	"storj.io/uplink/private/storage/streams/splitter"
	"storj.io/uplink/private/storage/streams/streamupload"
)

// MetainfoUpload are the metainfo methods needed to upload a stream.
type MetainfoUpload interface {
	metaclient.Batcher
	RetryBeginSegmentPieces(ctx context.Context, params metaclient.RetryBeginSegmentPiecesParams) (metaclient.RetryBeginSegmentPiecesResponse, error)
	io.Closer
}

// Uploader uploads object or part streams.
type Uploader struct {
	metainfo             MetainfoUpload
	piecePutter          pieceupload.PiecePutter
	segmentSize          int64
	encStore             *encryption.Store
	encryptionParameters storj.EncryptionParameters
	inlineThreshold      int
	safetyMargin         int

	// The backend is fixed to the real backend in production but is overridden
	// for testing.
	backend uploaderBackend
}

// NewUploader constructs a new stream putter.
func NewUploader(metainfo MetainfoUpload, piecePutter pieceupload.PiecePutter, segmentSize int64, encStore *encryption.Store, encryptionParameters storj.EncryptionParameters, inlineThreshold, safetyMargin int) (*Uploader, error) {
	switch {
	case segmentSize <= 0:
		return nil, errs.New("segment size must be larger than 0")
	case encryptionParameters.BlockSize <= 0:
		return nil, errs.New("encryption block size must be larger than 0")
	case inlineThreshold <= 0:
		return nil, errs.New("inline threshold must be larger than 0")
	}
	return &Uploader{
		metainfo:             metainfo,
		piecePutter:          piecePutter,
		segmentSize:          segmentSize,
		encStore:             encStore,
		encryptionParameters: encryptionParameters,
		inlineThreshold:      inlineThreshold,
		safetyMargin:         safetyMargin,
		backend:              realUploaderBackend{},
	}, nil
}

// Close closes the underlying resources for the uploader.
func (u *Uploader) Close() error {
	return u.metainfo.Close()
}

// UploadObject starts an upload of an object to the given location. The object
// contents can be written to the returned upload, which can then be committed.
func (u *Uploader) UploadObject(ctx context.Context, bucket, unencryptedKey string, metadata Metadata, expiration time.Time, sched segmentupload.Scheduler) (_ *Upload, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	done := make(chan uploadResult, 1)

	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(unencryptedKey), u.encStore)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	encPath, err := encryption.EncryptPathWithStoreCipher(bucket, paths.NewUnencrypted(unencryptedKey), u.encStore)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	split, err := splitter.New(splitter.Options{
		Split:      u.segmentSize,
		Minimum:    int64(u.inlineThreshold),
		Params:     u.encryptionParameters,
		Key:        derivedKey,
		PartNumber: 0,
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	go func() {
		<-ctx.Done()
		split.Finish(ctx.Err())
	}()

	beginObject := &metaclient.BeginObjectParams{
		Bucket:               []byte(bucket),
		EncryptedObjectKey:   []byte(encPath.Raw()),
		ExpiresAt:            expiration,
		EncryptionParameters: u.encryptionParameters,
	}

	uploader := segmentUploader{metainfo: u.metainfo, piecePutter: u.piecePutter, sched: sched, safetyMargin: u.safetyMargin}

	encMeta := u.newEncryptedMetadata(metadata, derivedKey)

	go func() {
		info, err := u.backend.UploadObject(
			ctx,
			split,
			uploader,
			u.metainfo,
			beginObject,
			encMeta,
		)
		// On failure, we need to "finish" the splitter with an error so that
		// outstanding writes to the splitter fail, otherwise the writes will
		// block waiting for the upload to read the stream.
		if err != nil {
			split.Finish(errs.Combine(errs.New("upload failed"), err))
		}
		done <- uploadResult{info: info, err: err}
	}()

	return &Upload{
		split:  split,
		done:   done,
		cancel: cancel,
	}, nil
}

// UploadPart starts an upload of a part to the given location for the given
// multipart upload stream. The eTag is an optional channel is used to provide
// the eTag to be encrypted and included in the final segment of the part. The
// eTag should be  sent on the channel only after the contents of the part have
// been fully written to the returned upload, but before calling Commit.
func (u *Uploader) UploadPart(ctx context.Context, bucket, unencryptedKey string, streamID storj.StreamID, partNumber int32, eTag <-chan []byte, sched segmentupload.Scheduler) (_ *Upload, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	done := make(chan uploadResult, 1)

	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(unencryptedKey), u.encStore)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	split, err := splitter.New(splitter.Options{
		Split:      u.segmentSize,
		Minimum:    int64(u.inlineThreshold),
		Params:     u.encryptionParameters,
		Key:        derivedKey,
		PartNumber: partNumber,
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	go func() {
		<-ctx.Done()
		split.Finish(ctx.Err())
	}()

	uploader := segmentUploader{metainfo: u.metainfo, piecePutter: u.piecePutter, sched: sched, safetyMargin: u.safetyMargin}

	go func() {
		info, err := u.backend.UploadPart(
			ctx,
			split,
			uploader,
			u.metainfo,
			streamID,
			eTag,
		)
		// On failure, we need to "finish" the splitter with an error so that
		// outstanding writes to the splitter fail, otherwise the writes will
		// block waiting for the upload to read the stream.
		if err != nil {
			split.Finish(errs.Combine(errs.New("upload failed"), err))
		}
		done <- uploadResult{info: info, err: err}
	}()

	return &Upload{
		split:  split,
		done:   done,
		cancel: cancel,
	}, nil
}

func (u *Uploader) newEncryptedMetadata(metadata Metadata, derivedKey *storj.Key) streamupload.EncryptedMetadata {
	return &encryptedMetadata{
		metadata:    metadata,
		segmentSize: u.segmentSize,
		derivedKey:  derivedKey,
		cipherSuite: u.encryptionParameters.CipherSuite,
	}
}

type segmentUploader struct {
	metainfo     MetainfoUpload
	piecePutter  pieceupload.PiecePutter
	sched        segmentupload.Scheduler
	safetyMargin int
}

func (u segmentUploader) Begin(ctx context.Context, beginSegment *metaclient.BeginSegmentResponse, segment splitter.Segment) (streamupload.SegmentUpload, error) {
	return segmentupload.Begin(ctx, beginSegment, segment, limitsExchanger{u.metainfo}, u.piecePutter, u.sched, u.safetyMargin)
}

type limitsExchanger struct {
	metainfo MetainfoUpload
}

func (e limitsExchanger) ExchangeLimits(ctx context.Context, segmentID storj.SegmentID, pieceNumbers []int) (storj.SegmentID, []*pb.AddressedOrderLimit, error) {
	resp, err := e.metainfo.RetryBeginSegmentPieces(ctx, metaclient.RetryBeginSegmentPiecesParams{
		SegmentID:         segmentID,
		RetryPieceNumbers: pieceNumbers,
	})
	if err != nil {
		return nil, nil, err
	}
	return resp.SegmentID, resp.Limits, nil
}

type encryptedMetadata struct {
	metadata    Metadata
	segmentSize int64
	derivedKey  *storj.Key
	cipherSuite storj.CipherSuite
}

func (e *encryptedMetadata) EncryptedMetadata(lastSegmentSize int64) (data []byte, encKey *storj.EncryptedPrivateKey, nonce *storj.Nonce, err error) {
	metadataBytes, err := e.metadata.Metadata()
	if err != nil {
		return nil, nil, nil, err
	}

	streamInfo, err := pb.Marshal(&pb.StreamInfo{
		SegmentsSize:    e.segmentSize,
		LastSegmentSize: lastSegmentSize,
		Metadata:        metadataBytes,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	var metadataKey storj.Key
	if _, err := rand.Read(metadataKey[:]); err != nil {
		return nil, nil, nil, err
	}

	var encryptedMetadataKeyNonce storj.Nonce
	if _, err := rand.Read(encryptedMetadataKeyNonce[:]); err != nil {
		return nil, nil, nil, err
	}

	// encrypt the metadata key with the derived key and the random encrypted key nonce
	encryptedMetadataKey, err := encryption.EncryptKey(&metadataKey, e.cipherSuite, e.derivedKey, &encryptedMetadataKeyNonce)
	if err != nil {
		return nil, nil, nil, err
	}

	// encrypt the stream info with the metadata key and the zero nonce
	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, e.cipherSuite, &metadataKey, &storj.Nonce{})
	if err != nil {
		return nil, nil, nil, err
	}

	streamMeta, err := pb.Marshal(&pb.StreamMeta{
		EncryptedStreamInfo: encryptedStreamInfo,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	return streamMeta, &encryptedMetadataKey, &encryptedMetadataKeyNonce, nil
}

type uploaderBackend interface {
	UploadObject(ctx context.Context, segmentSource streamupload.SegmentSource, segmentUploader streamupload.SegmentUploader, miBatcher metaclient.Batcher, beginObject *metaclient.BeginObjectParams, encMeta streamupload.EncryptedMetadata) (streamupload.Info, error)
	UploadPart(ctx context.Context, segmentSource streamupload.SegmentSource, segmentUploader streamupload.SegmentUploader, miBatcher metaclient.Batcher, streamID storj.StreamID, eTagCh <-chan []byte) (streamupload.Info, error)
}

type realUploaderBackend struct{}

func (realUploaderBackend) UploadObject(ctx context.Context, segmentSource streamupload.SegmentSource, segmentUploader streamupload.SegmentUploader, miBatcher metaclient.Batcher, beginObject *metaclient.BeginObjectParams, encMeta streamupload.EncryptedMetadata) (streamupload.Info, error) {
	return streamupload.UploadObject(ctx, segmentSource, segmentUploader, miBatcher, beginObject, encMeta)
}

func (realUploaderBackend) UploadPart(ctx context.Context, segmentSource streamupload.SegmentSource, segmentUploader streamupload.SegmentUploader, miBatcher metaclient.Batcher, streamID storj.StreamID, eTagCh <-chan []byte) (streamupload.Info, error) {
	return streamupload.UploadPart(ctx, segmentSource, segmentUploader, miBatcher, streamID, eTagCh)
}
