// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"io/ioutil"
	"time"

	"github.com/btcsuite/btcutil/base58"

	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metainfo"
	"storj.io/uplink/private/storage/streams"
)

// ErrStreamIDInvalid is returned when the stream ID is invalid.
var ErrStreamIDInvalid = errors.New("stream ID invalid")

// MultipartInfo contains information about multipart upload.
type MultipartInfo struct {
	// StreamID multipart upload identifier encoded with base58.
	StreamID string
}

// MultipartUploadOptions contains additional options for multipart upload.
type MultipartUploadOptions UploadOptions

// MultipartObjectOptions options for committing object.
type MultipartObjectOptions struct {
	CustomMetadata CustomMetadata
}

// ListPartsResult contains the result of a list object parts query.
type ListPartsResult struct {
	Items []PartInfo
	More  bool
}

// PartInfo contains information about uploaded part.
type PartInfo struct {
	PartNumber   int
	Size         int64
	LastModified time.Time
}

// NewMultipartUpload begins new multipart upload.
// Potential name: BeginObject.
func (project *Project) NewMultipartUpload(ctx context.Context, bucket, key string, options *MultipartUploadOptions) (info MultipartInfo, err error) {
	defer mon.Func().RestartTrace(&ctx)(&err)

	if bucket == "" {
		return MultipartInfo{}, ErrBucketNameInvalid
	}
	if key == "" {
		return MultipartInfo{}, ErrObjectKeyInvalid
	}

	if options == nil {
		options = &MultipartUploadOptions{}
	}

	encStore := project.access.encAccess.Store
	encPath, err := encryption.EncryptPathWithStoreCipher(bucket, paths.NewUnencrypted(key), encStore)
	if err != nil {
		return MultipartInfo{}, packageError.Wrap(err)
	}

	response, err := project.metainfo.BeginObject(ctx, metainfo.BeginObjectParams{
		Bucket:               []byte(bucket),
		EncryptedPath:        []byte(encPath.Raw()),
		ExpiresAt:            options.Expires,
		EncryptionParameters: project.encryption,
	})
	if err != nil {
		return MultipartInfo{}, convertKnownErrors(err, bucket, key)
	}

	encodedStreamID := base58.CheckEncode(response.StreamID[:], 1)
	return MultipartInfo{
		StreamID: encodedStreamID,
	}, nil
}

// PutObjectPart uploads a part.
func (project *Project) PutObjectPart(ctx context.Context, bucket, key string, streamID string, partNumber int, data io.Reader) (info PartInfo, err error) {
	defer mon.Func().RestartTrace(&ctx)(&err)

	// TODO
	// * use Batch to combine requests
	// * how pass expiration time
	// * most probably we need to adjust content nonce generation

	switch {
	case bucket == "":
		return PartInfo{}, errwrapf("%w (%q)", ErrBucketNameInvalid, bucket)
	case key == "":
		return PartInfo{}, errwrapf("%w (%q)", ErrObjectKeyInvalid, key)
	case streamID == "":
		return PartInfo{}, ErrStreamIDInvalid
	case partNumber < 1:
		return PartInfo{}, packageError.New("partNumber should be larger than 0")
	}

	decodedStreamID, version, err := base58.CheckDecode(streamID)
	if err != nil || version != 1 {
		return PartInfo{}, packageError.New("invalid streamID format")
	}

	var (
		currentSegment int64
		streamSize     int64
		contentKey     storj.Key
		encryptedKey   []byte
		keyNonce       storj.Nonce
	)

	maxEncryptedSegmentSize, err := encryption.CalcEncryptedSize(project.segmentSize, project.encryption)
	if err != nil {
		return PartInfo{}, packageError.Wrap(err)
	}

	encStore := project.access.encAccess.Store
	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(key), encStore)
	if err != nil {
		return PartInfo{}, packageError.Wrap(err)
	}

	eofReader := streams.NewEOFReader(data)
	for !eofReader.IsEOF() && !eofReader.HasError() {
		// generate random key for encrypting the segment's content
		_, err := rand.Read(contentKey[:])
		if err != nil {
			return PartInfo{}, packageError.Wrap(err)
		}

		// Initialize the content nonce with the current total segment incremented
		// by 1 because at this moment the next segment has not been already
		// uploaded.
		// The increment by 1 is to avoid nonce reuse with the metadata encryption,
		// which is encrypted with the zero nonce.
		contentNonce := storj.Nonce{}
		_, err = encryption.Increment(&contentNonce, currentSegment+1)
		if err != nil {
			return PartInfo{}, packageError.Wrap(err)
		}

		// generate random nonce for encrypting the content key
		_, err = rand.Read(keyNonce[:])
		if err != nil {
			return PartInfo{}, packageError.Wrap(err)
		}

		encryptedKey, err = encryption.EncryptKey(&contentKey, project.encryption.CipherSuite, derivedKey, &keyNonce)
		if err != nil {
			return PartInfo{}, packageError.Wrap(err)
		}

		sizeReader := streams.SizeReader(eofReader)
		segmentReader := io.LimitReader(sizeReader, project.segmentSize)
		segmentEncryption := storj.SegmentEncryption{}
		if project.encryption.CipherSuite != storj.EncNull {
			segmentEncryption = storj.SegmentEncryption{
				EncryptedKey:      encryptedKey,
				EncryptedKeyNonce: keyNonce,
			}
		}

		encrypter, err := encryption.NewEncrypter(project.encryption.CipherSuite, &contentKey, &contentNonce, int(project.encryption.BlockSize))
		if err != nil {
			return PartInfo{}, packageError.Wrap(err)
		}

		paddedReader := encryption.PadReader(ioutil.NopCloser(segmentReader), encrypter.InBlockSize())
		transformedReader := encryption.TransformReader(paddedReader, encrypter, 0)

		response, err := project.metainfo.BeginSegment(ctx, metainfo.BeginSegmentParams{
			StreamID:      decodedStreamID,
			MaxOrderLimit: maxEncryptedSegmentSize,
			Position: storj.SegmentPosition{
				PartNumber: int32(partNumber),
				Index:      int32(currentSegment),
			},
		})
		if err != nil {
			return PartInfo{}, convertKnownErrors(err, bucket, key)
		}

		encSizedReader := streams.SizeReader(transformedReader)

		// TODO handle expiration
		expiration := time.Time{}
		uploadResults, err := project.ec.PutSingleResult(ctx, response.Limits, response.PiecePrivateKey,
			response.RedundancyStrategy, encSizedReader, expiration)
		if err != nil {
			return PartInfo{}, packageError.Wrap(err)
		}

		plainSegmentSize := sizeReader.Size()
		if plainSegmentSize > 0 {
			err = project.metainfo.CommitSegment(ctx, metainfo.CommitSegmentParams{
				SegmentID:         response.SegmentID,
				SizeEncryptedData: encSizedReader.Size(),
				PlainSize:         plainSegmentSize,
				Encryption:        segmentEncryption,
				UploadResult:      uploadResults,
			})
			if err != nil {
				return PartInfo{}, convertKnownErrors(err, bucket, key)
			}
		}

		streamSize += plainSegmentSize
		currentSegment++
	}

	if streamSize == 0 {
		return PartInfo{}, packageError.New("input data reader was empty")
	}

	return PartInfo{
		PartNumber: partNumber,
		Size:       streamSize,
	}, nil
}

// CompleteMultipartUpload commits object after uploading all parts.
// TODO should we accept parameter with info uploaded parts.
func (project *Project) CompleteMultipartUpload(ctx context.Context, bucket, key, streamID string, opts *MultipartObjectOptions) (obj *Object, err error) {
	defer mon.Func().RestartTrace(&ctx)(&err)

	if bucket == "" {
		return nil, errwrapf("%w (%q)", ErrBucketNameInvalid, bucket)
	}
	if key == "" {
		return nil, errwrapf("%w (%q)", ErrObjectKeyInvalid, key)
	}

	if streamID == "" {
		return nil, packageError.New("streamID is missing")
	}

	decodedStreamID, version, err := base58.CheckDecode(streamID)
	if err != nil || version != 1 {
		return nil, errors.New("invalid streamID format")
	}

	if opts == nil {
		opts = &MultipartObjectOptions{}
	}

	id, err := storj.StreamIDFromBytes(decodedStreamID)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	metadataBytes, err := pb.Marshal(&pb.SerializableMeta{
		UserDefined: opts.CustomMetadata.Clone(),
	})
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	streamInfo, err := pb.Marshal(&pb.StreamInfo{
		Metadata: metadataBytes,
	})
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	encStore := project.access.encAccess.Store
	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(key), encStore)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	var metadataKey storj.Key
	// generate random key for encrypting the segment's content
	_, err = rand.Read(metadataKey[:])
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	var encryptedKeyNonce storj.Nonce
	// generate random nonce for encrypting the content key
	_, err = rand.Read(encryptedKeyNonce[:])
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	encryptedKey, err := encryption.EncryptKey(&metadataKey, project.encryption.CipherSuite, derivedKey, &encryptedKeyNonce)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	// encrypt metadata with the content encryption key and zero nonce.
	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, project.encryption.CipherSuite, &metadataKey, &storj.Nonce{})
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	// TODO should we commit StreamMeta or commit only encrypted StreamInfo
	streamMetaBytes, err := pb.Marshal(&pb.StreamMeta{
		EncryptedStreamInfo: encryptedStreamInfo,
	})
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	err = project.metainfo.CommitObject(ctx, metainfo.CommitObjectParams{
		StreamID:                      id,
		EncryptedMetadata:             streamMetaBytes,
		EncryptedMetadataEncryptedKey: encryptedKey,
		EncryptedMetadataNonce:        encryptedKeyNonce,
	})
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}

	// TODO return object after committing
	return &Object{}, nil
}

// AbortMultipartUpload aborts a multipart upload.
// TODO: implement dedicated metainfo methods to handle aborting correctly.
func (project *Project) AbortMultipartUpload(ctx context.Context, bucket, key, streamID string) (err error) {
	defer mon.Func().RestartTrace(&ctx)(&err)
	if bucket == "" {
		return ErrBucketNameInvalid
	}
	if key == "" {
		return ErrObjectKeyInvalid
	}
	if streamID == "" {
		return ErrStreamIDInvalid
	}

	encStore := project.access.encAccess.Store
	encPath, err := encryption.EncryptPathWithStoreCipher(bucket, paths.NewUnencrypted(key), encStore)
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}

	_, err = project.metainfo.BeginDeleteObject(ctx, metainfo.BeginDeleteObjectParams{
		Bucket:        []byte(bucket),
		EncryptedPath: []byte(encPath.Raw()),
	})
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}
	return nil
}

// ListParts lists  the  parts  that have been uploaded for a specific multipart upload.
// TODO: For now, maxParts is not correctly handled as the limit is applied to the number of segments we retrieve.
func (project *Project) ListParts(ctx context.Context, bucket, key, streamID string, partCursor, maxParts int) (infos ListPartsResult, err error) {
	defer mon.Func().RestartTrace(&ctx)(&err)

	if bucket == "" {
		return ListPartsResult{}, ErrBucketNameInvalid
	}

	if key == "" {
		return ListPartsResult{}, ErrObjectKeyInvalid
	}

	decodedStreamID, version, err := base58.CheckDecode(streamID)
	if err != nil || version != 1 {
		return ListPartsResult{}, errors.New("invalid streamID format")
	}

	id, err := storj.StreamIDFromBytes(decodedStreamID)
	if err != nil {
		return ListPartsResult{}, packageError.Wrap(err)
	}

	listResult, err := project.metainfo.ListSegments(ctx, metainfo.ListSegmentsParams{
		StreamID: id,
		Cursor:   storj.SegmentPosition{PartNumber: int32(partCursor), Index: 0},
		Limit:    int32(maxParts), // TODO: handle limit correctly
	})

	if err != nil {
		return ListPartsResult{}, convertKnownErrors(err, bucket, key)
	}

	partInfosMap := make(map[int]PartInfo)

	for _, item := range listResult.Items {
		partNumber := int(item.Position.PartNumber)
		_, exists := partInfosMap[partNumber]
		if !exists {
			partInfosMap[partNumber] = PartInfo{
				PartNumber:   partNumber,
				LastModified: time.Now(), // TODO: handle last modified time correctly
			}
		}
	}

	partInfos := make([]PartInfo, 0, len(partInfosMap))

	for _, partInfo := range partInfosMap {
		partInfos = append(partInfos, partInfo)
	}

	return ListPartsResult{Items: partInfos, More: listResult.More}, nil
}
