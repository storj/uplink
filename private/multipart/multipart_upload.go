// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package multipart

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"sort"
	"strings"
	"time"
	_ "unsafe" // for go:linkname

	"github.com/btcsuite/btcutil/base58"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/etag"
	"storj.io/uplink/private/metainfo"
	"storj.io/uplink/private/storage/streams"
)

// TODO should be in sync with uplink.maxInlineSize.
const maxInlineSize = 4096 // 4KiB

var mon = monkit.Package()

var packageError = errs.Class("multipart")

// ErrStreamIDInvalid is returned when the stream ID is invalid.
var ErrStreamIDInvalid = errors.New("stream ID invalid")

// Object object metadata.
type Object struct {
	// StreamID multipart upload identifier encoded with base58.
	StreamID string

	uplink.Object
}

// Info contains information about multipart upload.
type Info struct {
	// StreamID multipart upload identifier encoded with base58.
	StreamID string
}

// UploadOptions contains additional options for multipart upload.
type UploadOptions uplink.UploadOptions

// ObjectOptions options for committing object.
type ObjectOptions struct {
	CustomMetadata uplink.CustomMetadata
}

// ListObjectPartsResult contains the result of a list object parts query.
type ListObjectPartsResult struct {
	Items []PartInfo
	More  bool
}

// PartInfo contains information about uploaded part.
type PartInfo struct {
	PartNumber   int
	Size         int64
	LastModified time.Time
	ETag         []byte
}

// NewMultipartUpload begins new multipart upload.
// Potential name: BeginObject.
func NewMultipartUpload(ctx context.Context, project *uplink.Project, bucket, key string, options *UploadOptions) (info Info, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket == "" {
		return Info{}, uplink.ErrBucketNameInvalid
	}
	if key == "" {
		return Info{}, uplink.ErrObjectKeyInvalid
	}

	if options == nil {
		options = &UploadOptions{}
	}

	encPath, err := encryptPath(project, bucket, key)
	if err != nil {
		return Info{}, packageError.Wrap(err)
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return Info{}, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	response, err := metainfoClient.BeginObject(ctx, metainfo.BeginObjectParams{
		Bucket:               []byte(bucket),
		EncryptedPath:        []byte(encPath.Raw()),
		ExpiresAt:            options.Expires,
		EncryptionParameters: encryptionParameters(project),
	})
	if err != nil {
		return Info{}, convertKnownErrors(err, bucket, key)
	}

	encodedStreamID := base58.CheckEncode(response.StreamID[:], 1)
	return Info{
		StreamID: encodedStreamID,
	}, nil
}

// PutObjectPart uploads a part.
func PutObjectPart(ctx context.Context, project *uplink.Project, bucket, key string, streamID string, partNumber int, reader etag.Reader) (info PartInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO
	// * use Batch to combine requests
	// * how pass expiration time

	switch {
	case bucket == "":
		return PartInfo{}, errwrapf("%w (%q)", uplink.ErrBucketNameInvalid, bucket)
	case key == "":
		return PartInfo{}, errwrapf("%w (%q)", uplink.ErrObjectKeyInvalid, key)
	case streamID == "":
		return PartInfo{}, packageError.Wrap(ErrStreamIDInvalid)
	case partNumber < 0:
		return PartInfo{}, packageError.New("partNumber should be positive")
	case partNumber >= math.MaxInt32:
		return PartInfo{}, packageError.New("partNumber should be less than max(int32)")
	}

	decodedStreamID, version, err := base58.CheckDecode(streamID)
	if err != nil || version != 1 {
		return PartInfo{}, packageError.Wrap(ErrStreamIDInvalid)
	}

	var (
		currentSegment int64
		streamSize     int64
		contentKey     storj.Key
		encryptedKey   []byte
		keyNonce       storj.Nonce
	)

	encryptionParameters := encryptionParameters(project)
	maxEncryptedSegmentSize, err := encryption.CalcEncryptedSize(segmentSize(project), encryptionParameters)
	if err != nil {
		return PartInfo{}, packageError.Wrap(err)
	}

	derivedKey, err := deriveContentKey(project, bucket, key)
	if err != nil {
		return PartInfo{}, packageError.Wrap(err)
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return PartInfo{}, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	eofReader := streams.NewEOFReader(reader)
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
		_, err = encryption.Increment(&contentNonce, (int64(partNumber)<<32)|(currentSegment+1))
		if err != nil {
			return PartInfo{}, packageError.Wrap(err)
		}

		// generate random nonce for encrypting the content key
		_, err = rand.Read(keyNonce[:])
		if err != nil {
			return PartInfo{}, packageError.Wrap(err)
		}

		encryptedKey, err = encryption.EncryptKey(&contentKey, encryptionParameters.CipherSuite, derivedKey, &keyNonce)
		if err != nil {
			return PartInfo{}, packageError.Wrap(err)
		}

		sizeReader := streams.SizeReader(eofReader)
		segmentReader := io.LimitReader(sizeReader, segmentSize(project))
		peekReader := streams.NewPeekThresholdReader(segmentReader)
		// If the data is larger than the inline threshold size, then it will be a remote segment
		isRemote, err := peekReader.IsLargerThan(maxInlineSize)
		if err != nil {
			return PartInfo{}, packageError.Wrap(err)
		}

		segmentEncryption := metainfo.SegmentEncryption{}
		if encryptionParameters.CipherSuite != storj.EncNull {
			segmentEncryption = metainfo.SegmentEncryption{
				EncryptedKey:      encryptedKey,
				EncryptedKeyNonce: keyNonce,
			}
		}

		if isRemote {
			encrypter, err := encryption.NewEncrypter(encryptionParameters.CipherSuite, &contentKey, &contentNonce, int(encryptionParameters.BlockSize))
			if err != nil {
				return PartInfo{}, packageError.Wrap(err)
			}

			paddedReader := encryption.PadReader(ioutil.NopCloser(peekReader), encrypter.InBlockSize())
			transformedReader := encryption.TransformReader(paddedReader, encrypter, 0)

			response, err := metainfoClient.BeginSegment(ctx, metainfo.BeginSegmentParams{
				StreamID:      decodedStreamID,
				MaxOrderLimit: maxEncryptedSegmentSize,
				Position: metainfo.SegmentPosition{
					PartNumber: int32(partNumber),
					Index:      int32(currentSegment),
				},
			})
			if err != nil {
				return PartInfo{}, convertKnownErrors(err, bucket, key)
			}

			encSizedReader := streams.SizeReader(transformedReader)
			uploadResults, err := ecPutSingleResult(ctx, project, response.Limits, response.PiecePrivateKey,
				response.RedundancyStrategy, encSizedReader)
			if err != nil {
				return PartInfo{}, packageError.Wrap(err)
			}

			plainSegmentSize := sizeReader.Size()
			if plainSegmentSize > 0 {
				encryptedETag, err := encryptETag(reader.CurrentETag(), encryptionParameters, &contentKey)
				if err != nil {
					return PartInfo{}, packageError.Wrap(err)
				}

				err = metainfoClient.CommitSegment(ctx, metainfo.CommitSegmentParams{
					SegmentID:         response.SegmentID,
					SizeEncryptedData: encSizedReader.Size(),
					PlainSize:         plainSegmentSize,
					EncryptedTag:      encryptedETag,
					Encryption:        segmentEncryption,
					UploadResult:      uploadResults,
				})
				if err != nil {
					return PartInfo{}, convertKnownErrors(err, bucket, key)
				}
			}
		} else {
			data, err := ioutil.ReadAll(peekReader)
			if err != nil {
				return PartInfo{}, packageError.Wrap(err)
			}

			if len(data) > 0 {
				cipherData, err := encryption.Encrypt(data, encryptionParameters.CipherSuite, &contentKey, &contentNonce)
				if err != nil {
					return PartInfo{}, packageError.Wrap(err)
				}

				encryptedETag, err := encryptETag(reader.CurrentETag(), encryptionParameters, &contentKey)
				if err != nil {
					return PartInfo{}, packageError.Wrap(err)
				}

				err = metainfoClient.MakeInlineSegment(ctx, metainfo.MakeInlineSegmentParams{
					StreamID: decodedStreamID,
					Position: metainfo.SegmentPosition{
						PartNumber: int32(partNumber),
						Index:      int32(currentSegment),
					},
					Encryption:          segmentEncryption,
					EncryptedInlineData: cipherData,
					PlainSize:           int64(len(data)),
					EncryptedTag:        encryptedETag,
				})
				if err != nil {
					return PartInfo{}, packageError.Wrap(err)
				}
			}
		}
		streamSize += sizeReader.Size()
		currentSegment++
	}

	if streamSize == 0 {
		return PartInfo{}, packageError.New("input data reader was empty")
	}

	return PartInfo{
		PartNumber: partNumber,
		Size:       streamSize,
		ETag:       reader.CurrentETag(),
	}, nil
}

// CompleteMultipartUpload commits object after uploading all parts.
// TODO should we accept parameter with info uploaded parts.
func CompleteMultipartUpload(ctx context.Context, project *uplink.Project, bucket, key, streamID string, opts *ObjectOptions) (obj *uplink.Object, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket == "" {
		return nil, errwrapf("%w (%q)", uplink.ErrBucketNameInvalid, bucket)
	}
	if key == "" {
		return nil, errwrapf("%w (%q)", uplink.ErrObjectKeyInvalid, key)
	}

	if streamID == "" {
		return nil, packageError.Wrap(ErrStreamIDInvalid)
	}

	decodedStreamID, version, err := base58.CheckDecode(streamID)
	if err != nil || version != 1 {
		return nil, packageError.Wrap(ErrStreamIDInvalid)
	}

	if opts == nil {
		opts = &ObjectOptions{}
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

	derivedKey, err := deriveContentKey(project, bucket, key)
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

	encryptionParameters := encryptionParameters(project)
	encryptedKey, err := encryption.EncryptKey(&metadataKey, encryptionParameters.CipherSuite, derivedKey, &encryptedKeyNonce)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	// encrypt metadata with the content encryption key and zero nonce.
	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, encryptionParameters.CipherSuite, &metadataKey, &storj.Nonce{})
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

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	err = metainfoClient.CommitObject(ctx, metainfo.CommitObjectParams{
		StreamID:                      id,
		EncryptedMetadata:             streamMetaBytes,
		EncryptedMetadataEncryptedKey: encryptedKey,
		EncryptedMetadataNonce:        encryptedKeyNonce,
	})
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}

	// TODO return object after committing
	return &uplink.Object{}, nil
}

// AbortMultipartUpload aborts a multipart upload.
// TODO: implement dedicated metainfo methods to handle aborting correctly.
func AbortMultipartUpload(ctx context.Context, project *uplink.Project, bucket, key, streamID string) (err error) {
	defer mon.Task()(&ctx)(&err)
	if bucket == "" {
		return uplink.ErrBucketNameInvalid
	}
	if key == "" {
		return uplink.ErrObjectKeyInvalid
	}
	if streamID == "" {
		return packageError.Wrap(ErrStreamIDInvalid)
	}

	decodedStreamID, version, err := base58.CheckDecode(streamID)
	if err != nil || version != 1 {
		return packageError.Wrap(ErrStreamIDInvalid)
	}

	id, err := storj.StreamIDFromBytes(decodedStreamID)
	if err != nil {
		return packageError.Wrap(err)
	}

	encPath, err := encryptPath(project, bucket, key)
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	_, err = metainfoClient.BeginDeleteObject(ctx, metainfo.BeginDeleteObjectParams{
		Bucket:        []byte(bucket),
		EncryptedPath: []byte(encPath.Raw()),
		Version:       1,
		StreamID:      id,
		Status:        int32(pb.Object_UPLOADING),
	})
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}
	return nil
}

// ListObjectParts lists the parts in position order that have been uploaded for a specific multipart upload.
//
// BUG: For now, maxParts is not correctly handled as the limit is applied to the number of segments we retrieve.
func ListObjectParts(ctx context.Context, project *uplink.Project, bucket, key, streamID string, partCursor, maxParts int) (infos ListObjectPartsResult, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket == "" {
		return ListObjectPartsResult{}, uplink.ErrBucketNameInvalid
	}

	if key == "" {
		return ListObjectPartsResult{}, uplink.ErrObjectKeyInvalid
	}

	decodedStreamID, version, err := base58.CheckDecode(streamID)
	if err != nil || version != 1 {
		return ListObjectPartsResult{}, packageError.Wrap(ErrStreamIDInvalid)
	}

	id, err := storj.StreamIDFromBytes(decodedStreamID)
	if err != nil {
		return ListObjectPartsResult{}, packageError.Wrap(err)
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return ListObjectPartsResult{}, convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	listResult, err := metainfoClient.ListSegments(ctx, metainfo.ListSegmentsParams{
		StreamID: id,
		Cursor:   metainfo.SegmentPosition{PartNumber: int32(partCursor), Index: 0},
		Limit:    int32(maxParts), // TODO: handle limit correctly
	})

	if err != nil {
		return ListObjectPartsResult{}, convertKnownErrors(err, bucket, key)
	}

	partInfosMap := make(map[int]*PartInfo)

	for _, item := range listResult.Items {
		etag, err := decryptETag(project, bucket, key, listResult.EncryptionParameters, item)
		if err != nil {
			return ListObjectPartsResult{}, convertKnownErrors(err, bucket, key)
		}

		partNumber := int(item.Position.PartNumber)
		_, exists := partInfosMap[partNumber]
		if !exists {
			partInfosMap[partNumber] = &PartInfo{
				PartNumber:   partNumber,
				Size:         item.PlainSize,
				LastModified: item.CreatedAt,
				ETag:         etag,
			}
		} else {
			partInfosMap[partNumber].Size += item.PlainSize
			if item.CreatedAt.After(partInfosMap[partNumber].LastModified) {
				partInfosMap[partNumber].LastModified = item.CreatedAt
			}
			// The satellite returns the segments ordered by position. So it is
			// OK to just overwrite the ETag with the one from the next segment.
			// Eventually, the map will contain the ETag of the last segment,
			// which is the part's ETag.
			partInfosMap[partNumber].ETag = etag
		}
	}

	partInfos := make([]PartInfo, 0, len(partInfosMap))
	for _, partInfo := range partInfosMap {
		partInfos = append(partInfos, *partInfo)
	}
	sort.Slice(partInfos, func(i, k int) bool {
		return partInfos[i].PartNumber < partInfos[k].PartNumber
	})

	return ListObjectPartsResult{Items: partInfos, More: listResult.More}, nil
}

// ListMultipartUploadsOptions defines multipart uploads listing options.
type ListMultipartUploadsOptions struct {
	// Prefix allows to filter objects by a key prefix. If not empty, it must end with slash.
	Prefix string
	// Cursor sets the starting position of the iterator. The first item listed will be the one after the cursor.
	Cursor string
	// Recursive iterates the objects without collapsing prefixes.
	Recursive bool

	// TODO: Do we need System and Custom flags for listing pending multipart uploads?
	// System includes SystemMetadata in the results.
	System bool
	// Custom includes CustomMetadata in the results.
	Custom bool
}

// ListMultipartUploads returns an iterator over the multipart uploads.
func ListMultipartUploads(ctx context.Context, project *uplink.Project, bucket string, options *ListMultipartUploadsOptions) *UploadIterator {
	defer mon.Task()(&ctx)(nil)

	opts := metainfo.ListOptions{
		Direction: metainfo.After,
		Status:    int32(pb.Object_UPLOADING), // TODO: define object status constants in storj package?
	}

	if options != nil {
		opts.Prefix = options.Prefix
		opts.Cursor = options.Cursor
		opts.Recursive = options.Recursive
	}

	objects := UploadIterator{
		ctx:         ctx,
		project:     project,
		bucket:      metainfo.Bucket{Name: bucket},
		options:     opts,
		listObjects: listObjects,
	}

	if options != nil {
		objects.multipartOptions = *options
	}

	return &objects
}

// ListPendingObjectStreams returns an iterator over the multipart uploads.
func ListPendingObjectStreams(ctx context.Context, project *uplink.Project, bucket, objectKey string, options *ListMultipartUploadsOptions) *UploadIterator {
	defer mon.Task()(&ctx)(nil)

	opts := metainfo.ListOptions{
		Direction: metainfo.After,
	}

	if options != nil {
		opts.Cursor = options.Cursor
	}

	opts.Prefix = objectKey

	objects := UploadIterator{
		ctx:         ctx,
		project:     project,
		bucket:      metainfo.Bucket{Name: bucket},
		options:     opts,
		listObjects: listPendingObjectStreams,
	}
	if options != nil {
		objects.multipartOptions = *options
	}
	return &objects
}

// UploadIterator is an iterator over a collection of objects or prefixes.
type UploadIterator struct {
	ctx              context.Context
	project          *uplink.Project
	bucket           metainfo.Bucket
	options          metainfo.ListOptions
	multipartOptions ListMultipartUploadsOptions
	list             *metainfo.ObjectList
	position         int
	completed        bool
	err              error
	listObjects      func(tx context.Context, db *metainfo.DB, bucket string, options metainfo.ListOptions) (metainfo.ObjectList, error)
}

func listObjects(ctx context.Context, db *metainfo.DB, bucket string, options metainfo.ListOptions) (metainfo.ObjectList, error) {
	return db.ListObjects(ctx, bucket, options)
}

func listPendingObjectStreams(ctx context.Context, db *metainfo.DB, bucket string, options metainfo.ListOptions) (metainfo.ObjectList, error) {
	return db.ListPendingObjectStreams(ctx, bucket, options)
}

// Next prepares next Object for reading.
// It returns false if the end of the iteration is reached and there are no more uploads, or if there is an error.
func (uploads *UploadIterator) Next() bool {
	if uploads.err != nil {
		uploads.completed = true
		return false
	}

	if uploads.list == nil {
		more := uploads.loadNext()
		uploads.completed = !more
		return more
	}

	if uploads.position >= len(uploads.list.Items)-1 {
		if !uploads.list.More {
			uploads.completed = true
			return false
		}
		more := uploads.loadNext()
		uploads.completed = !more
		return more
	}

	uploads.position++

	return true
}

func (uploads *UploadIterator) loadNext() bool {
	ok, err := uploads.tryLoadNext()
	if err != nil {
		uploads.err = err
		return false
	}
	return ok
}

func (uploads *UploadIterator) tryLoadNext() (ok bool, err error) {
	db, err := dialMetainfoDB(uploads.ctx, uploads.project)
	if err != nil {
		return false, convertKnownErrors(err, uploads.bucket.Name, "")
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	list, err := uploads.listObjects(uploads.ctx, db, uploads.bucket.Name, uploads.options)
	if err != nil {
		return false, convertKnownErrors(err, uploads.bucket.Name, "")
	}
	uploads.list = &list
	if list.More {
		uploads.options = uploads.options.NextPage(list)
	}
	uploads.position = 0
	return len(list.Items) > 0, nil
}

// Err returns error, if one happened during iteration.
func (uploads *UploadIterator) Err() error {
	return packageError.Wrap(uploads.err)
}

// Item returns the current object in the iterator.
func (uploads *UploadIterator) Item() *Object {
	item := uploads.item()
	if item == nil {
		return nil
	}

	key := item.Path
	if len(uploads.options.Prefix) > 0 && strings.HasSuffix(uploads.options.Prefix, "/") {
		key = uploads.options.Prefix + item.Path
	}

	obj := Object{
		Object: uplink.Object{
			Key:      key,
			IsPrefix: item.IsPrefix,
		},
		StreamID: base58.CheckEncode(item.Stream.ID, 1),
	}

	// TODO: Make this filtering on the satellite
	if uploads.multipartOptions.System {
		obj.System = uplink.SystemMetadata{
			Created:       item.Created,
			Expires:       item.Expires,
			ContentLength: item.Size,
		}
	}

	// TODO: Make this filtering on the satellite
	if uploads.multipartOptions.Custom {
		obj.Custom = item.Metadata
	}

	return &obj
}

func (uploads *UploadIterator) item() *metainfo.Object {
	if uploads.completed {
		return nil
	}

	if uploads.err != nil {
		return nil
	}

	if uploads.list == nil {
		return nil
	}

	if len(uploads.list.Items) == 0 {
		return nil
	}

	return &uploads.list.Items[uploads.position]
}

func errwrapf(format string, err error, args ...interface{}) error {
	var all []interface{}
	all = append(all, err)
	all = append(all, args...)
	return packageError.Wrap(fmt.Errorf(format, all...))
}

func deriveETagKey(key *storj.Key) (*storj.Key, error) {
	return encryption.DeriveKey(key, "storj-etag-v1")
}

func encryptETag(etag []byte, encryptionParameters storj.EncryptionParameters, contentKey *storj.Key) ([]byte, error) {
	// Derive another key from the randomly generated content key to encrypt
	// the segment's ETag.
	etagKey, err := deriveETagKey(contentKey)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	encryptedETag, err := encryption.Encrypt(etag, encryptionParameters.CipherSuite, etagKey, &storj.Nonce{})
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	return encryptedETag, nil
}

func decryptETag(project *uplink.Project, bucket, key string, encryptionParameters storj.EncryptionParameters, segment metainfo.SegmentListItem) ([]byte, error) {
	if segment.EncryptedETag == nil {
		return nil, nil
	}

	derivedKey, err := deriveContentKey(project, bucket, key)
	if err != nil {
		return nil, err
	}

	contentKey, err := encryption.DecryptKey(segment.EncryptedKey, encryptionParameters.CipherSuite, derivedKey, &segment.EncryptedKeyNonce)
	if err != nil {
		return nil, err
	}

	// Derive another key from the randomly generated content key to decrypt
	// the segment's ETag.
	etagKey, err := deriveETagKey(contentKey)
	if err != nil {
		return nil, err
	}

	return encryption.Decrypt(segment.EncryptedETag, encryptionParameters.CipherSuite, etagKey, &storj.Nonce{})
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoClient storj.io/uplink.dialMetainfoClient
func dialMetainfoClient(ctx context.Context, project *uplink.Project) (*metainfo.Client, error)

//go:linkname dialMetainfoDB storj.io/uplink.dialMetainfoDB
func dialMetainfoDB(ctx context.Context, project *uplink.Project) (_ *metainfo.DB, err error)

//go:linkname encryptionParameters storj.io/uplink.encryptionParameters
func encryptionParameters(project *uplink.Project) storj.EncryptionParameters

//go:linkname segmentSize storj.io/uplink.segmentSize
func segmentSize(project *uplink.Project) int64

//go:linkname encryptPath storj.io/uplink.encryptPath
func encryptPath(project *uplink.Project, bucket, key string) (paths.Encrypted, error)

//go:linkname deriveContentKey storj.io/uplink.deriveContentKey
func deriveContentKey(project *uplink.Project, bucket, key string) (*storj.Key, error)

//go:linkname ecPutSingleResult storj.io/uplink.ecPutSingleResult
func ecPutSingleResult(ctx context.Context, project *uplink.Project, limits []*pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey,
	rs eestream.RedundancyStrategy, data io.Reader) (results []*pb.SegmentPieceUploadResult, err error)
