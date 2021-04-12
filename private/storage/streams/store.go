// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package streams

import (
	"context"
	"crypto/rand"
	"io"
	"io/ioutil"
	mathrand "math/rand" // Using mathrand here because crypto-graphic randomness is not required.
	"sort"
	"sync"
	"time"

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/ranger"
	"storj.io/common/storj"
	"storj.io/uplink/private/ecclient"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/metainfo"
	"storj.io/uplink/private/testuplink"
)

// DisableDeleteOnCancel is now a no-op.
func DisableDeleteOnCancel(ctx context.Context) context.Context {
	return ctx
}

var mon = monkit.Package()

// Meta info about a stream.
type Meta struct {
	Modified   time.Time
	Expiration time.Time
	Size       int64
	Data       []byte
}

// Metadata interface returns the latest metadata for an object.
type Metadata interface {
	Metadata() ([]byte, error)
}

// Store is a store for streams. It implements typedStore as part of an ongoing migration
// to use typed paths. See the shim for the store that the rest of the world interacts with.
type Store struct {
	metainfo             *metainfo.Client
	ec                   ecclient.Client
	segmentSize          int64
	encStore             *encryption.Store
	encryptionParameters storj.EncryptionParameters
	inlineThreshold      int

	rngMu sync.Mutex
	rng   *mathrand.Rand
}

// NewStreamStore constructs a stream store.
func NewStreamStore(metainfo *metainfo.Client, ec ecclient.Client, segmentSize int64, encStore *encryption.Store, encryptionParameters storj.EncryptionParameters, inlineThreshold int) (*Store, error) {
	if segmentSize <= 0 {
		return nil, errs.New("segment size must be larger than 0")
	}
	if encryptionParameters.BlockSize <= 0 {
		return nil, errs.New("encryption block size must be larger than 0")
	}

	return &Store{
		metainfo:             metainfo,
		ec:                   ec,
		segmentSize:          segmentSize,
		encStore:             encStore,
		encryptionParameters: encryptionParameters,
		inlineThreshold:      inlineThreshold,
		rng:                  mathrand.New(mathrand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Close closes the underlying resources passed to the metainfo DB.
func (s *Store) Close() error {
	return s.metainfo.Close()
}

// Put breaks up data as it comes in into s.segmentSize length pieces, then
// store the first piece at s0/<key>, second piece at s1/<key>, and the
// *last* piece at l/<key>. Store the given metadata, along with the number
// of segments, in a new protobuf, in the metadata of l/<key>.
//
// If there is an error, it cleans up any uploaded segment before returning.
func (s *Store) Put(ctx context.Context, bucket, unencryptedKey string, data io.Reader, metadata Metadata, expiration time.Time) (_ Meta, err error) {
	defer mon.Task()(&ctx)(&err)
	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(unencryptedKey), s.encStore)
	if err != nil {
		return Meta{}, err
	}
	encPath, err := encryption.EncryptPathWithStoreCipher(bucket, paths.NewUnencrypted(unencryptedKey), s.encStore)
	if err != nil {
		return Meta{}, err
	}

	beginObjectReq := &metainfo.BeginObjectParams{
		Bucket:               []byte(bucket),
		EncryptedPath:        []byte(encPath.Raw()),
		ExpiresAt:            expiration,
		EncryptionParameters: s.encryptionParameters,
	}

	var streamID storj.StreamID

	var (
		currentSegment      int64
		contentKey          storj.Key
		streamSize          int64
		lastSegmentSize     int64
		encryptedKey        []byte
		keyNonce            storj.Nonce
		objectRS, segmentRS eestream.RedundancyStrategy

		requestsToBatch = make([]metainfo.BatchItem, 0, 2)
	)

	maxEncryptedSegmentSize, err := encryption.CalcEncryptedSize(s.segmentSize, s.encryptionParameters)
	if err != nil {
		return Meta{}, err
	}

	eofReader := NewEOFReader(data)
	for !eofReader.IsEOF() && !eofReader.HasError() {
		// generate random key for encrypting the segment's content
		_, err := rand.Read(contentKey[:])
		if err != nil {
			return Meta{}, err
		}

		// Initialize the content nonce with the current total segment incremented
		// by 1 because at this moment the next segment has not been already
		// uploaded.
		// The increment by 1 is to avoid nonce reuse with the metadata encryption,
		// which is encrypted with the zero nonce.
		contentNonce := storj.Nonce{}
		_, err = encryption.Increment(&contentNonce, currentSegment+1)
		if err != nil {
			return Meta{}, err
		}

		// generate random nonce for encrypting the content key
		_, err = rand.Read(keyNonce[:])
		if err != nil {
			return Meta{}, err
		}

		encryptedKey, err = encryption.EncryptKey(&contentKey, s.encryptionParameters.CipherSuite, derivedKey, &keyNonce)
		if err != nil {
			return Meta{}, err
		}

		sizeReader := SizeReader(eofReader)
		segmentReader := io.LimitReader(sizeReader, s.segmentSize)
		peekReader := NewPeekThresholdReader(segmentReader)
		// If the data is larger than the inline threshold size, then it will be a remote segment
		isRemote, err := peekReader.IsLargerThan(s.inlineThreshold)
		if err != nil {
			return Meta{}, err
		}

		segmentEncryption := metainfo.SegmentEncryption{}
		if s.encryptionParameters.CipherSuite != storj.EncNull {
			segmentEncryption = metainfo.SegmentEncryption{
				EncryptedKey:      encryptedKey,
				EncryptedKeyNonce: keyNonce,
			}
		}

		if isRemote {
			encrypter, err := encryption.NewEncrypter(s.encryptionParameters.CipherSuite, &contentKey, &contentNonce, int(s.encryptionParameters.BlockSize))
			if err != nil {
				return Meta{}, err
			}

			paddedReader := encryption.PadReader(ioutil.NopCloser(peekReader), encrypter.InBlockSize())
			transformedReader := encryption.TransformReader(paddedReader, encrypter, 0)

			beginSegment := &metainfo.BeginSegmentParams{
				MaxOrderLimit: maxEncryptedSegmentSize,
				Position: metainfo.SegmentPosition{
					Index: int32(currentSegment),
				},
			}

			var responses []metainfo.BatchResponse
			if currentSegment == 0 {
				responses, err = s.metainfo.Batch(ctx, beginObjectReq, beginSegment)
				if err != nil {
					return Meta{}, err
				}
				objResponse, err := responses[0].BeginObject()
				if err != nil {
					return Meta{}, err
				}
				streamID = objResponse.StreamID
				objectRS = objResponse.RedundancyStrategy
			} else {
				beginSegment.StreamID = streamID
				responses, err = s.metainfo.Batch(ctx, append(requestsToBatch, beginSegment)...)
				requestsToBatch = requestsToBatch[:0]
				if err != nil {
					return Meta{}, err
				}
			}

			segResponse, err := responses[1].BeginSegment()
			if err != nil {
				return Meta{}, err
			}
			segmentID := segResponse.SegmentID
			limits := segResponse.Limits
			piecePrivateKey := segResponse.PiecePrivateKey
			segmentRS = segResponse.RedundancyStrategy

			if segmentRS == (eestream.RedundancyStrategy{}) {
				segmentRS = objectRS
			}
			encSizedReader := SizeReader(transformedReader)
			uploadResults, err := s.ec.PutSingleResult(ctx, limits, piecePrivateKey, segmentRS, encSizedReader)
			if err != nil {
				return Meta{}, err
			}

			plainSize := sizeReader.Size()
			if testuplink.IsWithoutPlainSize(ctx) {
				plainSize = 0
			}

			requestsToBatch = append(requestsToBatch, &metainfo.CommitSegmentParams{
				SegmentID:         segmentID,
				SizeEncryptedData: encSizedReader.Size(),
				PlainSize:         plainSize,
				Encryption:        segmentEncryption,
				UploadResult:      uploadResults,
			})
		} else {
			data, err := ioutil.ReadAll(peekReader)
			if err != nil {
				return Meta{}, err
			}

			cipherData, err := encryption.Encrypt(data, s.encryptionParameters.CipherSuite, &contentKey, &contentNonce)
			if err != nil {
				return Meta{}, err
			}

			plainSize := int64(len(data))
			if testuplink.IsWithoutPlainSize(ctx) {
				plainSize = 0
			}

			makeInlineSegment := &metainfo.MakeInlineSegmentParams{
				Position: metainfo.SegmentPosition{
					Index: int32(currentSegment),
				},
				Encryption:          segmentEncryption,
				EncryptedInlineData: cipherData,
				PlainSize:           plainSize,
			}
			if currentSegment == 0 {
				responses, err := s.metainfo.Batch(ctx, beginObjectReq, makeInlineSegment)
				if err != nil {
					return Meta{}, err
				}
				objResponse, err := responses[0].BeginObject()
				if err != nil {
					return Meta{}, err
				}
				streamID = objResponse.StreamID
			} else {
				makeInlineSegment.StreamID = streamID
				requestsToBatch = append(requestsToBatch, makeInlineSegment)
			}
		}

		lastSegmentSize = sizeReader.Size()
		streamSize += lastSegmentSize
		currentSegment++
	}

	totalSegments := currentSegment

	if eofReader.HasError() {
		return Meta{}, eofReader.err
	}

	metadataBytes, err := metadata.Metadata()
	if err != nil {
		return Meta{}, err
	}

	streamInfo, err := pb.Marshal(&pb.StreamInfo{
		SegmentsSize:    s.segmentSize,
		LastSegmentSize: lastSegmentSize,
		Metadata:        metadataBytes,
	})
	if err != nil {
		return Meta{}, err
	}

	// encrypt metadata with the content encryption key and zero nonce.
	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, s.encryptionParameters.CipherSuite, &contentKey, &storj.Nonce{})
	if err != nil {
		return Meta{}, err
	}

	streamMeta := pb.StreamMeta{
		NumberOfSegments:    totalSegments,
		EncryptedStreamInfo: encryptedStreamInfo,
		EncryptionType:      int32(s.encryptionParameters.CipherSuite),
		EncryptionBlockSize: s.encryptionParameters.BlockSize,
	}

	if s.encryptionParameters.CipherSuite != storj.EncNull {
		streamMeta.LastSegmentMeta = &pb.SegmentMeta{
			EncryptedKey: encryptedKey,
			KeyNonce:     keyNonce[:],
		}
	}

	objectMetadata, err := pb.Marshal(&streamMeta)
	if err != nil {
		return Meta{}, err
	}

	commitObject := metainfo.CommitObjectParams{
		StreamID:          streamID,
		EncryptedMetadata: objectMetadata,
	}
	if len(requestsToBatch) > 0 {
		_, err = s.metainfo.Batch(ctx, append(requestsToBatch, &commitObject)...)
	} else {
		err = s.metainfo.CommitObject(ctx, commitObject)
	}
	if err != nil {
		return Meta{}, err
	}

	satStreamID := &pb.SatStreamID{}
	err = pb.Unmarshal(streamID, satStreamID)
	if err != nil {
		return Meta{}, err
	}

	resultMeta := Meta{
		Modified:   satStreamID.CreationDate,
		Expiration: expiration,
		Size:       streamSize,
		Data:       metadataBytes,
	}

	return resultMeta, nil
}

// Get returns a ranger that knows what the overall size is (from l/<key>)
// and then returns the appropriate data from segments s0/<key>, s1/<key>,
// ..., l/<key>.
func (s *Store) Get(ctx context.Context, bucket, unencryptedKey string, info metainfo.DownloadInfo) (rr ranger.Ranger, err error) {
	defer mon.Task()(&ctx)(&err)

	object := info.Object
	if object.Size == 0 {
		return ranger.ByteRanger(nil), nil
	}

	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(unencryptedKey), s.encStore)
	if err != nil {
		return nil, err
	}

	// download all missing segments
	if info.ListSegments.More {
		for info.ListSegments.More {
			var cursor storj.SegmentPosition
			if len(info.ListSegments.Items) > 0 {
				last := info.ListSegments.Items[len(info.ListSegments.Items)-1]
				cursor = last.Position
			}

			result, err := s.metainfo.ListSegments(ctx, metainfo.ListSegmentsParams{
				StreamID: object.ID,
				Cursor:   cursor,
				Range:    info.Range,
			})
			if err != nil {
				return nil, err
			}

			info.ListSegments.Items = append(info.ListSegments.Items, result.Items...)
			info.ListSegments.More = result.More
		}
	}

	downloaded := info.DownloadedSegments
	listed := info.ListSegments.Items

	// calculate plain offset and plain size for migrated objects.
	for i := 0; i < len(info.DownloadedSegments); i++ {
		seg := &info.DownloadedSegments[i].Info
		seg.PlainOffset, seg.PlainSize = calculatePlain(*seg.Position, seg.PlainOffset, seg.PlainSize, object)
	}
	for i := 0; i < len(info.ListSegments.Items); i++ {
		seg := &info.ListSegments.Items[i]
		seg.PlainOffset, seg.PlainSize = calculatePlain(seg.Position, seg.PlainOffset, seg.PlainSize, object)
	}

	// ensure that the items are correctly sorted
	sort.Slice(downloaded, func(i, k int) bool {
		return downloaded[i].Info.PlainOffset < downloaded[k].Info.PlainOffset
	})
	sort.Slice(listed, func(i, k int) bool {
		return listed[i].PlainOffset < listed[k].PlainOffset
	})

	// calculate the offset for the range listed / downloaded
	var offset int64
	switch {
	case len(downloaded) > 0 && len(listed) > 0:
		if listed[0].PlainOffset < downloaded[0].Info.PlainOffset {
			offset = listed[0].PlainOffset
		} else {
			offset = downloaded[0].Info.PlainOffset
		}
	case len(downloaded) > 0:
		offset = downloaded[0].Info.PlainOffset
	case len(listed) > 0:
		offset = listed[0].PlainOffset
	}

	rangers := make([]ranger.Ranger, 0, len(downloaded)+len(listed)+2)

	if offset > 0 {
		rangers = append(rangers, &invalidRanger{size: offset})
	}

	for len(downloaded) > 0 || len(listed) > 0 {
		switch {
		case len(downloaded) > 0 && downloaded[0].Info.PlainOffset == offset:
			segment := downloaded[0]
			downloaded = downloaded[1:]

			// drop any duplicate segment info in listing
			for len(listed) > 0 && listed[0].PlainOffset == offset {
				if listed[0].Position != *segment.Info.Position {
					return nil, errs.New("segment info for download and list does not match: %v != %v", listed[0].Position, *segment.Info.Position)
				}
				listed = listed[1:]
			}

			encryptedRanger, err := s.Ranger(ctx, segment)
			if err != nil {
				return nil, err
			}

			contentNonce, err := deriveContentNonce(*segment.Info.Position)
			if err != nil {
				return nil, err
			}

			enc := segment.Info.SegmentEncryption
			decrypted, err := decryptRanger(ctx, encryptedRanger, segment.Info.PlainSize, object.EncryptionParameters, derivedKey, enc.EncryptedKey, &enc.EncryptedKeyNonce, &contentNonce)
			if err != nil {
				return nil, err
			}

			rangers = append(rangers, decrypted)
			offset += segment.Info.PlainSize

		case len(listed) > 0 && listed[0].PlainOffset == offset:
			segment := listed[0]
			listed = listed[1:]

			contentNonce, err := deriveContentNonce(segment.Position)
			if err != nil {
				return nil, err
			}

			rangers = append(rangers, &lazySegmentRanger{
				metainfo:             s.metainfo,
				streams:              s,
				streamID:             object.ID,
				position:             segment.Position,
				plainSize:            segment.PlainSize,
				derivedKey:           derivedKey,
				startingNonce:        &contentNonce,
				encryptionParameters: object.EncryptionParameters,
			})
			offset += segment.PlainSize

		default:
			return nil, errs.New("missing segment for offset %d", offset)
		}
	}

	if offset < object.Size {
		rangers = append(rangers, &invalidRanger{size: object.Size - offset})
	}
	if offset > object.Size {
		return nil, errs.New("invalid final offset %d; expected %d", offset, object.Size)
	}

	return ranger.Concat(rangers...), nil
}

func deriveContentNonce(pos storj.SegmentPosition) (storj.Nonce, error) {
	// The increment by 1 is to avoid nonce reuse with the metadata encryption,
	// which is encrypted with the zero nonce.
	var n storj.Nonce
	_, err := encryption.Increment(&n, int64(pos.PartNumber)<<32|(int64(pos.Index)+1))
	return n, err
}

// calculatePlain calculates segment plain size, taking into account migrated objects.
func calculatePlain(pos storj.SegmentPosition, rawOffset, rawSize int64, object storj.Object) (plainOffset, plainSize int64) {
	switch {
	case object.FixedSegmentSize <= 0:
		// this is a multipart object and has correct offset and size.
		return rawOffset, rawSize
	case pos.PartNumber > 0:
		// this case should be impossible, however let's return the initial values.
		return rawOffset, rawSize
	case pos.Index == int32(object.SegmentCount-1):
		// this is a last segment
		return int64(pos.Index) * object.FixedSegmentSize, object.LastSegment.Size
	default:
		// this is a fixed size segment
		return int64(pos.Index) * object.FixedSegmentSize, object.FixedSegmentSize
	}
}

type lazySegmentRanger struct {
	ranger               ranger.Ranger
	metainfo             *metainfo.Client
	streams              *Store
	streamID             storj.StreamID
	position             metainfo.SegmentPosition
	plainSize            int64
	derivedKey           *storj.Key
	startingNonce        *storj.Nonce
	encryptionParameters storj.EncryptionParameters
}

// Size implements Ranger.Size.
func (lr *lazySegmentRanger) Size() int64 {
	return lr.plainSize
}

// Range implements Ranger.Range to be lazily connected.
func (lr *lazySegmentRanger) Range(ctx context.Context, offset, length int64) (_ io.ReadCloser, err error) {
	defer mon.Task()(&ctx)(&err)

	if lr.ranger == nil {
		downloadResponse, err := lr.metainfo.DownloadSegmentWithRS(ctx, metainfo.DownloadSegmentParams{
			StreamID: lr.streamID,
			Position: metainfo.SegmentPosition{
				PartNumber: lr.position.PartNumber,
				Index:      lr.position.Index,
			},
		})
		if err != nil {
			return nil, err
		}

		rr, err := lr.streams.Ranger(ctx, downloadResponse)
		if err != nil {
			return nil, err
		}

		encryptedKey, keyNonce := downloadResponse.Info.SegmentEncryption.EncryptedKey, downloadResponse.Info.SegmentEncryption.EncryptedKeyNonce
		lr.ranger, err = decryptRanger(ctx, rr, lr.plainSize, lr.encryptionParameters, lr.derivedKey, encryptedKey, &keyNonce, lr.startingNonce)
		if err != nil {
			return nil, err
		}
	}
	return lr.ranger.Range(ctx, offset, length)
}

// decryptRanger returns a decrypted ranger of the given rr ranger.
func decryptRanger(ctx context.Context, rr ranger.Ranger, plainSize int64, encryptionParameters storj.EncryptionParameters, derivedKey *storj.Key, encryptedKey storj.EncryptedPrivateKey, encryptedKeyNonce, startingNonce *storj.Nonce) (decrypted ranger.Ranger, err error) {
	defer mon.Task()(&ctx)(&err)
	contentKey, err := encryption.DecryptKey(encryptedKey, encryptionParameters.CipherSuite, derivedKey, encryptedKeyNonce)
	if err != nil {
		return nil, err
	}

	decrypter, err := encryption.NewDecrypter(encryptionParameters.CipherSuite, contentKey, startingNonce, int(encryptionParameters.BlockSize))
	if err != nil {
		return nil, err
	}

	var rd ranger.Ranger
	if rr.Size()%int64(decrypter.InBlockSize()) != 0 {
		reader, err := rr.Range(ctx, 0, rr.Size())
		if err != nil {
			return nil, err
		}
		defer func() { err = errs.Combine(err, reader.Close()) }()
		cipherData, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		data, err := encryption.Decrypt(cipherData, encryptionParameters.CipherSuite, contentKey, startingNonce)
		if err != nil {
			return nil, err
		}
		return ranger.ByteRanger(data), nil
	}

	rd, err = encryption.Transform(rr, decrypter)
	if err != nil {
		return nil, err
	}
	return encryption.Unpad(rd, int(rd.Size()-plainSize))
}

// Ranger creates a ranger for downloading erasure codes from piece store nodes.
func (s *Store) Ranger(ctx context.Context, response metainfo.DownloadSegmentWithRSResponse) (rr ranger.Ranger, err error) {
	info := response.Info
	limits := response.Limits

	defer mon.Task()(&ctx, info, limits, info.RedundancyScheme)(&err)

	// no order limits also means its inline segment
	if len(info.EncryptedInlineData) != 0 || len(limits) == 0 {
		return ranger.ByteRanger(info.EncryptedInlineData), nil
	}

	needed := info.RedundancyScheme.DownloadNodes()
	selected := make([]*pb.AddressedOrderLimit, len(limits))
	s.rngMu.Lock()
	perm := s.rng.Perm(len(limits))
	s.rngMu.Unlock()

	for _, i := range perm {
		limit := limits[i]
		if limit == nil {
			continue
		}

		selected[i] = limit

		needed--
		if needed <= 0 {
			break
		}
	}

	redundancy, err := eestream.NewRedundancyStrategyFromStorj(info.RedundancyScheme)
	if err != nil {
		return nil, err
	}

	rr, err = s.ec.Get(ctx, selected, info.PiecePrivateKey, redundancy, info.EncryptedSize)
	return rr, err
}

// invalidRanger is used to mark a range as invalid.
type invalidRanger struct {
	size int64
}

func (d *invalidRanger) Size() int64 { return d.size }

func (d *invalidRanger) Range(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, errs.New("negative offset")
	}
	if length < 0 {
		return nil, errs.New("negative length")
	}
	// allow reading zero bytes from an invalid range.
	if 0 <= offset && offset <= d.size && length == 0 {
		return emptyReader{}, nil
	}
	return nil, errs.New("invalid range %d:%d (size:%d)", offset, length, d.size)
}

// emptyReader is used to read no data.
type emptyReader struct{}

func (emptyReader) Read(data []byte) (n int, err error) { return 0, io.EOF }

func (emptyReader) Close() error { return nil }
