// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package streams

import (
	"context"
	"crypto/rand"
	"io"
	"io/ioutil"
	mathrand "math/rand" // Using mathrand here because crypto-graphic randomness is not required.
	"sync"
	"time"

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/common/encryption"
	"storj.io/common/pb"
	"storj.io/common/ranger"
	"storj.io/common/storj"
	"storj.io/uplink/private/ecclient"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/metainfo"
)

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
// store the first piece at s0/<path>, second piece at s1/<path>, and the
// *last* piece at l/<path>. Store the given metadata, along with the number
// of segments, in a new protobuf, in the metadata of l/<path>.
//
// If there is an error, it cleans up any uploaded segment before returning.
func (s *Store) Put(ctx context.Context, path Path, data io.Reader, metadata Metadata, expiration time.Time) (_ Meta, err error) {
	defer mon.Task()(&ctx)(&err)
	derivedKey, err := encryption.DeriveContentKey(path.Bucket(), path.UnencryptedPath(), s.encStore)
	if err != nil {
		return Meta{}, err
	}
	encPath, err := encryption.EncryptPathWithStoreCipher(path.Bucket(), path.UnencryptedPath(), s.encStore)
	if err != nil {
		return Meta{}, err
	}

	beginObjectReq := &metainfo.BeginObjectParams{
		Bucket:               []byte(path.Bucket()),
		EncryptedPath:        []byte(encPath.Raw()),
		ExpiresAt:            expiration,
		EncryptionParameters: s.encryptionParameters,
	}

	var streamID storj.StreamID
	defer func() {
		if err != nil {
			s.cancelHandler(context.Background(), path)
			return
		}
	}()

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

		segmentEncryption := storj.SegmentEncryption{}
		if s.encryptionParameters.CipherSuite != storj.EncNull {
			segmentEncryption = storj.SegmentEncryption{
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
				Position: storj.SegmentPosition{
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
			uploadResults, err := s.ec.PutSingleResult(ctx, limits, piecePrivateKey, segmentRS, encSizedReader, expiration)
			if err != nil {
				return Meta{}, err
			}

			requestsToBatch = append(requestsToBatch, &metainfo.CommitSegmentParams{
				SegmentID:         segmentID,
				SizeEncryptedData: encSizedReader.Size(),
				PlainSize:         sizeReader.Size(),
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

			makeInlineSegment := &metainfo.MakeInlineSegmentParams{
				Position: storj.SegmentPosition{
					Index: int32(currentSegment),
				},
				Encryption:          segmentEncryption,
				EncryptedInlineData: cipherData,
				PlainSize:           int64(len(data)),
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

// Get returns a ranger that knows what the overall size is (from l/<path> or listing all segments)
// and then returns the appropriate data from segments s0/<path>, s1/<path>,
// ..., n/<path>.
func (s *Store) Get(ctx context.Context, path Path, object storj.Object) (rr ranger.Ranger, err error) {
	defer mon.Task()(&ctx)(&err)

	if object.Size == 0 {
		return ranger.ByteRanger(nil), nil
	}

	if object.FixedSegmentSize != 0 {
		return s.getWithLastSegment(ctx, path, object)
	}

	derivedKey, err := encryption.DeriveContentKey(path.Bucket(), path.UnencryptedPath(), s.encStore)
	if err != nil {
		return nil, err
	}

	// TODO can be batched with GetObject
	segmentsList, err := s.metainfo.ListSegments(ctx, metainfo.ListSegmentsParams{
		StreamID: object.ID,
	})
	if err != nil {
		return nil, err
	}

	var rangers []ranger.Ranger
	for _, segment := range segmentsList.Items {
		var contentNonce storj.Nonce
		_, err = encryption.Increment(&contentNonce, int64(segment.Position.Index)+1)
		if err != nil {
			return nil, err
		}

		rangers = append(rangers, &lazySegmentRanger{
			metainfo:             s.metainfo,
			streams:              s,
			streamID:             object.ID,
			position:             segment.Position,
			rs:                   object.RedundancyScheme,
			size:                 segment.PlainSize,
			derivedKey:           derivedKey,
			startingNonce:        &contentNonce,
			encryptionParameters: object.EncryptionParameters,
		})
	}

	return ranger.Concat(rangers...), nil
}

func (s *Store) getWithLastSegment(ctx context.Context, path Path, object storj.Object) (rr ranger.Ranger, err error) {
	defer mon.Task()(&ctx)(&err)

	info, limits, err := s.metainfo.DownloadSegment(ctx, metainfo.DownloadSegmentParams{
		StreamID: object.ID,
		Position: storj.SegmentPosition{
			Index: -1, // Request the last segment
		},
	})
	if err != nil {
		return nil, err
	}

	lastSegmentRanger, err := s.Ranger(ctx, info, limits, object.RedundancyScheme)
	if err != nil {
		return nil, err
	}

	derivedKey, err := encryption.DeriveContentKey(path.Bucket(), path.UnencryptedPath(), s.encStore)
	if err != nil {
		return nil, err
	}

	var rangers []ranger.Ranger
	for i := int64(0); i < object.SegmentCount-1; i++ {
		var contentNonce storj.Nonce
		_, err = encryption.Increment(&contentNonce, i+1)
		if err != nil {
			return nil, err
		}

		rangers = append(rangers, &lazySegmentRanger{
			metainfo: s.metainfo,
			streams:  s,
			streamID: object.ID,
			position: storj.SegmentPosition{
				Index: int32(i),
			},
			rs:                   object.RedundancyScheme,
			size:                 object.FixedSegmentSize,
			derivedKey:           derivedKey,
			startingNonce:        &contentNonce,
			encryptionParameters: object.EncryptionParameters,
		})
	}

	var contentNonce storj.Nonce
	_, err = encryption.Increment(&contentNonce, object.SegmentCount)
	if err != nil {
		return nil, err
	}

	decryptedLastSegmentRanger, err := decryptRanger(
		ctx,
		lastSegmentRanger,
		object.LastSegment.Size,
		object.EncryptionParameters,
		derivedKey,
		info.SegmentEncryption.EncryptedKey,
		&info.SegmentEncryption.EncryptedKeyNonce,
		&contentNonce,
	)
	if err != nil {
		return nil, err
	}

	rangers = append(rangers, decryptedLastSegmentRanger)
	return ranger.Concat(rangers...), nil
}

// Delete all the segments, with the last one last.
func (s *Store) Delete(ctx context.Context, path Path) (err error) {
	defer mon.Task()(&ctx)(&err)

	encPath, err := encryption.EncryptPathWithStoreCipher(path.Bucket(), path.UnencryptedPath(), s.encStore)
	if err != nil {
		return err
	}

	_, err = s.metainfo.BeginDeleteObject(ctx, metainfo.BeginDeleteObjectParams{
		Bucket:        []byte(path.Bucket()),
		EncryptedPath: []byte(encPath.Raw()),
	})
	return err
}

type lazySegmentRanger struct {
	ranger               ranger.Ranger
	metainfo             *metainfo.Client
	streams              *Store
	streamID             storj.StreamID
	position             storj.SegmentPosition
	rs                   storj.RedundancyScheme
	size                 int64
	derivedKey           *storj.Key
	startingNonce        *storj.Nonce
	encryptionParameters storj.EncryptionParameters
}

// Size implements Ranger.Size.
func (lr *lazySegmentRanger) Size() int64 {
	return lr.size
}

// Range implements Ranger.Range to be lazily connected.
func (lr *lazySegmentRanger) Range(ctx context.Context, offset, length int64) (_ io.ReadCloser, err error) {
	defer mon.Task()(&ctx)(&err)

	if lr.ranger == nil {
		info, limits, err := lr.metainfo.DownloadSegment(ctx, metainfo.DownloadSegmentParams{
			StreamID: lr.streamID,
			Position: storj.SegmentPosition{
				PartNumber: lr.position.PartNumber,
				Index:      lr.position.Index,
			},
		})
		if err != nil {
			return nil, err
		}

		rr, err := lr.streams.Ranger(ctx, info, limits, lr.rs)
		if err != nil {
			return nil, err
		}

		encryptedKey, keyNonce := info.SegmentEncryption.EncryptedKey, info.SegmentEncryption.EncryptedKeyNonce
		lr.ranger, err = decryptRanger(ctx, rr, lr.size, lr.encryptionParameters, lr.derivedKey, encryptedKey, &keyNonce, lr.startingNonce)
		if err != nil {
			return nil, err
		}
	}
	return lr.ranger.Range(ctx, offset, length)
}

// decryptRanger returns a decrypted ranger of the given rr ranger.
func decryptRanger(ctx context.Context, rr ranger.Ranger, decryptedSize int64, encryptionParameters storj.EncryptionParameters, derivedKey *storj.Key, encryptedKey storj.EncryptedPrivateKey, encryptedKeyNonce, startingNonce *storj.Nonce) (decrypted ranger.Ranger, err error) {
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
	return encryption.Unpad(rd, int(rd.Size()-decryptedSize))
}

// CancelHandler handles clean up of segments on receiving CTRL+C.
func (s *Store) cancelHandler(ctx context.Context, path Path) {
	defer mon.Task()(&ctx)(nil)

	// satellite deletes now from 0 to l so we can just use BeginDeleteObject
	err := s.Delete(ctx, path)
	if err != nil {
		zap.L().Warn("Failed deleting object", zap.Stringer("path", path), zap.Error(err))
	}
}

// Ranger creates a ranger for downloading erasure codes from piece store nodes.
func (s *Store) Ranger(
	ctx context.Context, info storj.SegmentDownloadInfo, limits []*pb.AddressedOrderLimit, objectRS storj.RedundancyScheme,
) (rr ranger.Ranger, err error) {
	defer mon.Task()(&ctx, info, limits, objectRS)(&err)

	// no order limits also means its inline segment
	if len(info.EncryptedInlineData) != 0 || len(limits) == 0 {
		return ranger.ByteRanger(info.EncryptedInlineData), nil
	}

	needed := objectRS.DownloadNodes()
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

	redundancy, err := eestream.NewRedundancyStrategyFromStorj(objectRS)
	if err != nil {
		return nil, err
	}

	rr, err = s.ec.Get(ctx, selected, info.PiecePrivateKey, redundancy, info.Size)
	return rr, err
}
