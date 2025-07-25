// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package streams

import (
	"bytes"
	"context"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream/scheduler"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams/splitter"
	"storj.io/uplink/private/storage/streams/streamupload"
)

var (
	creationDate         = time.Date(2023, time.February, 23, 10, 0, 0, 0, time.UTC)
	segmentSize          = int64(4096)
	cipherSuite          = storj.EncAESGCM
	encryptionParameters = storj.EncryptionParameters{CipherSuite: cipherSuite, BlockSize: 32}
	inlineThreshold      = 1024
	longTailMargin       = 1
	storjKey             = storj.Key{0: 1}
	expiration           = creationDate.Add(time.Hour)
	uploadInfo           = streamupload.Info{CreationDate: creationDate, PlainSize: 123}
	streamID             = storj.StreamID("STREAMID")
	partNumber           = int32(1)
	eTagCh               = make(chan []byte)
)

func TestNewUploader(t *testing.T) {
	type config struct {
		segmentSize          int64
		encryptionParameters storj.EncryptionParameters
		inlineThreshold      int
		longTailMargin       int
	}

	for _, tc := range []struct {
		desc           string
		overrideConfig func(c *config)
		expectNewErr   string
	}{
		{
			desc: "segment size is zero",
			overrideConfig: func(c *config) {
				c.segmentSize = 0
			},
			expectNewErr: "segment size must be larger than 0",
		},
		{
			desc: "block size is zero",
			overrideConfig: func(c *config) {
				c.encryptionParameters.BlockSize = 0
			},
			expectNewErr: "encryption block size must be larger than 0",
		},
		{
			desc: "inline threshold is zero",
			overrideConfig: func(c *config) {
				c.inlineThreshold = 0
			},
			expectNewErr: "inline threshold must be larger than 0",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// These parameters are not validated by NewUploader but stashed
			// and used later.
			var (
				metainfo *metaclient.Client
				encStore *encryption.Store
			)
			c := config{
				segmentSize:          segmentSize,
				encryptionParameters: encryptionParameters,
				inlineThreshold:      inlineThreshold,
			}
			tc.overrideConfig(&c)

			uploader, err := NewUploader(metainfo, piecePutter{}, c.segmentSize, encStore, c.encryptionParameters, c.inlineThreshold, c.longTailMargin)
			if uploader != nil {
				defer func() { assert.NoError(t, uploader.Close()) }()
			}
			if tc.expectNewErr != "" {
				require.EqualError(t, err, tc.expectNewErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, uploader)
		})
	}
}

func TestUpload(t *testing.T) {
	type config struct {
		bucket   string
		key      string
		metadata Metadata
		backend  uploaderBackend
	}

	testUpload := func(t *testing.T, uploadFn func(uploader *Uploader, c config) (*Upload, error)) {
		for _, tc := range []struct {
			desc            string
			overrideConfig  func(c *config)
			expectUploadErr string
			expectCommitErr string
		}{
			{
				desc: "no access to bucket",
				overrideConfig: func(c *config) {
					c.bucket = "OHNO"
				},
				expectUploadErr: `missing encryption base: "OHNO"/"KEY"`,
			},
			{
				desc: "no access to key",
				overrideConfig: func(c *config) {
					c.key = "OHNO"
				},
				expectUploadErr: `missing encryption base: "BUCKET"/"OHNO"`,
			},
			{
				desc: "upload fails",
				overrideConfig: func(c *config) {
					c.backend = fakeUploaderBackend{err: errs.New("upload failed")}
				},
				expectCommitErr: "upload failed",
			},
			{
				desc:           "upload success",
				overrideConfig: func(c *config) {},
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				var (
					encStore = encryption.NewStore()
					unenc    = paths.NewUnencrypted("KEY")
					enc      = paths.NewEncrypted("ENCKEY")
				)

				err := encStore.Add("BUCKET", unenc, enc, storjKey)
				require.NoError(t, err)

				c := config{
					bucket:   "BUCKET",
					key:      "KEY",
					metadata: fixedMetadata{},
					backend:  fakeUploaderBackend{},
				}
				tc.overrideConfig(&c)

				uploader, err := NewUploader(metainfoUpload{}, piecePutter{}, segmentSize, encStore, encryptionParameters, inlineThreshold, longTailMargin)
				require.NoError(t, err)
				defer func() { assert.NoError(t, uploader.Close()) }()

				uploader.backend = c.backend

				upload, err := uploadFn(uploader, c)
				if tc.expectUploadErr != "" {
					require.EqualError(t, err, tc.expectUploadErr)
					return
				}
				require.NoError(t, err)
				require.NotNil(t, upload)

				meta := upload.Meta()
				require.Nil(t, meta, "upload has metadata before being committed")

				err = upload.Commit()

				// Whether or not the commit succeeds or fails, writing to the
				// upload should fail, since either path finishes the underlying
				// splitter.
				_, copyErr := io.Copy(upload, strings.NewReader("JUNK"))
				require.EqualError(t, copyErr, "upload already done")

				if tc.expectCommitErr != "" {
					require.EqualError(t, err, tc.expectCommitErr)
					return
				}
				require.NoError(t, err)

				meta = upload.Meta()
				require.Equal(t, &Meta{Modified: uploadInfo.CreationDate, Size: uploadInfo.PlainSize}, meta)
			})
		}
	}

	t.Run("Object", func(t *testing.T) {
		testUpload(t, func(uploader *Uploader, c config) (*Upload, error) {
			return uploader.UploadObject(context.Background(), c.bucket, c.key, c.metadata, noopScheduler{}, &metaclient.UploadOptions{
				Expires: expiration,
			})
		})
	})

	t.Run("Part", func(t *testing.T) {
		testUpload(t, func(uploader *Uploader, c config) (*Upload, error) {
			return uploader.UploadPart(context.Background(), c.bucket, c.key, streamID, partNumber, eTagCh, noopScheduler{})
		})
	})
}

func TestEncryptedMetadata(t *testing.T) {
	e := encryptedMetadata{
		metadata:    fixedMetadata{},
		segmentSize: segmentSize,
		derivedKey:  &storjKey,
		cipherSuite: cipherSuite,
	}

	encData, err := e.EncryptedMetadata(segmentSize - 1)
	require.NoError(t, err)
	require.NotNil(t, encData.EncryptedMetadata)
	require.NotNil(t, encData.EncryptedMetadataEncryptedKey)
	require.NotNil(t, encData.EncryptedMetadataNonce)
	require.NotNil(t, encData.EncryptedETag)

	streamMeta := new(pb.StreamMeta)
	err = pb.Unmarshal(encData.EncryptedMetadata, streamMeta)
	require.NoError(t, err)

	// Decrypt and assert contents
	metadataKey, err := encryption.DecryptKey(encData.EncryptedMetadataEncryptedKey, e.cipherSuite, e.derivedKey, &encData.EncryptedMetadataNonce)
	require.NoError(t, err)
	streamInfoBytes, err := encryption.Decrypt(streamMeta.EncryptedStreamInfo, e.cipherSuite, metadataKey, &storj.Nonce{})
	require.NoError(t, err)

	streamInfo := new(pb.StreamInfo)
	err = pb.Unmarshal(streamInfoBytes, streamInfo)
	require.NoError(t, err)

	require.Equal(t, &pb.StreamInfo{
		SegmentsSize:    segmentSize,
		LastSegmentSize: segmentSize - 1,
		Metadata:        []byte("METADATA"),
	}, streamInfo)

	// Decrypt etag and asset contents
	etagBytes, err := encryption.Decrypt(encData.EncryptedETag, e.cipherSuite, metadataKey, &storj.Nonce{1})
	require.NoError(t, err)

	require.Equal(t, []byte("ETAG"), etagBytes)
}

func TestLimitsExchanger(t *testing.T) {
	e := limitsExchanger{metainfo: retryBeginSegments{}}

	t.Run("success", func(t *testing.T) {
		segmentID, limits, err := e.ExchangeLimits(context.Background(), storj.SegmentID("IN"), []int{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, storj.SegmentID("OUT"), segmentID)
		require.Equal(t, []*pb.AddressedOrderLimit{{Limit: &pb.OrderLimit{Limit: 123}}}, limits)
	})
}

type retryBeginSegments struct {
	MetainfoUpload
}

func (r retryBeginSegments) RetryBeginSegmentPieces(ctx context.Context, params metaclient.RetryBeginSegmentPiecesParams) (metaclient.RetryBeginSegmentPiecesResponse, error) {
	// Calculate a limit that helps us detect that we passed the piece numbers correctly.
	var limit int64
	for i, num := range params.RetryPieceNumbers {
		limit += int64(math.Pow(10, float64(len(params.RetryPieceNumbers)-i-1))) * int64(num)
	}
	switch string(params.SegmentID) {
	case "IN":
		return metaclient.RetryBeginSegmentPiecesResponse{SegmentID: []byte("OUT"), Limits: []*pb.AddressedOrderLimit{{Limit: &pb.OrderLimit{Limit: limit}}}}, nil
	case "ERR":
		return metaclient.RetryBeginSegmentPiecesResponse{}, errs.New("expected error")
	default:
		return metaclient.RetryBeginSegmentPiecesResponse{}, errs.New("segment ID not passed correctly")
	}
}

type fakeUploaderBackend struct {
	err error
}

func (b fakeUploaderBackend) UploadObject(ctx context.Context, segmentSource streamupload.SegmentSource, segmentUploader streamupload.SegmentUploader, miBatcher metaclient.Batcher, beginObject *metaclient.BeginObjectParams, encMeta streamupload.EncryptedMetadata) (streamupload.Info, error) {
	if err := b.checkCommonParams(segmentSource, segmentUploader, miBatcher); err != nil {
		return streamupload.Info{}, err
	}

	m, ok := encMeta.(*encryptedMetadata)
	if !ok {
		return streamupload.Info{}, errs.New("encMeta is of type %T but expected %T", encMeta, m)
	}

	if um, ok := m.metadata.(fixedMetadata); !ok {
		return streamupload.Info{}, errs.New("encryptedMetadata metadata is of type %T but expected %T", m.metadata, um)
	}
	if m.segmentSize != segmentSize {
		return streamupload.Info{}, errs.New("encryptedMetadata segmentSize should be %d but is %d", segmentSize, m.segmentSize)
	}
	if m.derivedKey == nil {
		return streamupload.Info{}, errs.New("encryptedMetadata segment derived key is nil")
	}
	if m.cipherSuite != cipherSuite {
		return streamupload.Info{}, errs.New("encryptedMetadata cipherSuite should be %d but got %d", cipherSuite, m.cipherSuite)
	}

	return b.upload()
}

func (b fakeUploaderBackend) UploadPart(ctx context.Context, segmentSource streamupload.SegmentSource, segmentUploader streamupload.SegmentUploader, miBatcher metaclient.Batcher, streamIDIn storj.StreamID, eTagChIn <-chan []byte) (streamupload.Info, error) {
	if err := b.checkCommonParams(segmentSource, segmentUploader, miBatcher); err != nil {
		return streamupload.Info{}, err
	}
	if !bytes.Equal(streamIDIn, streamID) {
		return streamupload.Info{}, errs.New("expected stream ID %x but got %x", streamID, streamIDIn)
	}
	if eTagChIn != eTagCh {
		return streamupload.Info{}, errs.New("unexpected eTag channel")
	}
	return b.upload()
}

func (fakeUploaderBackend) checkCommonParams(source streamupload.SegmentSource, uploader streamupload.SegmentUploader, batcher metaclient.Batcher) error {
	if s, ok := source.(*splitter.Splitter); !ok {
		return errs.New("segment source is of type %T but expected %T", source, s)
	} else if s == nil {
		return errs.New("segment source is not a valid splitter: nil")
	}

	u, ok := uploader.(segmentUploader)
	if !ok {
		return errs.New("segmentUploader is of type %T but expected %T", uploader, u)
	}

	if um, ok := u.metainfo.(metainfoUpload); !ok {
		return errs.New("segmentUploader metainfo is of type %T but expected %T", u.metainfo, um)
	}
	if up, ok := u.piecePutter.(piecePutter); !ok {
		return errs.New("segmentUploader piece putter is of type %T but expected %T", u.piecePutter, up)
	}
	if s, ok := u.sched.(noopScheduler); !ok {
		return errs.New("segmentUploader scheduler is of type %T but expected %T", u.sched, s)
	}
	if u.longTailMargin != longTailMargin {
		return errs.New("segmentUploader long tail margin is %d but expected %d", u.longTailMargin, longTailMargin)
	}

	if b, ok := batcher.(metainfoUpload); !ok {
		return errs.New("batcher is of type %T but expected %T", batcher, b)
	}

	return nil
}

func (b fakeUploaderBackend) upload() (streamupload.Info, error) {
	if b.err != nil {
		return streamupload.Info{}, b.err
	}
	return uploadInfo, nil
}

type metainfoUpload struct{}

func (metainfoUpload) Batch(ctx context.Context, batchItems ...metaclient.BatchItem) ([]metaclient.BatchResponse, error) {
	return nil, errs.New("should not be called")
}

func (metainfoUpload) RetryBeginSegmentPieces(ctx context.Context, params metaclient.RetryBeginSegmentPiecesParams) (metaclient.RetryBeginSegmentPiecesResponse, error) {
	return metaclient.RetryBeginSegmentPiecesResponse{}, errs.New("should not be called")
}

func (metainfoUpload) Close() error {
	return nil
}

type piecePutter struct{}

func (piecePutter) PutPiece(longTailCtx, uploadCtx context.Context, limit *pb.AddressedOrderLimit, privateKey storj.PiecePrivateKey, data io.ReadCloser) (hash *pb.PieceHash, deprecated *struct{}, err error) {
	return nil, nil, errs.New("should not be called")
}

type fixedMetadata struct{}

func (fixedMetadata) Metadata() ([]byte, error) {
	return []byte("METADATA"), nil
}

func (fixedMetadata) ETag() ([]byte, error) {
	return []byte("ETAG"), nil
}

type noopScheduler struct{}

func (noopScheduler) Join(ctx context.Context) (scheduler.Handle, bool) {
	return noopHandle{}, true
}

type noopHandle struct{}

func (noopHandle) Get(context.Context) (scheduler.Resource, bool) {
	return noopResource{}, false
}

func (noopHandle) Done() {}

type noopResource struct{}

func (noopResource) Done() {}
