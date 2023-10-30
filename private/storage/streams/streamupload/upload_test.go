// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package streamupload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams/splitter"
)

var (
	streamID            = mustNewStreamID()
	bucket              = []byte("BUCKET")
	encryptedObjectKey  = []byte("ENCRYPTED-OBJECT-KEY")
	beginObject         = &metaclient.BeginObjectParams{Bucket: bucket, EncryptedObjectKey: encryptedObjectKey}
	encMetadataKey      = storj.EncryptedPrivateKey("METADATA-KEY")
	encMetadataKeyNonce = storj.Nonce{0: 0, 1: 1, 2: 2, 3: 3}
	creationDate        = time.Now().UTC()
	eTag                = []byte("ETAG")
)

func TestUploadObject(t *testing.T) {
	for _, tc := range []struct {
		desc                    string
		segments                []splitter.Segment
		blockNext               bool
		sourceErr               error
		sourceErrAfter          int
		expectBeginObject       bool
		expectCommitObject      bool
		expectBeginDeleteObject bool
		expectErr               string
	}{
		{
			desc:      "source must provide at least one segment",
			expectErr: "programmer error: there should always be at least one segment",
		},
		{
			desc:     "inline",
			segments: makeSegments(goodInline),
		},
		{
			desc:     "remote",
			segments: makeSegments(goodRemote),
		},
		{
			desc:     "remote+inline",
			segments: makeSegments(goodRemote, goodInline),
		},
		{
			desc:     "remote+remote",
			segments: makeSegments(goodRemote, goodRemote),
		},
		{
			desc:           "source fails on first segment",
			segments:       makeSegments(blockWaitRemote, blockWaitRemote),
			sourceErr:      errs.New("source failed on first"),
			sourceErrAfter: 0,
			expectErr:      "source failed on first",
		},
		{
			desc:                    "source fails on second segment",
			segments:                makeSegments(blockWaitRemote, blockWaitRemote),
			sourceErr:               errs.New("source failed on second"),
			sourceErrAfter:          1,
			expectBeginObject:       true,
			expectBeginDeleteObject: true,
			expectErr:               "source failed on second",
		},
		{
			desc:                    "failed segment upload begin",
			segments:                makeSegments(goodRemote, badBeginRemote),
			blockNext:               true,
			expectBeginObject:       true,
			expectBeginDeleteObject: true,
			expectErr:               "begin failed",
		},
		{
			desc:                    "failed segment upload wait",
			segments:                makeSegments(goodRemote, badWaitRemote),
			blockNext:               true,
			expectBeginObject:       true,
			expectBeginDeleteObject: true,
			expectErr:               "wait failed",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var (
				encMeta         = encryptedMetadata{}
				segmentSource   = &segmentSource{segments: tc.segments, blockNext: tc.blockNext, err: tc.sourceErr, errAfter: tc.sourceErrAfter}
				segmentUploader = segmentUploader{}
				miBatcher       = newMetainfoBatcher(t, false, len(tc.segments))
			)
			info, err := UploadObject(context.Background(), segmentSource, segmentUploader, miBatcher, beginObject, encMeta)
			if tc.expectErr != "" {
				require.NoError(t, miBatcher.CheckObject(tc.expectBeginObject, tc.expectCommitObject, tc.expectBeginDeleteObject))
				require.EqualError(t, err, tc.expectErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, Info{
				PlainSize: segmentsPlainSize(len(tc.segments)),
			}, info)

			// Assert that the expected operations took place on the metainfo store
			require.NoError(t, miBatcher.CheckObject(true, true, false))
			require.NoError(t, miBatcher.CheckSegments(tc.segments))

			// Assert that all segments were marked as "done" reading with the result error
			for i, segment := range tc.segments {
				require.NoError(t, segment.(interface{ Check(error) error }).Check(err), "segment %d failed check", i)
			}
		})
	}
}

func TestUploadPart(t *testing.T) {
	for _, tc := range []struct {
		desc           string
		segments       []splitter.Segment
		blockNext      bool
		sourceErr      error
		sourceErrAfter int
		expectErr      string
	}{
		{
			desc:      "source must provide at least one segment",
			expectErr: "programmer error: there should always be at least one segment",
		},
		{
			desc:     "inline",
			segments: makeSegments(goodInline),
		},
		{
			desc:     "remote",
			segments: makeSegments(goodRemote),
		},
		{
			desc:     "remote+inline",
			segments: makeSegments(goodRemote, goodInline),
		},
		{
			desc:     "remote+remote",
			segments: makeSegments(goodRemote, goodRemote),
		},
		{
			desc:           "source fails on first segment",
			segments:       makeSegments(blockWaitRemote, blockWaitRemote),
			sourceErr:      errs.New("source failed on first"),
			sourceErrAfter: 0,
			expectErr:      "source failed on first",
		},
		{
			desc:           "source fails on second segment",
			segments:       makeSegments(blockWaitRemote, blockWaitRemote),
			sourceErr:      errs.New("source failed on second"),
			sourceErrAfter: 1,
			expectErr:      "source failed on second",
		},
		{
			desc:      "failed segment upload begin",
			segments:  makeSegments(goodRemote, badBeginRemote),
			blockNext: true,
			expectErr: "begin failed",
		},
		{
			desc:      "failed segment upload wait",
			segments:  makeSegments(goodRemote, badWaitRemote),
			blockNext: true,
			expectErr: "wait failed",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var (
				segmentSource   = &segmentSource{segments: tc.segments, blockNext: tc.blockNext, err: tc.sourceErr, errAfter: tc.sourceErrAfter}
				segmentUploader = segmentUploader{}
				miBatcher       = newMetainfoBatcher(t, true, len(tc.segments))
			)

			eTagCh := make(chan []byte, 1)
			eTagCh <- eTag

			info, err := UploadPart(context.Background(), segmentSource, segmentUploader, miBatcher, streamID, eTagCh)
			if tc.expectErr != "" {
				require.NoError(t, miBatcher.CheckObject(false, false, false))
				require.EqualError(t, err, tc.expectErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, Info{
				PlainSize: segmentsPlainSize(len(tc.segments)),
			}, info)

			// Assert that the expected operations took place on the metainfo store
			require.NoError(t, miBatcher.CheckObject(false, false, false))
			require.NoError(t, miBatcher.CheckSegments(tc.segments))

			// Assert that all segments were marked as "done" reading with the result error
			for i, segment := range tc.segments {
				require.NoError(t, segment.(interface{ Check(error) error }).Check(err), "segment %d failed check", i)
			}
		})
	}
}

type segmentSource struct {
	next      int
	segments  []splitter.Segment
	blockNext bool
	err       error
	errAfter  int
}

func (src *segmentSource) Next(ctx context.Context) (segment splitter.Segment, err error) {
	switch {
	case src.err != nil && src.next == src.errAfter:
		return nil, src.err
	case src.next < len(src.segments):
		segment := src.segments[src.next]
		src.next++
		return segment, nil
	case src.blockNext:
		<-ctx.Done()
		return nil, ctx.Err()
	default:
		return nil, nil
	}
}

type segmentType int

const (
	goodInline segmentType = iota
	goodRemote
	badBeginRemote
	badWaitRemote
	blockWaitRemote
)

func makeSegments(types ...segmentType) []splitter.Segment {
	var ss []splitter.Segment
	for i, typ := range types {
		switch typ {
		case goodInline:
			ss = append(ss, &segment{inline: true, index: i})
		case goodRemote:
			ss = append(ss, &segment{inline: false, index: i})
		case badBeginRemote:
			ss = append(ss, &segment{inline: false, index: i, beginErr: errs.New("begin failed")})
		case badWaitRemote:
			ss = append(ss, &segment{inline: false, index: i, waitErr: errs.New("wait failed")})
		case blockWaitRemote:
			ss = append(ss, &segment{inline: false, index: i, blockWait: true})
		}
	}
	return ss
}

type segment struct {
	inline    bool
	index     int
	done      *error
	beginErr  error
	waitErr   error
	blockWait bool
	uploadCtx context.Context
	canceled  bool
}

func (s *segment) Begin() metaclient.BatchItem {
	if s.inline {
		return &metaclient.MakeInlineSegmentParams{
			Position:  s.Position(),
			PlainSize: int64(s.index),
		}
	}
	return &metaclient.BeginSegmentParams{
		Position: s.Position(),
	}
}

func (s *segment) Position() metaclient.SegmentPosition {
	return metaclient.SegmentPosition{Index: int32(s.index)}
}
func (s *segment) Inline() bool      { return s.inline }
func (s *segment) Reader() io.Reader { return strings.NewReader("HELLO") }

func (s *segment) Finalize() *splitter.SegmentInfo {
	return &splitter.SegmentInfo{
		PlainSize: int64(s.index),
	}
}

func (s *segment) DoneReading(err error) { s.done = &err }

func (s *segment) Check(uploadErr error) error {
	switch {
	case s.done == nil:
		return errs.New("expected DoneReading(%v) but never called", uploadErr)
	case !errors.Is(*s.done, uploadErr):
		return errs.New("expected DoneReading(%v) but got DoneReading(%v)", uploadErr, *s.done)
	case s.blockWait && !s.canceled:
		return errs.New("expected upload to be canceled")
	}
	return nil
}

func (s *segment) EncryptETag(eTagIn []byte) ([]byte, error) {
	if !bytes.Equal(eTagIn, eTag) {
		return nil, errs.New("expected eTag %q but got %q", string(eTag), string(eTagIn))
	}
	return encryptETag(s.index), nil
}

func (s *segment) beginUpload(ctx context.Context) (SegmentUpload, error) {
	if s.inline {
		return nil, errs.New("programmer error: inline segments should not be uploaded")
	}
	if s.beginErr != nil {
		return nil, s.beginErr
	}
	if s.uploadCtx != nil {
		return nil, errs.New("programmer error: upload already began")
	}
	s.uploadCtx = ctx
	return s, nil
}

func (s *segment) Wait() (*metaclient.CommitSegmentParams, error) {
	if s.inline {
		return nil, errs.New("programmer error: Wait should not be called on an inline segment")
	}
	if s.uploadCtx == nil {
		return nil, errs.New("programmer error: Wait called before upload was began")
	}
	if s.waitErr != nil {
		return nil, s.waitErr
	}
	if s.blockWait {
		<-s.uploadCtx.Done()
		s.canceled = true
		return nil, s.uploadCtx.Err()
	}
	return &metaclient.CommitSegmentParams{
		PlainSize: int64(s.index),
	}, nil
}

type metainfoBatcher struct {
	t                 *testing.T
	partUpload        bool
	lastSegmentIndex  int32
	batchCount        int
	beginObject       int
	commitObject      int
	beginDeleteObject int
	beginSegment      map[int32]struct{}
	commitSegment     map[int32]struct{}
	makeInlineSegment map[int32]struct{}
}

func newMetainfoBatcher(t *testing.T, partUpload bool, segmentCount int) *metainfoBatcher {
	return &metainfoBatcher{
		t:                 t,
		partUpload:        partUpload,
		lastSegmentIndex:  int32(segmentCount - 1),
		beginSegment:      make(map[int32]struct{}),
		commitSegment:     make(map[int32]struct{}),
		makeInlineSegment: make(map[int32]struct{}),
	}
}

func (m *metainfoBatcher) Batch(ctx context.Context, items ...metaclient.BatchItem) ([]metaclient.BatchResponse, error) {
	expectedStreamID := streamID

	m.batchCount++
	if !m.partUpload && m.batchCount == 1 {
		// object uploads won't include the stream ID on the first batch since
		// that batch contains the BeginObject request.
		expectedStreamID = nil
	}

	var responses []metaclient.BatchResponse
	for _, item := range items {
		req := item.BatchItem()
		resp := &pb.BatchResponseItem{}
		switch req := req.Request.(type) {
		case *pb.BatchRequestItem_ObjectBegin:
			assert.Equal(m.t, &pb.BeginObjectRequest{
				Bucket:               bucket,
				EncryptedObjectKey:   encryptedObjectKey,
				RedundancyScheme:     &pb.RedundancyScheme{},
				EncryptionParameters: &pb.EncryptionParameters{},
			}, req.ObjectBegin)
			m.beginObject++
			resp.Response = &pb.BatchResponseItem_ObjectBegin{ObjectBegin: &pb.BeginObjectResponse{StreamId: streamID}}

		case *pb.BatchRequestItem_ObjectCommit:
			assert.Equal(m.t, &pb.CommitObjectRequest{
				StreamId:                      expectedStreamID,
				EncryptedMetadataNonce:        encMetadataKeyNonce,
				EncryptedMetadataEncryptedKey: encMetadataKey,
				EncryptedMetadata:             encryptMetadata(int64(m.lastSegmentIndex)),
			}, req.ObjectCommit)
			m.commitObject++
			resp.Response = &pb.BatchResponseItem_ObjectCommit{ObjectCommit: &pb.CommitObjectResponse{
				Object: &pb.Object{
					PlainSize: segmentsPlainSize(len(m.makeInlineSegment) + len(m.commitSegment)),
				},
			}}

		case *pb.BatchRequestItem_ObjectBeginDelete:
			assert.Equal(m.t, &pb.BeginDeleteObjectRequest{
				Bucket:             bucket,
				EncryptedObjectKey: encryptedObjectKey,
				StreamId:           &streamID,
				Version:            1,
				Status:             int32(pb.Object_UPLOADING),
			}, req.ObjectBeginDelete)
			m.beginDeleteObject++
			resp.Response = &pb.BatchResponseItem_ObjectBeginDelete{ObjectBeginDelete: &pb.BeginDeleteObjectResponse{}}

		case *pb.BatchRequestItem_SegmentBegin:
			segmentIndex := req.SegmentBegin.Position.Index
			assert.Equal(m.t, req.SegmentBegin.StreamId, expectedStreamID, "unexpected stream ID on segment %d", segmentIndex)
			m.beginSegment[segmentIndex] = struct{}{}
			resp.Response = &pb.BatchResponseItem_SegmentBegin{SegmentBegin: &pb.BeginSegmentResponse{}}

		case *pb.BatchRequestItem_SegmentCommit:
			segmentIndex := int32(req.SegmentCommit.PlainSize)
			m.assertEncryptedETag(segmentIndex, req.SegmentCommit.EncryptedETag)
			m.commitSegment[segmentIndex] = struct{}{}
			resp.Response = &pb.BatchResponseItem_SegmentCommit{SegmentCommit: &pb.CommitSegmentResponse{}}

		case *pb.BatchRequestItem_SegmentMakeInline:
			segmentIndex := req.SegmentMakeInline.Position.Index
			m.assertEncryptedETag(segmentIndex, req.SegmentMakeInline.EncryptedETag)
			m.makeInlineSegment[segmentIndex] = struct{}{}
			resp.Response = &pb.BatchResponseItem_SegmentMakeInline{SegmentMakeInline: &pb.MakeInlineSegmentResponse{}}
		default:
			return nil, errs.New("unexpected batch request type %T", req)
		}
		responses = append(responses, metaclient.MakeBatchResponse(req, resp))
	}
	return responses, nil
}

func (m *metainfoBatcher) CheckObject(expectBeginObject, expectCommitObject, expectBeginDeleteObject bool) error {
	var eg errs.Group

	checkCall := func(rpc string, callCount int, expectCalled bool) {
		expectedCallCount := 0
		if expectCalled {
			expectedCallCount = 1
		}
		if callCount != expectedCallCount {
			eg.Add(errs.New("expected %d %s(s) but got %d", expectedCallCount, rpc, callCount))
		}
	}

	checkCall("BeginObject", m.beginObject, expectBeginObject)
	checkCall("CommitObject", m.commitObject, expectCommitObject)
	checkCall("BeginDeleteObject", m.beginDeleteObject, expectBeginDeleteObject)

	return eg.Err()
}

func (m *metainfoBatcher) CheckSegments(segments []splitter.Segment) error {
	var eg errs.Group

	if len(segments) != len(m.commitSegment)+len(m.makeInlineSegment) {
		eg.Add(errs.New("mismatched number of segments made/committed: expected %d but got %d remote and %d inline", len(segments), len(m.commitSegment), len(m.makeInlineSegment)))
	}

	for i, segment := range segments {
		index := segment.Position().Index
		if i != int(index) {
			eg.Add(errs.New("segment %d had unexpected index %d", i, index))
		}
		if segment.Inline() {
			if _, ok := m.makeInlineSegment[index]; !ok {
				eg.Add(errs.New("segment %d (inline) was never made", i))
			}
		} else {
			if _, ok := m.beginSegment[index]; !ok {
				eg.Add(errs.New("segment %d (remote) was never began", i))
			}
			if _, ok := m.commitSegment[index]; !ok {
				eg.Add(errs.New("segment %d (remote) was never committed", i))
			}
		}
	}

	return eg.Err()
}

func (m *metainfoBatcher) assertEncryptedETag(segmentIndex int32, encryptedETag []byte) {
	// Last segment in a part upload needs to include the encrypted ETag
	if m.partUpload && segmentIndex == m.lastSegmentIndex {
		assert.Equal(m.t, encryptETag(int(segmentIndex)), encryptedETag, "unexpected encrypted eTag on segment %d", segmentIndex)
	} else {
		assert.Nil(m.t, encryptedETag, "unexpected encrypted eTag on segment %d", segmentIndex)
	}
}

type segmentUploader struct{}

func (segmentUploader) Begin(ctx context.Context, resp *metaclient.BeginSegmentResponse, opaque splitter.Segment) (SegmentUpload, error) {
	seg := opaque.(*segment)
	return seg.beginUpload(ctx)
}

type encryptedMetadata struct{}

func (encryptedMetadata) EncryptedMetadata(lastSegmentSize int64) (data []byte, encKey *storj.EncryptedPrivateKey, nonce *storj.Nonce, err error) {
	return encryptMetadata(lastSegmentSize), &encMetadataKey, &encMetadataKeyNonce, nil
}

func mustNewStreamID() storj.StreamID {
	streamID, err := pb.Marshal(&pb.SatStreamID{
		CreationDate: creationDate,
	})
	if err != nil {
		panic(err)
	}
	return storj.StreamID(streamID)
}

func segmentsPlainSize(numSegments int) int64 {
	var plainSize int64
	for i := 0; i < numSegments; i++ {
		plainSize += segmentPlainSize(i)
	}
	return plainSize
}

func segmentPlainSize(index int) int64 {
	return int64(index)
}

func encryptETag(index int) []byte {
	return []byte(fmt.Sprintf("%s-%d", string(eTag), index))
}

func encryptMetadata(lastSegmentSize int64) []byte {
	return []byte(fmt.Sprintf("ENCRYPTED-METADATA-%d", lastSegmentSize))
}
