// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package streamupload

import (
	"context"

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"golang.org/x/sync/errgroup"

	"storj.io/common/context2"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams/batchaggregator"
	"storj.io/uplink/private/storage/streams/segmenttracker"
	"storj.io/uplink/private/storage/streams/splitter"
	"storj.io/uplink/private/storage/streams/streambatcher"
)

var (
	mon = monkit.Package()
)

// SegmentSource is a source of segments to be uploaded.
type SegmentSource interface {
	// Next returns the next segment. It will return all-nil when there are no
	// more segments left.
	Next(context.Context) (splitter.Segment, error)
}

// SegmentUploader uploads a single remote segment of the stream.
type SegmentUploader interface {
	// Begin starts an Upload for a single remote segment of the stream.
	// Callers can wait for the upload to finish using the Wait() method.
	Begin(ctx context.Context, resp *metaclient.BeginSegmentResponse, segment splitter.Segment) (SegmentUpload, error)
}

// SegmentUpload is an upload for a single remote segment of the stream.
type SegmentUpload interface {
	// Wait waits until the segment is uploaded and returns the request needed
	// to commit the segment to the metainfo store.
	Wait() (*metaclient.CommitSegmentParams, error)
}

// EncryptedMetadata is used to encrypt the metadata from the object.
type EncryptedMetadata interface {
	// EncryptedMetadata creates stream metadata, including the size of the
	// final segment. The stream metadata is encrypted and returned. Also
	// returned is an encrypted version of the key used to encrypt the metadata
	// as well as the nonce used to encrypt the key.
	EncryptedMetadata(lastSegmentSize int64) (data []byte, encKey *storj.EncryptedPrivateKey, nonce *storj.Nonce, err error)
}

// Info is the information about the stream upload.
type Info = streambatcher.Info

// UploadObject uploads a stream of segments as an object identified by the
// given beginObject response.
func UploadObject(ctx context.Context, segmentSource SegmentSource, segmentUploader SegmentUploader, miBatcher metaclient.Batcher, beginObject *metaclient.BeginObjectParams, encMeta EncryptedMetadata) (Info, error) {
	return uploadSegments(ctx, segmentSource, segmentUploader, miBatcher, beginObject, encMeta, nil, nil)
}

// UploadPart uploads a stream of segments as a part of a multipart upload
// identified by the given streamID.
func UploadPart(ctx context.Context, segmentSource SegmentSource, segmentUploader SegmentUploader, miBatcher metaclient.Batcher, streamID storj.StreamID, eTagCh <-chan []byte) (Info, error) {
	return uploadSegments(ctx, segmentSource, segmentUploader, miBatcher, nil, nil, streamID, eTagCh)
}

func uploadSegments(ctx context.Context, segmentSource SegmentSource, segmentUploader SegmentUploader, miBatcher metaclient.Batcher, beginObject *metaclient.BeginObjectParams, encMeta EncryptedMetadata, streamID storj.StreamID, eTagCh <-chan []byte) (_ Info, err error) {
	batcher := streambatcher.New(miBatcher, streamID)
	aggregator := batchaggregator.New(batcher)

	if beginObject != nil {
		aggregator.Schedule(beginObject)
		defer func() {
			if err != nil {
				if batcherStreamID := batcher.StreamID(); !batcherStreamID.IsZero() {
					if deleteErr := deleteCancelledObject(ctx, miBatcher, beginObject, batcherStreamID); deleteErr != nil {
						mon.Event("failed to delete cancelled object")
					}
				}
			}
		}()
	}

	tracker := segmenttracker.New(aggregator, eTagCh)

	var segments []splitter.Segment
	defer func() {
		for _, segment := range segments {
			segment.DoneReading(err)
		}
	}()

	var eg errgroup.Group
	defer func() { _ = eg.Wait() }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		segment, err := segmentSource.Next(ctx)
		if err != nil {
			return Info{}, err
		} else if segment == nil {
			break
		}
		segments = append(segments, segment)

		if segment.Inline() {
			tracker.SegmentDone(segment, segment.Begin())
			break
		}

		resp, err := aggregator.ScheduleAndFlush(ctx, segment.Begin())
		if err != nil {
			return Info{}, err
		}

		beginSegment, err := resp.BeginSegment()
		if err != nil {
			return Info{}, err
		}

		upload, err := segmentUploader.Begin(ctx, &beginSegment, segment)
		if err != nil {
			return Info{}, err
		}

		eg.Go(func() error {
			commitSegment, err := upload.Wait()
			if err != nil {
				// an upload has failed so we should cancel the rest of the uploads
				cancel()
				return err
			}
			tracker.SegmentDone(segment, commitSegment)
			return nil
		})
	}

	if len(segments) == 0 {
		return Info{}, errs.New("programmer error: there should always be at least one segment")
	}

	lastSegment := segments[len(segments)-1]

	tracker.SegmentsScheduled(lastSegment)

	if err := eg.Wait(); err != nil {
		return Info{}, err
	}

	if err := tracker.Flush(ctx); err != nil {
		return Info{}, err
	}

	// we need to schedule a commit object if we had a begin object
	if beginObject != nil {
		commitObject, err := createCommitObjectParams(lastSegment, encMeta)
		if err != nil {
			return Info{}, err
		}
		aggregator.Schedule(commitObject)
	}

	if err := aggregator.Flush(ctx); err != nil {
		return Info{}, err
	}

	return batcher.Info()
}

func createCommitObjectParams(lastSegment splitter.Segment, encMeta EncryptedMetadata) (*metaclient.CommitObjectParams, error) {
	info := lastSegment.Finalize()

	encryptedMetadata, encryptedMetadataKey, encryptedMetadataKeyNonce, err := encMeta.EncryptedMetadata(info.PlainSize)
	if err != nil {
		return nil, err
	}

	return &metaclient.CommitObjectParams{
		StreamID:                      nil, // set by the stream batcher
		EncryptedMetadataNonce:        *encryptedMetadataKeyNonce,
		EncryptedMetadataEncryptedKey: *encryptedMetadataKey,
		EncryptedMetadata:             encryptedMetadata,
	}, nil
}

func deleteCancelledObject(ctx context.Context, batcher metaclient.Batcher, beginObject *metaclient.BeginObjectParams, streamID storj.StreamID) (err error) {
	defer mon.Task()(&ctx)(&err)

	ctx = context2.WithoutCancellation(ctx)
	_, err = batcher.Batch(ctx, &metaclient.BeginDeleteObjectParams{
		Bucket:             beginObject.Bucket,
		EncryptedObjectKey: beginObject.EncryptedObjectKey,
		// TODO remove it or set to 0 when satellite side will be fixed
		Version:  1,
		StreamID: streamID,
		Status:   int32(pb.Object_UPLOADING),
	})
	return err
}
