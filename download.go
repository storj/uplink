// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"errors"
	"go.opentelemetry.io/otel"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/paths"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams"
	"storj.io/uplink/private/stream"
)

// DownloadOptions contains additional options for downloading.
type DownloadOptions struct {
	// When Offset is negative it will read the suffix of the blob.
	// Combining negative offset and positive length is not supported.
	Offset int64
	// When Length is negative it will read until the end of the blob.
	Length int64
}

// DownloadObject starts a download from the specific key.
func (project *Project) DownloadObject(ctx context.Context, bucket, key string, options *DownloadOptions) (_ *Download, err error) {
	download := &Download{
		bucket: bucket,
		stats:  newOperationStats(ctx, project.access.satelliteURL),
	}

	ctx, span := otel.Tracer("uplink").Start(ctx, "Download")
	defer span.End()
	defer func() {
		if err != nil {
			download.stats.flagFailure(err)
		}
	}()
	defer download.stats.trackWorking()()
	pc, _, _, _ := runtime.Caller(0)
	ctx, span = otel.Tracer("uplink").Start(ctx, runtime.FuncForPC(pc).Name())
	defer span.End()

	if bucket == "" {
		return nil, errwrapf("%w (%q)", ErrBucketNameInvalid, bucket)
	}
	if key == "" {
		return nil, errwrapf("%w (%q)", ErrObjectKeyInvalid, key)
	}

	var opts metaclient.DownloadOptions
	switch {
	case options == nil:
		opts.Range = metaclient.StreamRange{
			Mode: metaclient.StreamRangeAll,
		}
	case options.Offset < 0:
		if options.Length >= 0 {
			return nil, packageError.New("suffix requires length to be negative, got %v", options.Length)
		}
		opts.Range = metaclient.StreamRange{
			Mode:   metaclient.StreamRangeSuffix,
			Suffix: -options.Offset,
		}
	case options.Length < 0:
		opts.Range = metaclient.StreamRange{
			Mode:  metaclient.StreamRangeStart,
			Start: options.Offset,
		}

	default:
		opts.Range = metaclient.StreamRange{
			Mode:  metaclient.StreamRangeStartLimit,
			Start: options.Offset,
			Limit: options.Offset + options.Length,
		}
	}

	// N.B. we always call dbCleanup which closes the db because
	// closing it earlier has the benefit of returning a connection to
	// the pool, so we try to do that as early as possible.

	db, err := project.dialMetainfoDB(ctx)
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	// TODO: handle DownloadObject & downloadInfo.ListSegments.More in the same location.
	//       currently this code is rather disjoint.

	objectDownload, err := db.DownloadObject(ctx, bucket, key, opts)
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}

	download.stats.encPath = objectDownload.EncPath

	// store this data so even failing events have the best chance of
	// reporting this.
	streamRange := objectDownload.Range
	download.sizes.offset = streamRange.Start
	download.sizes.length = streamRange.Limit - streamRange.Start
	download.sizes.total = objectDownload.Object.Size

	// Return the connection to the pool as soon as we can.
	if err := db.Close(); err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}

	streams, err := project.getStreamsStore(ctx)
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}
	download.streams = streams

	download.object = convertObject(&objectDownload.Object)
	download.download = stream.NewDownloadRange(ctx, objectDownload, streams, streamRange.Start, streamRange.Limit-streamRange.Start)
	return download, nil
}

// Download is a download from Storj Network.
type Download struct {
	mu       sync.Mutex
	download *stream.Download
	object   *Object
	bucket   string
	streams  *streams.Store

	sizes struct {
		offset, length, total int64
	}
	ttfb  time.Duration
	stats operationStats
	task  func(*error)
}

// Info returns the last information about the object.
func (download *Download) Info() *Object {
	return download.object
}

// Read downloads up to len(p) bytes into p from the object's data stream.
// It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
func (download *Download) Read(p []byte) (n int, err error) {
	track := download.stats.trackWorking()
	n, err = download.download.Read(p)
	download.mu.Lock()
	download.stats.bytes += int64(n)
	if err != nil && !errors.Is(err, io.EOF) {
		download.stats.flagFailure(err)
	}
	if download.ttfb == 0 && n > 0 {
		download.ttfb = time.Since(download.stats.start)
	}
	track()
	download.mu.Unlock()
	return n, convertKnownErrors(err, download.bucket, download.object.Key)
}

// Close closes the reader of the download.
func (download *Download) Close() error {
	track := download.stats.trackWorking()
	err := errs.Combine(
		download.download.Close(),
		download.streams.Close(),
	)
	download.mu.Lock()
	track()
	download.stats.flagFailure(err)
	download.mu.Unlock()
	return convertKnownErrors(err, download.bucket, download.object.Key)
}

func pathChecksum(encPath paths.Encrypted) []byte {
	mac := hmac.New(sha1.New, []byte(encPath.Raw()))
	_, err := mac.Write([]byte("event"))
	if err != nil {
		panic(err)
	}
	return mac.Sum(nil)[:16]
}
