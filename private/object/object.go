// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package object

import (
	"context"
	"errors"
	"io"
	"sync"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/memory"
	"storj.io/common/ranger"
	"storj.io/common/storj"
	"storj.io/common/sync2"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams"
)

var mon = monkit.Package()

// Error is default error class for uplink.
var packageError = errs.Class("object")

// IPSummary contains information about the object IP-s.
type IPSummary = metaclient.GetObjectIPsResponse

// GetObjectIPs returns the IP-s for a given object.
//
// TODO: delete, once we have stopped using it.
func GetObjectIPs(ctx context.Context, config uplink.Config, access *uplink.Access, bucket, key string) (_ [][]byte, err error) {
	summary, err := GetObjectIPSummary(ctx, config, access, bucket, key)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	return summary.IPPorts, nil
}

// GetObjectIPSummary returns the object IP summary.
func GetObjectIPSummary(ctx context.Context, config uplink.Config, access *uplink.Access, bucket, key string) (_ *IPSummary, err error) {
	dialer, err := expose.ConfigGetDialer(config, ctx)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, dialer.Pool.Close()) }()

	metainfoClient, err := metaclient.DialNodeURL(ctx, dialer, access.SatelliteAddress(), expose.AccessGetAPIKey(access), config.UserAgent)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	db := metaclient.New(metainfoClient, expose.AccessGetEncAccess(access).Store)

	summary, err := db.GetObjectIPs(ctx, metaclient.Bucket{Name: bucket}, key)
	return summary, packageError.Wrap(err)
}

// WriterAt interface for writing data into specific offset.
// Contains also method to truncate writer to final object size.
type WriterAt interface {
	io.WriterAt

	Truncate(int64) error
}

// DownloadObjectAtOptions options for DownloadObjectAt.
type DownloadObjectAtOptions struct {
	Concurrency int
}

// DownloadObjectAt downloads object into specified io.WriterAt.
func DownloadObjectAt(ctx context.Context, project *uplink.Project, bucket, key string, writer WriterAt, opts *DownloadObjectAtOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	if project == nil {
		return errs.New("project is nil")
	}
	if bucket == "" {
		return convertKnownErrors(metaclient.ErrNoBucket.New(""), bucket, key)
	}
	if key == "" {
		return convertKnownErrors(metaclient.ErrNoPath.New(""), bucket, key)
	}

	// TODO add range support
	var downloadOpts metaclient.DownloadOptions
	downloadOpts.Range = metaclient.StreamRange{
		Mode: metaclient.StreamRangeAll,
	}

	if opts == nil {
		opts = &DownloadObjectAtOptions{
			Concurrency: 2,
		}
	}

	db, err := dialMetainfoDBWithProject(ctx, project)
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	info, err := db.DownloadObject(ctx, bucket, key, downloadOpts)
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}

	if info.Object.Size == 0 {
		return nil
	}

	// TODO use FixedSegmentSize for old style objects
	if len(info.ListSegments.Items) > 0 &&
		(info.ListSegments.Items[0].PlainSize == 0 && info.ListSegments.Items[0].PlainOffset == 0) {
		return packageError.New("old style objects not supported yet")
	}

	if err := writer.Truncate(info.Object.Size); err != nil {
		return packageError.Wrap(err)
	}

	// TODO combine it somehow with this same code from streams.Get()
	// download all missing segments
	for info.ListSegments.More {
		var cursor storj.SegmentPosition
		if len(info.ListSegments.Items) > 0 {
			last := info.ListSegments.Items[len(info.ListSegments.Items)-1]
			cursor = last.Position
		}

		result, err := db.ListSegments(ctx, metaclient.ListSegmentsParams{
			StreamID: info.Object.ID,
			Cursor:   cursor,
			Range:    info.Range,
		})
		if err != nil {
			return convertKnownErrors(err, bucket, key)
		}

		info.ListSegments.Items = append(info.ListSegments.Items, result.Items...)
		info.ListSegments.More = result.More
	}

	streams, err := getStreamsStoreWithProject(ctx, project)
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}

	ranger, err := streams.Get(ctx, bucket, key, info)
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}

	// Return the connection to the pool as soon as we can.
	if err := db.Close(); err != nil {
		return convertKnownErrors(err, bucket, key)
	}

	var errGroup errs.Group
	var errGroupMu sync.Mutex

	cancelCtx, cancel := context.WithCancel(ctx)
	addError := func(err error) {
		errGroupMu.Lock()
		defer errGroupMu.Unlock()

		errGroup.Add(err)
		cancel()
	}

	limiter := sync2.NewLimiter(opts.Concurrency)
	for _, segment := range info.ListSegments.Items {
		segment := segment

		// ignore empty segments because when object size is
		// multplication of max segment then it will have
		// empty inline segment at the end, not true for
		// multipart upload.
		if segment.PlainSize == 0 {
			continue
		}

		ok := limiter.Go(cancelCtx, func() {
			downloadSegment(cancelCtx, ranger, segment, writer, addError)
		})
		if !ok {
			break
		}
	}

	limiter.Wait()

	// TODO handle multiple errors in a nicer way
	return convertKnownErrors(errGroup.Err(), bucket, key)
}

func downloadSegment(ctx context.Context, ranger ranger.Ranger, segment metaclient.SegmentListItem, writer io.WriterAt, addError func(err error)) {
	reader, err := ranger.Range(ctx, segment.PlainOffset, segment.PlainSize)
	if err != nil {
		addError(err)
		return
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			addError(err)
		}
	}()

	offset := segment.PlainOffset
	buffer := make([]byte, 32*memory.KiB.Int())
	for {
		if err := ctx.Err(); err != nil {
			addError(err)
			return
		}

		read, err := reader.Read(buffer)
		if errors.Is(err, io.EOF) && read == 0 {
			return
		}
		if err != nil && !errors.Is(err, io.EOF) {
			addError(err)
			return
		}
		_, err = writer.WriteAt(buffer[:read], offset)
		if err != nil {
			addError(err)
			return
		}
		offset += int64(read)
	}
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoDBWithProject storj.io/uplink.dialMetainfoDBWithProject
func dialMetainfoDBWithProject(ctx context.Context, project *uplink.Project) (_ *metaclient.DB, err error)

//go:linkname getStreamsStoreWithProject storj.io/uplink.getStreamsStoreWithProject
func getStreamsStoreWithProject(ctx context.Context, project *uplink.Project) (_ *streams.Store, err error)
