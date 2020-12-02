// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"github.com/zeebo/errs"

	"storj.io/common/storj"
	"storj.io/uplink/private/storage/streams"
	"storj.io/uplink/private/stream"
)

// DownloadOptions contains additional options for downloading.
type DownloadOptions struct {
	Offset int64
	// When Length is negative it will read until the end of the blob.
	Length int64
}

// DownloadObject starts a download from the specific key.
func (project *Project) DownloadObject(ctx context.Context, bucket, key string, options *DownloadOptions) (download *Download, err error) {
	defer mon.Func().RestartTrace(&ctx)(&err)

	if bucket == "" {
		return nil, errwrapf("%w (%q)", ErrBucketNameInvalid, bucket)
	}
	if key == "" {
		return nil, errwrapf("%w (%q)", ErrObjectKeyInvalid, key)
	}

	if options == nil {
		options = &DownloadOptions{
			Offset: 0,
			Length: -1,
		}
	}

	b := storj.Bucket{Name: bucket}

	// N.B. we always call dbCleanup which closes the db because
	// closing it earlier has the benefit of returning a connection to
	// the pool, so we try to do that as early as possible.

	db, err := project.getMetainfoDB(ctx)
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	obj, err := db.GetObject(ctx, b, key)
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}

	// Return the connection to the pool as soon as we can.
	if err := db.Close(); err != nil {
		return nil, packageError.Wrap(err)
	}

	streams, err := project.getStreamsStore(ctx)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	return &Download{
		streams:  streams,
		download: stream.NewDownloadRange(ctx, obj, streams, options.Offset, options.Length),
		object:   convertObject(&obj),
	}, nil
}

// Download is a download from Storj Network.
type Download struct {
	download *stream.Download
	object   *Object
	streams  *streams.Store
}

// Info returns the last information about the object.
func (download *Download) Info() *Object {
	return download.object
}

// Read downloads up to len(p) bytes into p from the object's data stream.
// It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
func (download *Download) Read(p []byte) (n int, err error) {
	return download.download.Read(p)
}

// Close closes the reader of the download.
func (download *Download) Close() error {
	return errs.Combine(
		download.download.Close(),
		download.streams.Close(),
	)
}
