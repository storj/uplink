// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"storj.io/common/storj"
	"storj.io/uplink/private/metainfo/kvmetainfo"
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
	defer mon.Task()(&ctx)(&err)

	if options == nil {
		options = &DownloadOptions{
			Offset: 0,
			Length: -1,
		}
	}

	b := storj.Bucket{Name: bucket}

	obj, err := project.db.GetObject(ctx, b, key)
	if err != nil {
		if storj.ErrNoPath.Has(err) {
			return nil, ErrObjectKeyInvalid.New("%v", key)
		} else if storj.ErrObjectNotFound.Has(err) {
			return nil, ErrObjectNotFound.New("%v", key)
		}
		return nil, Error.Wrap(err)
	}

	objectStream, err := project.db.GetObjectStream(ctx, b, obj)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return &Download{
		ctx:     ctx,
		stream:  objectStream,
		streams: project.streams,
		options: options,
		object:  convertObject(&obj),
	}, nil
}

// Download is a download from Storj Network.
type Download struct {
	download *stream.Download
	ctx      context.Context
	stream   kvmetainfo.ReadOnlyStream
	streams  streams.Store
	options  *DownloadOptions
	object   *Object
}

// Info returns the last information about the object.
func (download *Download) Info() *Object {
	return download.object
}

// Read downloads up to len(p) bytes into p from the object's data stream.
// It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
func (download *Download) Read(data []byte) (n int, err error) {
	if download.download == nil {
		download.download = stream.NewDownloadRange(download.ctx, download.stream, download.streams, download.options.Offset, download.options.Length)
	}
	return download.download.Read(data)
}

// Range allows to further specify the offset and length of the initial requested download.
// It returns a new Download on which one can just call Read() again.
func (download *Download) Range(opts *DownloadOptions) *Download {
	return &Download{
		download: nil,
		ctx:      download.ctx,
		stream:   download.stream,
		streams:  download.streams,
		options:  opts,
		object:   download.object,
	}
}

// Close closes the reader of the download.
func (download *Download) Close() error {
	return download.download.Close()
}
