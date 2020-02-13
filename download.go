// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"storj.io/common/storj"
	"storj.io/uplink/stream"
)

// DownloadObject starts an download to the specified key.
func (project *Project) DownloadObject(ctx context.Context, bucket, key string) (*Download, error) {
	return (&DownloadRequest{
		Bucket: bucket,
		Key:    key,
		Offset: 0,
		Length: -1,
	}).Do(ctx, project)
}

// DownloadRequest specifies all parameters for a download.
//
// Downloads at most Length bytes.
// If Length is negative it will read until the end of the blob.
type DownloadRequest struct {
	Bucket string
	Key    string

	Offset int64
	Length int64
}

// Do executes the download request.
func (request *DownloadRequest) Do(ctx context.Context, project *Project) (_ *Download, err error) {
	defer mon.Task()(&ctx)(&err)

	b := storj.Bucket{Name: request.Bucket}

	obj, err := project.db.GetObjectExtended(ctx, b, request.Key)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	objectStream, err := project.db.GetObjectExtendedStream(ctx, b, obj)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return &Download{
		download: stream.NewDownloadRange(ctx, objectStream, project.streams, request.Offset, request.Length),
		object:   convertObjectExtended(&obj),
	}, nil
}

// Download is a partial download to Storj Network.
type Download struct {
	download *stream.Download
	object   *Object
}

// Info returns the last information about the object.
func (download *Download) Info() *Object {
	return download.object
}

// Read downloads up to len(p) bytes into p from the object's data stream.
// It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
func (download *Download) Read(data []byte) (n int, err error) {
	return download.download.Read(data)
}

// Close closes the reader of the download.
func (download *Download) Close() error {
	return download.download.Close()
}
