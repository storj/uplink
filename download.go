// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"storj.io/common/storj"
	"storj.io/uplink/stream"
)

// Download is a partial download to Storj Network.
type Download interface {
	// Info returns the last information about the object.
	// Info() Object
	Read(data []byte) (n int, err error)
	Close() error
}

// DownloadObject starts an download to the specified key.
func (project *Project) DownloadObject(ctx context.Context, bucket, key string) (Download, error) {
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
func (request *DownloadRequest) Do(ctx context.Context, project *Project) (_ Download, err error) {
	defer mon.Task()(&ctx)(&err)

	b := storj.Bucket{Name: request.Bucket, PathCipher: storj.EncAESGCM}

	obj, err := project.db.GetObject(ctx, b, request.Key)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	objectStream, err := project.db.GetObjectStream(ctx, b, obj)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return &download{
		download: stream.NewDownloadRange(ctx, objectStream, project.streams, request.Offset, request.Length),
	}, nil
}

type download struct {
	download *stream.Download
}

func (download *download) Read(data []byte) (n int, err error) {
	return download.download.Read(data)
}

func (download *download) Close() error {
	return download.download.Close()
}
