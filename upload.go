// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"github.com/zeebo/errs"

	"storj.io/common/storj"
	"storj.io/uplink/stream"
)

// ErrUploadDone is returned when either Abort or Commit has already been called.
var ErrUploadDone = errs.Class("upload done")

// Upload is a partial upload to Storj Network.
type Upload interface {
	// Info returns the last information about the uploaded object.
	// Info() Object

	Write(p []byte) (n int, err error)
	// ReadFrom(r io.Reader) (int64, error)

	// Commit commits data to the store.
	//
	// Returns ErrUploadDone when either Abort or Commit has already been called.
	Commit(opts *CommitOptions) error
	// Abort aborts partial upload.
	//
	// Returns ErrUploadDone when either Abort or Commit has already been called.
	// Abort() error
}

// CommitOptions allows to specify additional metadata about the object.
// The options will override any previous data.
type CommitOptions struct {
	// ContentType string
	// User        Metadata
}

// UploadObject starts an upload to the specified key.
func (project *Project) UploadObject(ctx context.Context, bucket, key string) (Upload, error) {
	return (&UploadRequest{
		Bucket: bucket,
		Key:    key,
	}).Do(ctx, project)
}

// UploadRequest specifies all parameters for an upload.
type UploadRequest struct {
	Bucket string
	Key    string

	// TODO: Add upload options later
	// Expires     time.Time
	// ContentType string
	// User        Metadata
}

// Do executes the upload request.
func (request *UploadRequest) Do(ctx context.Context, project *Project) (_ Upload, err error) {
	defer mon.Task()(&ctx)(&err)

	b := storj.Bucket{Name: request.Bucket, PathCipher: storj.EncAESGCM}
	obj, err := project.db.CreateObject(ctx, b, request.Key, nil)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	mutableStream, err := obj.CreateStream(ctx)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return &upload{
		upload: stream.NewUpload(ctx, mutableStream, project.streams),
	}, nil
}

type upload struct {
	upload *stream.Upload
}

func (upload *upload) Write(p []byte) (n int, err error) {
	return upload.upload.Write(p)
}

func (upload *upload) Commit(opts *CommitOptions) error {
	return upload.upload.Close()
}
