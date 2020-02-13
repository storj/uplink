// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"sync/atomic"

	"github.com/zeebo/errs"

	"storj.io/common/storj"
	"storj.io/uplink/stream"
)

// ErrUploadDone is returned when either Abort or Commit has already been called.
var ErrUploadDone = errs.Class("upload done")

// UploadObject starts an upload to the specified key.
func (project *Project) UploadObject(ctx context.Context, bucket, key string) (*Upload, error) {
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
}

// Do executes the upload request.
func (request *UploadRequest) Do(ctx context.Context, project *Project) (_ *Upload, err error) {
	defer mon.Task()(&ctx)(&err)

	b := storj.Bucket{Name: request.Bucket}
	obj, err := project.db.CreateObject(ctx, b, request.Key, nil)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	info := obj.Info()
	mutableStream, err := obj.CreateStream(ctx)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	ctx, cancel := context.WithCancel(ctx)

	return &Upload{
		cancel: cancel,
		upload: stream.NewUpload(ctx, mutableStream, project.streams),
		object: convertObject(&info),
	}, nil
}

// Upload is a partial upload to Storj Network.
type Upload struct {
	aborted int32
	cancel  context.CancelFunc
	upload  *stream.Upload
	object  *Object
}

// Info returns the last information about the uploaded object.
func (upload *Upload) Info() *Object {
	meta := upload.upload.Meta()
	if meta != nil {
		upload.object.Info.Created = meta.Modified
	}
	return upload.object
}

// TODO: (upload *Upload) (Update|Set)Metadata(ctx context.Context, ???) error {

// Write uploads len(p) bytes from p to the object's data stream.
// It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
func (upload *Upload) Write(p []byte) (n int, err error) {
	return upload.upload.Write(p)
}

// Commit commits data to the store.
//
// Returns ErrUploadDone when either Abort or Commit has already been called.
func (upload *Upload) Commit() error {
	if atomic.LoadInt32(&upload.aborted) == 1 {
		return ErrUploadDone.New("already aborted")
	}

	err := upload.upload.Close()
	if err != nil && errs.Unwrap(err).Error() == "already closed" {
		return ErrUploadDone.New("already committed")
	}

	return nil
}

// Abort aborts partial upload.
//
// Returns ErrUploadDone when either Abort or Commit has already been called.
func (upload *Upload) Abort() error {
	if upload.upload.Meta() != nil {
		return ErrUploadDone.New("already committed")
	}

	if !atomic.CompareAndSwapInt32(&upload.aborted, 0, 1) {
		return ErrUploadDone.New("already aborted")
	}

	upload.cancel()
	return nil
}
