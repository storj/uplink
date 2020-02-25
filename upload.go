// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/zeebo/errs"

	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metainfo/kvmetainfo"
	"storj.io/uplink/private/stream"
)

// ErrUploadDone is returned when either Abort or Commit has already been called.
var ErrUploadDone = errs.Class("upload done")

// UploadOptions contains additional options for uploading.
type UploadOptions struct {
	// When Expires is zero, there is no expiration.
	Expires time.Time
}

// UploadObject starts an upload to the specific key.
func (project *Project) UploadObject(ctx context.Context, bucket, key string, options *UploadOptions) (upload *Upload, err error) {
	defer mon.Task()(&ctx)(&err)

	if options == nil {
		options = &UploadOptions{}
	}

	b := storj.Bucket{Name: bucket}
	obj, err := project.db.CreateObject(ctx, b, key, nil)
	if err != nil {
		if storj.ErrNoPath.Has(err) {
			return nil, ErrObjectKeyInvalid.New("%v", key)
		}
		return nil, Error.Wrap(err)
	}

	info := obj.Info()
	mutableStream, err := obj.CreateStream(ctx)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	ctx, cancel := context.WithCancel(ctx)

	upload = &Upload{
		cancel: cancel,
		object: convertObject(&info),
	}
	upload.upload = stream.NewUpload(ctx, dynamicMetadata{
		MutableStream: mutableStream,
		object:        upload.object,
		expires:       options.Expires,
	}, project.streams)
	return upload, nil
}

// Upload is an upload to Storj Network.
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
		upload.object.System.Created = meta.Modified
	}
	return upload.object
}

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

	return err
}

// Abort aborts the upload.
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

// SetCustomMetadata updates custom metadata to be included with the object.
// If it is nil, it won't be modified.
func (upload *Upload) SetCustomMetadata(ctx context.Context, custom CustomMetadata) error {
	if atomic.LoadInt32(&upload.aborted) == 1 {
		return ErrUploadDone.New("upload aborted")
	}
	if upload.upload.Meta() != nil {
		return ErrUploadDone.New("already committed")
	}

	if custom != nil {
		upload.object.Custom = custom.Clone()
	}

	return nil
}

type dynamicMetadata struct {
	kvmetainfo.MutableStream
	object  *Object
	expires time.Time
}

func (meta dynamicMetadata) Metadata() ([]byte, error) {
	return proto.Marshal(&pb.SerializableMeta{
		UserDefined:   meta.object.Custom.Clone(),
		ContentLength: meta.object.System.ContentLength,
	})
}

func (meta dynamicMetadata) Expires() time.Time {
	return meta.expires
}
