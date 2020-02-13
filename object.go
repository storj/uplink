// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"time"

	"storj.io/common/storj"
)

// Object contains information about an object.
type Object struct {
	Key string

	Info     ObjectInfo
	Standard StandardMetadata
	Custom   CustomMetadata
}

// ObjectInfo contains information about the object that cannot be changed directly.
type ObjectInfo struct {
	Created time.Time
	Expires time.Time
}

// StandardMetadata is user metadata for standard information for web and files.
type StandardMetadata struct {
	ContentLength int64
	ContentType   string
	ETag          string

	FileCreated     time.Time
	FileModified    time.Time
	FilePermissions uint32
}

// CustomMetadata contains custom user metadata about the object.
type CustomMetadata map[string]string

// StatObject returns information about an object at the specific key.
func (project *Project) StatObject(ctx context.Context, bucket, key string) (_ *Object, err error) {
	defer mon.Task()(&ctx)(&err)

	b := storj.Bucket{Name: bucket}
	obj, err := project.db.GetObject(ctx, b, key)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return convertObject(&obj), nil
}

// DeleteObject deletes the object at the specific key.
func (project *Project) DeleteObject(ctx context.Context, bucket, key string) (err error) {
	defer mon.Task()(&ctx)(&err)

	b := storj.Bucket{Name: bucket}
	err = project.db.DeleteObject(ctx, b, key)

	return Error.Wrap(err)
}

// convertObject converts storj.Object to uplink.Object.
func convertObject(obj *storj.Object) *Object {
	return &Object{
		Key: obj.Path,
		Info: ObjectInfo{
			Created: obj.Created,
			Expires: obj.Expires,
		},
		Custom: obj.Metadata,
	}
}
