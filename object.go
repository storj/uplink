// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/storj"
	"storj.io/uplink/metainfo/kvmetainfo"
)

// ErrObjectNotFound is returned when the object is not found.
var ErrObjectNotFound = errs.Class("object not found")

// Object contains information about an object.
type Object struct {
	Key string
	// IsPrefix indicates whether the Key is a prefix for other objects.
	IsPrefix bool

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

	FileCreated     time.Time
	FileModified    time.Time
	FilePermissions uint32

	// Unknown stores fields that this version of the client does not know about.
	// The client should copy this information verbatim when updating metadata
	// otherwise the unknown fields will be deleted.
	Unknown []byte
}

// Clone makes a deep clone.
func (meta StandardMetadata) Clone() StandardMetadata {
	clone := meta
	clone.Unknown = append([]byte{}, meta.Unknown...)
	return clone
}

// CustomMetadata contains custom user metadata about the object.
type CustomMetadata map[string]string

// Clone makes a deep clone.
func (meta CustomMetadata) Clone() CustomMetadata {
	r := CustomMetadata{}
	for k, v := range meta {
		r[k] = v
	}
	return r
}

// StatObject returns information about an object at the specific key.
func (project *Project) StatObject(ctx context.Context, bucket, key string) (_ *Object, err error) {
	defer mon.Task()(&ctx)(&err)

	b := storj.Bucket{Name: bucket}
	obj, err := project.db.GetObjectExtended(ctx, b, key)
	if err != nil {
		if storj.ErrObjectNotFound.Has(err) {
			return nil, ErrObjectNotFound.New(key)
		}
		return nil, Error.Wrap(err)
	}

	return convertObjectExtended(&obj), nil
}

// DeleteObject deletes the object at the specific key.
func (project *Project) DeleteObject(ctx context.Context, bucket, key string) (deleted *Object, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO: Ideally, this should be done on the satellite
	object, err := project.StatObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}

	b := storj.Bucket{Name: bucket}
	err = project.db.DeleteObject(ctx, b, key)
	if err != nil {
		if storj.ErrObjectNotFound.Has(err) {
			return nil, ErrObjectNotFound.New(key)
		}
		return nil, Error.Wrap(err)
	}
	return object, nil
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

// convertObjectExtended converts kvmetainfo.ObjectExtended to uplink.Object.
func convertObjectExtended(obj *kvmetainfo.ObjectExtended) *Object {
	return &Object{
		Key: obj.Path,
		Info: ObjectInfo{
			Created: obj.Info.Created,
			Expires: obj.Info.Expires,
		},
		Standard: StandardMetadata{
			ContentLength: obj.Standard.ContentLength,
			ContentType:   obj.Standard.ContentType,

			FileCreated:     obj.Standard.FileCreated,
			FileModified:    obj.Standard.FileModified,
			FilePermissions: obj.Standard.FilePermissions,

			Unknown: obj.Standard.Unknown,
		},
		Custom: obj.Custom,
	}
}
