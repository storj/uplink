// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/storj"
)

// ErrObjectKeyInvalid is returned when the object key is invalid.
var ErrObjectKeyInvalid = errs.Class("object key invalid")

// ErrObjectNotFound is returned when the object is not found.
var ErrObjectNotFound = errs.Class("object not found")

// Object contains information about an object.
type Object struct {
	Key string
	// IsPrefix indicates whether the Key is a prefix for other objects.
	IsPrefix bool

	System SystemMetadata
	Custom CustomMetadata
}

// SystemMetadata contains information about the object that cannot be changed directly.
type SystemMetadata struct {
	Created       time.Time
	Expires       time.Time
	ContentLength int64
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
func (project *Project) StatObject(ctx context.Context, bucket, key string) (info *Object, err error) {
	defer mon.Task()(&ctx)(&err)

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

	return convertObject(&obj), nil
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
			return nil, ErrObjectNotFound.New("%v", key)
		}
		return nil, Error.Wrap(err)
	}
	return object, nil
}

// convertObject converts storj.Object to uplink.Object.
func convertObject(obj *storj.Object) *Object {
	return &Object{
		Key: obj.Path,
		System: SystemMetadata{
			Created:       obj.Created,
			Expires:       obj.Expires,
			ContentLength: obj.Size,
		},
		Custom: obj.Metadata,
	}
}
