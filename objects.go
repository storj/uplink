// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"storj.io/common/storj"
	"storj.io/uplink/metainfo/kvmetainfo"
)

// ObjectIteratorOptions defines iteration options.
type ObjectIteratorOptions struct {
	// Prefix allows to filter objects by a key prefix. Should always end with slash.
	Prefix string
	// The first item listed will be cursor or the one after it.
	Cursor string
	// Recursive iterates objects as a single namespace.
	Recursive bool

	// Info includes ObjectInfo in the results.
	Info bool
	// Standard includes StandardMetadata in the results.
	Standard bool
	// Custom include CustomMetadata in the results.
	Custom bool
}

// ListObjects returns an iterator over the objects.
func (project *Project) ListObjects(ctx context.Context, bucket string, options *ObjectIteratorOptions) *ObjectIterator {
	b := storj.Bucket{Name: bucket, PathCipher: storj.EncAESGCM}
	opts := storj.ListOptions{
		Direction: storj.After,
	}

	if options != nil {
		opts.Prefix = options.Prefix
		opts.Cursor = options.Cursor
		opts.Recursive = options.Recursive
	}

	objects := ObjectIterator{
		ctx:     ctx,
		project: project,
		bucket:  b,
		options: opts,
	}

	if options != nil {
		objects.objOptions = *options
	}

	return &objects
}

// ObjectIterator is an iterator over a collection of objects or prefixes.
type ObjectIterator struct {
	ctx        context.Context
	project    *Project
	bucket     storj.Bucket
	options    storj.ListOptions
	objOptions ObjectIteratorOptions
	list       *kvmetainfo.ObjectListExtended
	position   int
	completed  bool
	err        error
}

// Next prepares next Object for reading.
// It returns false if the end of the iteration is reached and there are no more objects, or if there is an error.
func (objects *ObjectIterator) Next() bool {
	if objects.err != nil {
		objects.completed = true
		return false
	}

	if objects.list == nil {
		more := objects.loadNext()
		objects.completed = !more
		return more
	}

	if objects.position >= len(objects.list.Items)-1 {
		if !objects.list.More {
			objects.completed = true
			return false
		}
		more := objects.loadNext()
		objects.completed = !more
		return more
	}

	objects.position++

	return true
}

func (objects *ObjectIterator) loadNext() bool {
	list, err := objects.project.db.ListObjectsExtended(objects.ctx, objects.bucket, objects.options)
	if err != nil {
		objects.err = err
		return false
	}
	objects.list = &list
	objects.position = 0
	return len(list.Items) > 0
}

// Err returns error, if one happened during iteration.
func (objects *ObjectIterator) Err() error {
	return Error.Wrap(objects.err)
}

// Item returns the current object in the iterator.
func (objects *ObjectIterator) Item() *Object {
	item := objects.item()
	if item == nil {
		return nil
	}

	key := item.Path
	if len(objects.options.Prefix) > 0 {
		key = objects.options.Prefix + item.Path
	}

	obj := Object{
		Key:      key,
		IsPrefix: item.IsPrefix,
	}

	// TODO: Make this filtering on the satellite
	if objects.objOptions.Info {
		obj.Info = ObjectInfo{
			Created: item.Info.Created,
			Expires: item.Info.Expires,
		}
	}

	// TODO: Make this filtering on the satellite
	if objects.objOptions.Standard {
		obj.Standard = StandardMetadata{
			ContentLength: item.Standard.ContentLength,
			ContentType:   item.Standard.ContentType,

			FileCreated:     item.Standard.FileCreated,
			FileModified:    item.Standard.FileModified,
			FilePermissions: item.Standard.FilePermissions,

			Unknown: item.Standard.Unknown,
		}

		if obj.Standard.ContentLength == 0 && item.Stream.Size != 0 {
			obj.Standard.ContentLength = item.Stream.Size
		}
	}

	// TODO: Make this filtering on the satellite
	if objects.objOptions.Custom {
		obj.Custom = item.Custom
	}

	return &obj
}

func (objects *ObjectIterator) item() *kvmetainfo.ObjectExtended {
	if objects.completed {
		return nil
	}

	if objects.err != nil {
		return nil
	}

	if objects.list == nil {
		return nil
	}

	if len(objects.list.Items) == 0 {
		return nil
	}

	return &objects.list.Items[objects.position]
}
