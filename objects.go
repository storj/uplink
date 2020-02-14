// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"storj.io/common/storj"
)

// ObjectsOptions defines iteration options.
type ObjectsOptions struct {
	// Prefix allows to filter objects by a key prefix.
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
func (project *Project) ListObjects(ctx context.Context, bucket string, options *ObjectsOptions) *Objects {
	b := storj.Bucket{Name: bucket, PathCipher: storj.EncAESGCM}
	opts := storj.ListOptions{
		Direction: storj.After,
	}

	if options != nil {
		opts.Prefix = options.Prefix
		opts.Cursor = options.Cursor
		opts.Recursive = options.Recursive
	}

	objects := Objects{
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

// Objects is an iterator over a collection of objects or prefixes.
type Objects struct {
	ctx        context.Context
	project    *Project
	bucket     storj.Bucket
	options    storj.ListOptions
	objOptions ObjectsOptions
	list       *storj.ObjectList
	position   int
	completed  bool
	err        error
}

// Next prepares next Object for reading.
// It returns false if the end of the iteration is reached and there are no more objects, or if there is an error.
func (objects *Objects) Next() bool {
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

func (objects *Objects) loadNext() bool {
	list, err := objects.project.db.ListObjects(objects.ctx, objects.bucket, objects.options)
	if err != nil {
		objects.err = err
		return false
	}
	objects.list = &list
	objects.position = 0
	return len(list.Items) > 0
}

// Err returns error, if one happened during iteration.
func (objects *Objects) Err() error {
	return Error.Wrap(objects.err)
}

// Key returns the key without the prefix of the current object.
func (objects *Objects) Key() string {
	item := objects.item()
	if item == nil {
		return ""
	}
	return item.Path
}

// Item returns the current object in the iterator.
func (objects *Objects) Item() *ListObject {
	item := objects.item()
	if item == nil {
		return nil
	}

	key := item.Path
	if len(objects.options.Prefix) > 0 {
		key = storj.JoinPaths(objects.options.Prefix, item.Path)
	}

	obj := ListObject{
		Object: Object{
			Key: key,
		},
		IsPrefix: item.IsPrefix,
	}

	// TODO: Make this filtering on the satellite
	if objects.objOptions.Info {
		obj.Info = ObjectInfo{
			Created: item.Created,
			Expires: item.Expires,
		}
	}

	// TODO: Make this filtering on the satellite
	if objects.objOptions.Standard {
		obj.Standard = StandardMetadata{
			ContentLength: item.Size,
			ContentType:   item.ContentType,
			ETag:          string(item.Checksum),
			// FileCreated:     // TODO
			// FileModified:    // TODO
			// FilePermissions: // TODO
		}
	}

	// TODO: Make this filtering on the satellite
	if objects.objOptions.Custom {
		obj.Custom = item.Metadata
	}

	return &obj
}

func (objects *Objects) item() *storj.Object {
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

// ListObject reprsents an object from objects iterator.
type ListObject struct {
	Object
	// IsPrefix indicates whether the Key is a prefix for other objects.
	IsPrefix bool
}
