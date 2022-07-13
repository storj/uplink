// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package bucket

import (
	"context"
	"time"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/uplink"
	"storj.io/uplink/private/metaclient"
)

// TODO this code should be unified with code for ListBuckets.

var mon = monkit.Package()

// Bucket contains information about the bucket.
type Bucket struct {
	Name        string
	Created     time.Time
	Attribution string
}

// ListBucketsOptions defines bucket listing options.
type ListBucketsOptions struct {
	// Cursor sets the starting position of the iterator. The first item listed will be the one after the cursor.
	Cursor string
}

// ListBucketsWithAttribution returns an iterator over the buckets.
func ListBucketsWithAttribution(ctx context.Context, project *uplink.Project, options *ListBucketsOptions) *Iterator {
	defer mon.Task()(&ctx)(nil)

	opts := metaclient.BucketListOptions{
		Direction: metaclient.After,
	}

	if options != nil {
		opts.Cursor = options.Cursor
	}

	buckets := Iterator{
		ctx:     ctx,
		project: project,
		options: opts,
	}

	return &buckets
}

// Iterator is an iterator over a collection of buckets.
type Iterator struct {
	ctx       context.Context
	project   *uplink.Project
	options   metaclient.BucketListOptions
	list      *metaclient.BucketList
	position  int
	completed bool
	err       error
}

// Next prepares next Bucket for reading.
// It returns false if the end of the iteration is reached and there are no more buckets, or if there is an error.
func (buckets *Iterator) Next() bool {
	if buckets.err != nil {
		buckets.completed = true
		return false
	}

	if buckets.list == nil {
		more := buckets.loadNext()
		buckets.completed = !more
		return more
	}

	if buckets.position >= len(buckets.list.Items)-1 {
		if !buckets.list.More {
			buckets.completed = true
			return false
		}
		more := buckets.loadNext()
		buckets.completed = !more
		return more
	}

	buckets.position++

	return true
}

func (buckets *Iterator) loadNext() bool {
	ok, err := buckets.tryLoadNext()
	if err != nil {
		buckets.err = convertKnownErrors(err, "", "")
		return false
	}
	return ok
}

func (buckets *Iterator) tryLoadNext() (ok bool, err error) {
	db, err := dialMetainfoDB(buckets.ctx, buckets.project)
	if err != nil {
		return false, err
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	list, err := db.ListBuckets(buckets.ctx, buckets.options)
	if err != nil {
		return false, err
	}
	buckets.list = &list
	if list.More {
		buckets.options = buckets.options.NextPage(list)
	}
	buckets.position = 0
	return len(list.Items) > 0, nil
}

// Err returns error, if one happened during iteration.
func (buckets *Iterator) Err() error {
	return convertKnownErrors(buckets.err, "", "")
}

// Item returns the current bucket in the iterator.
func (buckets *Iterator) Item() *Bucket {
	item := buckets.item()
	if item == nil {
		return nil
	}
	return &Bucket{
		Name:        item.Name,
		Created:     item.Created,
		Attribution: item.Attribution,
	}
}

func (buckets *Iterator) item() *metaclient.Bucket {
	if buckets.completed {
		return nil
	}

	if buckets.err != nil {
		return nil
	}

	if buckets.list == nil {
		return nil
	}

	if len(buckets.list.Items) == 0 {
		return nil
	}

	return &buckets.list.Items[buckets.position]
}

//go:linkname dialMetainfoDB storj.io/uplink.dialMetainfoDB
func dialMetainfoDB(ctx context.Context, project *uplink.Project) (_ *metaclient.DB, err error)

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error
