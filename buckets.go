// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"storj.io/common/storj"
)

// BucketsOptions defines iteration options.
type BucketsOptions struct {
	// The first item listed will be cursor or the one after it.
	Cursor string
}

// ListBuckets returns an iterator over the buckets.
func (project *Project) ListBuckets(ctx context.Context, options *BucketsOptions) *Buckets {
	opts := storj.BucketListOptions{
		Direction: storj.After,
	}

	if options != nil {
		opts.Cursor = options.Cursor
	}

	buckets := Buckets{
		ctx:     ctx,
		project: project,
		options: opts,
	}

	return &buckets
}

// Buckets is an iterator over a collection of buckets.
type Buckets struct {
	ctx       context.Context
	project   *Project
	options   storj.BucketListOptions
	list      *storj.BucketList
	position  int
	completed bool
	err       error
}

// Next prepares next Bucket for reading.
// It returns false if the end of the iteration is reached and there are no more buckets, or if there is an error.
func (buckets *Buckets) Next() bool {
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

func (buckets *Buckets) loadNext() bool {
	list, err := buckets.project.db.ListBuckets(buckets.ctx, buckets.options)
	if err != nil {
		buckets.err = err
		return false
	}
	buckets.list = &list
	buckets.position = 0
	return len(list.Items) > 0
}

// Err returns error, if one happened during iteration.
func (buckets *Buckets) Err() error {
	return Error.Wrap(buckets.err)
}

// Name returns the bucket name.
func (buckets *Buckets) Name() string {
	item := buckets.item()
	if item == nil {
		return ""
	}
	return item.Name
}

// Item returns the current bucket in the iterator.
func (buckets *Buckets) Item() *Bucket {
	item := buckets.item()
	if item == nil {
		return nil
	}
	return &Bucket{
		Name:    item.Name,
		Created: item.Created,
	}
}

func (buckets *Buckets) item() *storj.Bucket {
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
