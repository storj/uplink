// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package bucket

import (
	"context"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/uplink"
	"storj.io/uplink/private/metaclient"
)

var mon = monkit.Package()

// Bucket contains information about the bucket.
type Bucket metaclient.Bucket

// ListBucketsOptions defines bucket listing options.
type ListBucketsOptions struct {
	// Cursor sets the starting position of the iterator. The first item listed will be the one after the cursor.
	Cursor string
}

// ListBucketsWithAttribution returns an iterator over the buckets.
func ListBucketsWithAttribution(ctx context.Context, project *uplink.Project, options *ListBucketsOptions) *Iterator {
	defer mon.Task()(&ctx)(nil)

	if options == nil {
		options = &ListBucketsOptions{}
	}

	buckets := Iterator{
		iterator: metaclient.IterateBuckets(ctx, metaclient.IterateBucketsOptions{
			Cursor: options.Cursor,
			DialClientFunc: func() (*metaclient.Client, error) {
				return dialMetainfoClient(ctx, project)
			},
		}),
	}

	return &buckets
}

// Iterator is an iterator over a collection of buckets.
type Iterator struct {
	iterator *metaclient.BucketIterator
}

// Next prepares next Bucket for reading.
// It returns false if the end of the iteration is reached and there are no more buckets, or if there is an error.
func (buckets *Iterator) Next() bool {
	return buckets.iterator.Next()
}

// Err returns error, if one happened during iteration.
func (buckets *Iterator) Err() error {
	return convertKnownErrors(buckets.iterator.Err(), "", "")
}

// Item returns the current bucket in the iterator.
func (buckets *Iterator) Item() *Bucket {
	item := buckets.iterator.Item()
	if item == nil {
		return nil
	}
	return &Bucket{
		Name:        item.Name,
		Created:     item.Created,
		Attribution: item.Attribution,
	}
}

// GetBucketLocation returns bucket location.
func GetBucketLocation(ctx context.Context, project *uplink.Project, bucketName string) (_ string, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucketName == "" {
		return "", convertKnownErrors(metaclient.ErrNoBucket.New(""), bucketName, "")
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return "", convertKnownErrors(err, bucketName, "")
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	response, err := metainfoClient.GetBucketLocation(ctx, metaclient.GetBucketLocationParams{
		Name: []byte(bucketName),
	})
	return string(response.Location), convertKnownErrors(err, bucketName, "")
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoClient storj.io/uplink.dialMetainfoClient
func dialMetainfoClient(ctx context.Context, project *uplink.Project) (_ *metaclient.Client, err error)
