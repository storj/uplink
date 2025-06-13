// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package bucket

import (
	"context"
	"errors"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/errs2"
	"storj.io/common/rpc/rpcstatus"
	"storj.io/uplink"
	"storj.io/uplink/private/metaclient"
	privateProject "storj.io/uplink/private/project"
)

var mon = monkit.Package()

var (
	// ErrBucketNoLock is returned when a bucket has object lock disabled.
	ErrBucketNoLock = errors.New("object lock is not enabled for this bucket")

	// ErrBucketInvalidObjectLockConfig is returned when bucket object lock config is invalid.
	ErrBucketInvalidObjectLockConfig = errors.New("bucket object lock configuration is invalid")

	// ErrBucketInvalidStateObjectLock is returned when attempting to set object lock without versioning enabled.
	// This error is also returned when attempting to suspend versioning when object lock is enabled on the bucket.
	ErrBucketInvalidStateObjectLock = errors.New("object lock requires bucket versioning to be enabled")
)

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

// GetBucketVersioning returns bucket versioning state.
func GetBucketVersioning(ctx context.Context, project *uplink.Project, bucketName string) (_ int32, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucketName == "" {
		return 0, convertKnownErrors(metaclient.ErrNoBucket.New(""), bucketName, "")
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return 0, convertKnownErrors(err, bucketName, "")
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	response, err := metainfoClient.GetBucketVersioning(ctx, metaclient.GetBucketVersioningParams{
		Name: []byte(bucketName),
	})
	return response.Versioning, convertKnownErrors(err, bucketName, "")
}

// SetBucketVersioning sets the versioning state for a bucket. True will attempt to enable versioning,
// false will attempt to disable it.
func SetBucketVersioning(ctx context.Context, project *uplink.Project, bucketName string, versioning bool) (err error) {
	defer mon.Task()(&ctx)(&err)

	if bucketName == "" {
		return convertKnownErrors(metaclient.ErrNoBucket.New(""), bucketName, "")
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return convertKnownErrors(err, bucketName, "")
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	err = metainfoClient.SetBucketVersioning(ctx, metaclient.SetBucketVersioningParams{
		Name:       []byte(bucketName),
		Versioning: versioning,
	})
	if errs2.IsRPC(err, rpcstatus.ObjectLockInvalidBucketState) {
		err = ErrBucketInvalidStateObjectLock
	}

	return convertKnownErrors(err, bucketName, "")
}

// CreateBucketWithObjectLockParams contains parameters for CreateBucketWithObjectLock method.
type CreateBucketWithObjectLockParams struct {
	Name              string
	ObjectLockEnabled bool
	Placement         string
}

// CreateBucketWithObjectLock creates a new bucket with object lock enabled/disabled.
func CreateBucketWithObjectLock(ctx context.Context, project *uplink.Project, params CreateBucketWithObjectLockParams) (_ *Bucket, err error) {
	defer mon.Task()(&ctx)(&err)

	if params.Name == "" {
		return nil, convertKnownErrors(metaclient.ErrNoBucket.New(""), params.Name, "")
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return nil, convertKnownErrors(err, params.Name, "")
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	response, err := metainfoClient.CreateBucket(ctx, metaclient.CreateBucketParams{
		Name:              []byte(params.Name),
		Placement:         []byte(params.Placement),
		ObjectLockEnabled: params.ObjectLockEnabled,
	})
	if err != nil {
		if errs2.IsRPC(err, rpcstatus.AlreadyExists) {
			return nil, convertKnownErrors(uplink.ErrBucketAlreadyExists, params.Name, "")
		}
		return nil, convertKnownErrors(err, params.Name, "")
	}
	return (*Bucket)(&response), convertKnownErrors(err, params.Name, "")
}

// GetBucketObjectLockConfiguration returns bucket object lock configuration.
func GetBucketObjectLockConfiguration(ctx context.Context, project *uplink.Project, bucketName string) (_ *metaclient.BucketObjectLockConfiguration, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucketName == "" {
		return nil, convertKnownErrors(metaclient.ErrNoBucket.New(""), bucketName, "")
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return nil, convertKnownErrors(err, bucketName, "")
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	response, err := metainfoClient.GetBucketObjectLockConfiguration(ctx, metaclient.GetBucketObjectLockConfigurationParams{
		Name: []byte(bucketName),
	})
	// TODO: remove when we expose those in convertKnownErrors
	switch {
	case metaclient.ErrProjectNoLock.Has(err):
		err = privateProject.ErrProjectNoLock
	case metaclient.ErrBucketNoLock.Has(err):
		err = ErrBucketNoLock
	}

	return &metaclient.BucketObjectLockConfiguration{
		Enabled:          response.Enabled,
		DefaultRetention: response.DefaultRetention,
	}, convertKnownErrors(err, bucketName, "")
}

// SetBucketObjectLockConfiguration updates bucket object lock configuration.
func SetBucketObjectLockConfiguration(ctx context.Context, project *uplink.Project, bucketName string, config *metaclient.BucketObjectLockConfiguration) (err error) {
	defer mon.Task()(&ctx)(&err)

	if bucketName == "" {
		return convertKnownErrors(metaclient.ErrNoBucket.New(""), bucketName, "")
	}

	if config == nil {
		return convertKnownErrors(ErrBucketInvalidObjectLockConfig, bucketName, "")
	}

	if config.DefaultRetention != nil && config.DefaultRetention.Days > 0 && config.DefaultRetention.Years > 0 {
		return convertKnownErrors(ErrBucketInvalidObjectLockConfig, bucketName, "")
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return convertKnownErrors(err, bucketName, "")
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	err = metainfoClient.SetBucketObjectLockConfiguration(ctx, metaclient.SetBucketObjectLockConfigurationParams{
		Name:             []byte(bucketName),
		Enabled:          config.Enabled,
		DefaultRetention: config.DefaultRetention,
	})

	switch {
	case metaclient.ErrBucketInvalidObjectLockConfig.Has(err):
		err = ErrBucketInvalidObjectLockConfig
	case metaclient.ErrBucketInvalidStateObjectLock.Has(err):
		err = ErrBucketInvalidStateObjectLock
	}

	return convertKnownErrors(err, bucketName, "")
}

// DeleteBucketWithObjectsBypassGovernanceRetention deletes all objects in the bucket and bypasses any object lock settings.
func DeleteBucketWithObjectsBypassGovernanceRetention(ctx context.Context, project *uplink.Project, bucketName string) (_ *uplink.Bucket, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucketName == "" {
		return nil, convertKnownErrors(metaclient.ErrNoBucket.New(""), bucketName, "")
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return nil, convertKnownErrors(err, bucketName, "")
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	existing, err := metainfoClient.DeleteBucket(ctx, metaclient.DeleteBucketParams{
		Name: []byte(bucketName),

		DeleteAll:                 true,
		BypassGovernanceRetention: true,
	})
	if err != nil {
		return nil, convertKnownErrors(err, bucketName, "")
	}

	if len(existing.Name) == 0 {
		return &uplink.Bucket{Name: bucketName}, nil
	}

	return &uplink.Bucket{
		Name:    existing.Name,
		Created: existing.Created,
	}, nil
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoClient storj.io/uplink.dialMetainfoClient
func dialMetainfoClient(ctx context.Context, project *uplink.Project) (_ *metaclient.Client, err error)
