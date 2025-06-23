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

	// ErrTagsNotFound is returned when no tags were found on a bucket.
	ErrTagsNotFound = errors.New("tags not found")

	// ErrTooManyTags is returned when a set of tags is too long.
	ErrTooManyTags = errors.New("too many tags")

	// ErrTagKeyInvalid is returned when the key of a tag is invalid.
	ErrTagKeyInvalid = errors.New("tag key invalid")

	// ErrTagKeyDuplicate is returned when a set of tags contains multiple tags with the same key.
	ErrTagKeyDuplicate = errors.New("tag key is duplicated")

	// ErrTagValueInvalid is returned when the value of a tag is invalid.
	ErrTagValueInvalid = errors.New("tag value invalid")
)

// Bucket contains information about the bucket.
type Bucket metaclient.Bucket

// Tag represents a bucket tag.
type Tag = metaclient.BucketTag

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

// GetBucketTagging returns the set of tags placed on a bucket.
func GetBucketTagging(ctx context.Context, project *uplink.Project, bucketName string) (_ []Tag, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucketName == "" {
		return nil, convertKnownErrors(metaclient.ErrNoBucket.New(""), bucketName, "")
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return nil, convertKnownErrors(err, bucketName, "")
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	response, err := metainfoClient.GetBucketTagging(ctx, metaclient.GetBucketTaggingParams{
		Name: []byte(bucketName),
	})
	return response.Tags, packageConvertKnownErrors(err, bucketName, "")
}

// SetBucketTagging places a set of tags on a bucket.
func SetBucketTagging(ctx context.Context, project *uplink.Project, bucketName string, tags []Tag) (err error) {
	defer mon.Task()(&ctx)(&err)

	if bucketName == "" {
		return convertKnownErrors(metaclient.ErrNoBucket.New(""), bucketName, "")
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return convertKnownErrors(err, bucketName, "")
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	err = metainfoClient.SetBucketTagging(ctx, metaclient.SetBucketTaggingParams{
		Name: []byte(bucketName),
		Tags: tags,
	})

	return packageConvertKnownErrors(err, bucketName, "")
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

	return packageConvertKnownErrors(err, bucketName, "")
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

	return &metaclient.BucketObjectLockConfiguration{
		Enabled:          response.Enabled,
		DefaultRetention: response.DefaultRetention,
	}, packageConvertKnownErrors(err, bucketName, "")
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

	return packageConvertKnownErrors(err, bucketName, "")
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

func packageConvertKnownErrors(err error, bucket, key string) error {
	switch {
	case metaclient.ErrProjectNoLock.Has(err):
		return privateProject.ErrProjectNoLock
	case metaclient.ErrBucketNoLock.Has(err):
		return ErrBucketNoLock
	case metaclient.ErrBucketInvalidObjectLockConfig.Has(err):
		return ErrBucketInvalidObjectLockConfig
	case metaclient.ErrBucketInvalidStateObjectLock.Has(err):
		return ErrBucketInvalidStateObjectLock
	case metaclient.ErrBucketTagsNotFound.Has(err):
		return ErrTagsNotFound
	case metaclient.ErrTooManyBucketTags.Has(err):
		return ErrTooManyTags
	case metaclient.ErrBucketTagKeyInvalid.Has(err):
		return ErrTagKeyInvalid
	case metaclient.ErrBucketTagKeyDuplicate.Has(err):
		return ErrTagKeyDuplicate
	case metaclient.ErrBucketTagValueInvalid.Has(err):
		return ErrTagValueInvalid
	}

	return convertKnownErrors(err, bucket, key)
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoClient storj.io/uplink.dialMetainfoClient
func dialMetainfoClient(ctx context.Context, project *uplink.Project) (_ *metaclient.Client, err error)
