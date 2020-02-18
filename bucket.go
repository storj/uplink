// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/errs2"
	"storj.io/common/rpc/rpcstatus"
	"storj.io/common/storj"
)

var (
	// ErrBucketExists is returned when the bucket already exists during creation.
	ErrBucketExists = errs.Class("bucket already exists")

	// ErrBucketNotEmpty is returned when the bucket is not empty during deletion.
	ErrBucketNotEmpty = errs.Class("bucket not empty")

	// ErrBucketNotFound is returned when the bucket is not found.
	ErrBucketNotFound = errs.Class("bucket not found")
)

// Bucket contains information about the bucket.
type Bucket struct {
	Name    string
	Created time.Time
}

// StatBucket returns information about a bucket.
func (project *Project) StatBucket(ctx context.Context, name string) (_ *Bucket, err error) {
	defer mon.Task()(&ctx)(&err)

	b, err := project.project.GetBucket(ctx, name)

	bucket := &Bucket{
		Name:    b.Name,
		Created: b.Created,
	}

	return bucket, Error.Wrap(err)
}

// CreateBucket creates a new bucket.
//
// When bucket already exists it returns a valid *Bucket and ErrBucketExists.
func (project *Project) CreateBucket(ctx context.Context, name string) (_ *Bucket, err error) {
	defer mon.Task()(&ctx)(&err)

	b, err := project.project.CreateBucket(ctx, name, nil)

	bucket := &Bucket{
		Name:    b.Name,
		Created: b.Created,
	}
	if errs2.IsRPC(err, rpcstatus.AlreadyExists) {
		// TODO: Ideally, the satellite should return the existing bucket when this error occurs.
		bucket, err = project.StatBucket(ctx, name)
		if err != nil {
			return bucket, errs.Combine(ErrBucketExists.New(name), err)
		}
		return bucket, ErrBucketExists.New(name)
	}

	return bucket, Error.Wrap(err)
}

// EnsureBucket opens or creates a new bucket.
//
// When bucket already exists it returns a valid *Bucket and nil.
func (project *Project) EnsureBucket(ctx context.Context, name string) (_ *Bucket, err error) {
	defer mon.Task()(&ctx)(&err)

	bucket, err := project.CreateBucket(ctx, name)
	if ErrBucketExists.Has(err) {
		err = nil
	}

	return bucket, Error.Wrap(err)
}

// DeleteBucket deletes a bucket.
//
// When bucket is not empty it returns ErrBucketNotEmpty.
func (project *Project) DeleteBucket(ctx context.Context, name string) (err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO: Most probably this will delete any objects in the bucket.
	// We may need to add a check that the bucket is empty.
	err = project.project.DeleteBucket(ctx, name)
	if errs2.IsRPC(err, rpcstatus.FailedPrecondition) {
		return ErrBucketNotEmpty.New(name)
	} else if storj.ErrBucketNotFound.Has(err) {
		return ErrBucketNotFound.New(name)
	}
	return Error.Wrap(err)
}
