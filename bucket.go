// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"errors"
	"time"

	"storj.io/common/storj"
)

var (
	// ErrBucketExists is returned when the bucket already exists during creation.
	ErrBucketExists = errors.New("bucket already exists")

	// ErrBucketNotEmpty is returned when the bucket is not empty during deletion.
	ErrBucketNotEmpty = errors.New("bucket not empty")
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

	info := storj.Bucket{
		// TODO: This is required for creating bucket at the moment.
		// PathCipher should be part of the EncryptionAccess and used
		// with object operations instead of creating a bucket.
		PathCipher: storj.EncAESGCM,
	}

	b, err := project.project.CreateBucket(ctx, name, &info)

	bucket := &Bucket{
		Name:    b.Name,
		Created: b.Created,
	}

	return bucket, Error.Wrap(err)
}

// EnsureBucket opens or creates a new bucket.
//
// When bucket already exists it returns a valid *Bucket and nil.
func (project *Project) EnsureBucket(ctx context.Context, name string) (_ *Bucket, err error) {
	defer mon.Task()(&ctx)(&err)

	bucket, err := project.CreateBucket(ctx, name)
	if errors.Is(err, ErrBucketExists) {
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

	return Error.Wrap(err)
}
