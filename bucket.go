// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/errs2"
	"storj.io/common/memory"
	"storj.io/common/rpc/rpcstatus"
	"storj.io/common/storj"
)

// ErrBucketNameInvalid is returned when the bucket name is invalid.
var ErrBucketNameInvalid = errs.Class("bucket name invalid")

// ErrBucketAlreadyExists is returned when the bucket already exists during creation.
var ErrBucketAlreadyExists = errs.Class("bucket already exists")

// ErrBucketNotEmpty is returned when the bucket is not empty during deletion.
var ErrBucketNotEmpty = errs.Class("bucket not empty")

// ErrBucketNotFound is returned when the bucket is not found.
var ErrBucketNotFound = errs.Class("bucket not found")

// Bucket contains information about the bucket.
type Bucket struct {
	Name    string
	Created time.Time
}

// StatBucket returns information about a bucket.
func (project *Project) StatBucket(ctx context.Context, bucket string) (info *Bucket, err error) {
	defer mon.Task()(&ctx)(&err)

	b, err := project.project.GetBucket(ctx, bucket)
	if err != nil {
		if storj.ErrNoBucket.Has(err) {
			return nil, ErrBucketNameInvalid.New("%v", bucket)
		} else if storj.ErrBucketNotFound.Has(err) {
			return nil, ErrBucketNotFound.New("%v", bucket)
		}
		return nil, convertKnownErrors(err)
	}

	return &Bucket{
		Name:    b.Name,
		Created: b.Created,
	}, nil
}

// CreateBucket creates a new bucket.
//
// When bucket already exists it returns a valid Bucket and ErrBucketExists.
func (project *Project) CreateBucket(ctx context.Context, bucket string) (created *Bucket, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO remove bucket configuration when proper fix will be deployed on satellite
	b, err := project.project.CreateBucket(ctx, bucket, &storj.Bucket{
		PathCipher: storj.EncAESGCM,
		DefaultRedundancyScheme: storj.RedundancyScheme{
			Algorithm:      storj.ReedSolomon,
			ShareSize:      256 * memory.B.Int32(),
			RequiredShares: 29,
			RepairShares:   35,
			OptimalShares:  80,
			TotalShares:    110,
		},
	})

	if err != nil {
		if storj.ErrNoBucket.Has(err) {
			return nil, ErrBucketNameInvalid.New("%v", bucket)
		}
		if errs2.IsRPC(err, rpcstatus.AlreadyExists) {
			// TODO: Ideally, the satellite should return the existing bucket when this error occurs.
			existing, err := project.StatBucket(ctx, bucket)
			if err != nil {
				return existing, errs.Combine(ErrBucketAlreadyExists.New("%v", bucket), convertKnownErrors(err))
			}
			return existing, ErrBucketAlreadyExists.New("%v", bucket)
		}
		return nil, convertKnownErrors(err)
	}

	return &Bucket{
		Name:    b.Name,
		Created: b.Created,
	}, nil
}

// EnsureBucket ensures that a bucket exists or creates a new one.
//
// When bucket already exists it returns a valid Bucket and no error.
func (project *Project) EnsureBucket(ctx context.Context, bucket string) (ensured *Bucket, err error) {
	defer mon.Task()(&ctx)(&err)

	ensured, err = project.CreateBucket(ctx, bucket)
	if err != nil && !ErrBucketAlreadyExists.Has(err) {
		return nil, convertKnownErrors(err)
	}

	return ensured, nil
}

// DeleteBucket deletes a bucket.
//
// When bucket is not empty it returns ErrBucketNotEmpty.
func (project *Project) DeleteBucket(ctx context.Context, bucket string) (deleted *Bucket, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO: Ideally, this should be done on the satellite
	existing, err := project.StatBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}

	err = project.project.DeleteBucket(ctx, bucket)
	if err != nil {
		if errs2.IsRPC(err, rpcstatus.FailedPrecondition) {
			return nil, ErrBucketNotEmpty.New("%v", bucket)
		}
		if storj.ErrBucketNotFound.Has(err) {
			return nil, ErrBucketNotFound.New("%v", bucket)
		}
		return nil, convertKnownErrors(err)
	}
	return existing, nil
}
