// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import "storj.io/uplink/private/metaclient"

// Client creates a grpcClient.
type Client = metaclient.Client

// DialNodeURL dials to metainfo endpoint with the specified api key.
var DialNodeURL = metaclient.DialNodeURL

// ListObjectsParams parameters for ListObjects method.
type ListObjectsParams = metaclient.ListObjectsParams

// BeginObjectParams parmaters for BeginObject method.
type BeginObjectParams = metaclient.BeginObjectParams

// BeginSegmentParams parameters for BeginSegment method.
type BeginSegmentParams = metaclient.BeginSegmentParams

// CommitSegmentParams parameters for CommitSegment method.
type CommitSegmentParams = metaclient.CommitSegmentParams

// ListBucketsParams parameters for ListBuckets method.
type ListBucketsParams = metaclient.ListBucketsParams

// BeginDeleteObjectParams parameters for BeginDeleteObject method.
type BeginDeleteObjectParams = metaclient.BeginDeleteObjectParams

// CreateBucketParams parameters for CreateBucket method.
type CreateBucketParams = metaclient.CreateBucketParams

// DeleteBucketParams parmaters for DeleteBucket method.
type DeleteBucketParams = metaclient.DeleteBucketParams

// GetBucketParams parmaters for GetBucketParams method.
type GetBucketParams = metaclient.GetBucketParams

// GetObjectParams parameters for GetObject method.
type GetObjectParams = metaclient.GetObjectParams

// CommitObjectParams parmaters for CommitObject method.
type CommitObjectParams = metaclient.CommitObjectParams

// MakeInlineSegmentParams parameters for MakeInlineSegment method.
type MakeInlineSegmentParams = metaclient.MakeInlineSegmentParams

// DownloadSegmentParams parameters for DownloadSegment method.
type DownloadSegmentParams = metaclient.DownloadSegmentParams

// ListSegmentsParams parameters for ListSegments method.
type ListSegmentsParams = metaclient.ListSegmentsParams

// BatchItem represents single request in batch.
type BatchItem = metaclient.BatchItem

// ListPendingObjectStreamsParams parameters for ListPendingObjectStreams method.
type ListPendingObjectStreamsParams = metaclient.ListPendingObjectStreamsParams
