// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package object

import (
	"context"
	_ "unsafe" // for go:linkname

	"storj.io/common/pb"
	"storj.io/uplink"
	"storj.io/uplink/internal/privateprops"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/testuplink"
)

// ListUploadsOptions options for listing uncommitted uploads.
type ListUploadsOptions struct {
	// Prefix allows to filter uncommitted uploads by a key prefix. If not empty, it must end with slash.
	Prefix string
	// Cursor sets the starting position of the iterator.
	// The first item listed will be the one after the cursor.
	// Cursor is relative to Prefix.
	Cursor string
	// Recursive iterates the objects without collapsing prefixes.
	Recursive bool

	// System includes SystemMetadata in the results.
	System bool
	// Custom includes CustomMetadata in the results.
	Custom bool
	// ETag includes object ETags in the results.
	ETag bool
	// Checksum includes object checksums in the results.
	Checksum bool
}

// ListUploads returns an iterator over the uncommitted uploads in bucket.
// Both multipart and regular uploads are returned. An object may not be
// visible through ListUploads until it has a committed part.
func ListUploads(ctx context.Context, project *uplink.Project, bucket string, options *ListUploadsOptions) *UploadIterator {
	defer mon.Task()(&ctx)(nil)

	opts := metaclient.ListOptions{
		Direction: metaclient.After,
		Status:    int32(pb.Object_UPLOADING), // TODO: define object status constants in storj package?
		Delimiter: "/",
		Limit:     testuplink.GetListLimit(ctx),
	}

	if options != nil {
		opts.Prefix = options.Prefix
		opts.Cursor = options.Cursor
		opts.Recursive = options.Recursive
		opts.IncludeSystemMetadata = options.System
		opts.IncludeCustomMetadata = options.Custom
		opts.IncludeETag = options.ETag
		opts.IncludeChecksum = options.Checksum
	}

	return &UploadIterator{
		iter: listUploads(ctx, project, bucket, opts),
	}
}

// UploadIterator is an iterator over a collection of uncommitted uploads.
type UploadIterator struct {
	iter *uplink.UploadIterator
}

// Next prepares next entry for reading.
// It returns false if the end of the iteration is reached and there are no more uploads, or if there is an error.
func (uploads *UploadIterator) Next() bool {
	return uploads.iter.Next()
}

// Err returns error, if one happened during iteration.
func (uploads *UploadIterator) Err() error {
	return packageError.Wrap(uploads.iter.Err())
}

// Item returns the current entry in the iterator.
func (uploads *UploadIterator) Item() *UploadInfo {
	item := uploads.iter.Item()
	if item == nil {
		return nil
	}

	privateItem := uploadInfo_getPrivate(item)

	return &UploadInfo{
		UploadInfo: *item,
		ETag:       privateItem.ETag,
		Checksum:   privateItem.Checksum,
	}
}

// ListUploadParts returns an iterator over the parts of a multipart upload started with BeginUpload.
func ListUploadParts(ctx context.Context, project *uplink.Project, bucket, key, uploadID string, options *uplink.ListUploadPartsOptions) *PartIterator {
	defer mon.Task()(&ctx)(nil)

	return &PartIterator{
		iter: project.ListUploadParts(ctx, bucket, key, uploadID, options),
	}
}

// PartIterator is an iterator over a collection of parts of an upload.
type PartIterator struct {
	iter *uplink.PartIterator
}

// Next prepares next entry for reading.
func (parts *PartIterator) Next() bool {
	return parts.iter.Next()
}

// Item returns the current entry in the iterator.
func (parts *PartIterator) Item() *Part {
	item := parts.iter.Item()
	if item == nil {
		return nil
	}

	privateItem := part_getPrivate(item)

	return &Part{
		Part:     *item,
		Checksum: privateItem.Checksum,
	}
}

// Err returns error, if one happened during iteration.
func (parts *PartIterator) Err() error {
	return packageError.Wrap(parts.iter.Err())
}

//go:linkname listUploads storj.io/uplink.listUploads
func listUploads(ctx context.Context, project *uplink.Project, bucket string, opts metaclient.ListOptions) *uplink.UploadIterator

//go:linkname uploadInfo_getPrivate storj.io/uplink.uploadInfo_getPrivate
func uploadInfo_getPrivate(uploadInfo *uplink.UploadInfo) privateprops.UploadInfo
