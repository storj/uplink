// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package multipart

import (
	"context"
	"time"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"

	"storj.io/uplink"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/object"
)

var mon = monkit.Package()

// UploadOptions contains additional options for uploading.
type UploadOptions struct {
	// When Expires is zero, there is no expiration.
	Expires time.Time

	CustomMetadata uplink.CustomMetadata

	Retention metaclient.Retention
	LegalHold bool
}

// BeginUpload begins a new multipart upload to bucket and key.
//
// Use project.UploadPart to upload individual parts.
//
// Use project.CommitUpload to finish the upload.
//
// Use project.AbortUpload to cancel the upload at any time.
//
// UploadObject is a convenient way to upload single part objects.
//
// Deprecated: Use object.BeginUpload instead.
func BeginUpload(ctx context.Context, project *uplink.Project, bucket, key string, options *UploadOptions) (_ uplink.UploadInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	var uploadOpts *object.MultipartUploadOptions
	if options != nil {
		uploadOpts = &object.MultipartUploadOptions{
			Expires: options.Expires,
			UserData: metaclient.ObjectUserData{
				Custom: options.CustomMetadata,
			},
			Retention: options.Retention,
			LegalHold: options.LegalHold,
		}
	}

	info, err := object.BeginUpload(ctx, project, bucket, key, uploadOpts)
	if err != nil {
		return uplink.UploadInfo{}, err
	}

	return uplink.UploadInfo{
		UploadID: info.UploadID,
		Key:      info.Key,
		IsPrefix: info.IsPrefix,
		System:   info.System,
		Custom:   info.UserData.Custom,
	}, nil
}
