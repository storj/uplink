// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"github.com/btcsuite/btcutil/base58"

	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/uplink/private/metainfo"
)

// MultipartInfo contains information about multipart upload.
type MultipartInfo struct {
	// StreamID multipart upload identifier encoded with base58.
	StreamID string
}

// MultipartUploadOptions contains additional options for multipart upload.
type MultipartUploadOptions UploadOptions

// NewMultipartUpload begins new multipart upload.
// Potential name: BeginObject.
func (project *Project) NewMultipartUpload(ctx context.Context, bucket, key string, options *MultipartUploadOptions) (info MultipartInfo, err error) {
	defer mon.Func().RestartTrace(&ctx)(&err)

	if bucket == "" {
		return MultipartInfo{}, errwrapf("%w (%q)", ErrBucketNameInvalid, bucket)
	}
	if key == "" {
		return MultipartInfo{}, errwrapf("%w (%q)", ErrObjectKeyInvalid, key)
	}

	if options == nil {
		options = &MultipartUploadOptions{}
	}

	encStore := project.access.encAccess.Store
	encPath, err := encryption.EncryptPathWithStoreCipher(bucket, paths.NewUnencrypted(key), encStore)
	if err != nil {
		return MultipartInfo{}, err
	}

	response, err := project.metainfo.BeginObject(ctx, metainfo.BeginObjectParams{
		Bucket:               []byte(bucket),
		EncryptedPath:        []byte(encPath.Raw()),
		ExpiresAt:            options.Expires,
		EncryptionParameters: project.encryption,
	})
	if err != nil {
		return MultipartInfo{}, convertKnownErrors(err, bucket, key)
	}

	encodedStreamID := base58.CheckEncode(response.StreamID[:], 1)
	return MultipartInfo{
		StreamID: encodedStreamID,
	}, nil
}
