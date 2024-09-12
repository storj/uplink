// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package multipart

import (
	"context"
	"crypto/rand"
	"time"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/base58"
	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink"
	"storj.io/uplink/private/metaclient"
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
func BeginUpload(ctx context.Context, project *uplink.Project, bucket, key string, options *UploadOptions) (info uplink.UploadInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	switch {
	case bucket == "":
		return uplink.UploadInfo{}, convertKnownErrors(metaclient.ErrNoBucket.New(""), bucket, key)
	case key == "":
		return uplink.UploadInfo{}, convertKnownErrors(metaclient.ErrNoPath.New(""), bucket, key)
	}

	if options == nil {
		options = &UploadOptions{}
	}

	encPath, err := encryptPath(project, bucket, key)
	if err != nil {
		return uplink.UploadInfo{}, convertKnownErrors(err, bucket, key)
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return uplink.UploadInfo{}, convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	metadata, err := encryptMetadata(project, bucket, key, options.CustomMetadata)
	if err != nil {
		return uplink.UploadInfo{}, convertKnownErrors(err, bucket, key)
	}

	response, err := metainfoClient.BeginObject(ctx, metaclient.BeginObjectParams{
		Bucket:               []byte(bucket),
		EncryptedObjectKey:   []byte(encPath.Raw()),
		ExpiresAt:            options.Expires,
		EncryptionParameters: encryptionParameters(project),

		EncryptedMetadata:             metadata.EncryptedContent,
		EncryptedMetadataEncryptedKey: metadata.EncryptedKey,
		EncryptedMetadataNonce:        metadata.EncryptedKeyNonce,

		Retention: options.Retention,
		LegalHold: options.LegalHold,
	})
	if err != nil {
		return uplink.UploadInfo{}, convertKnownErrors(err, bucket, key)
	}

	encodedStreamID := base58.CheckEncode(response.StreamID[:], 1)
	return uplink.UploadInfo{
		Key:      key,
		UploadID: encodedStreamID,
		System: uplink.SystemMetadata{
			Expires: options.Expires,
		},
		Custom: options.CustomMetadata,
	}, nil
}

type encryptedMetadata struct {
	EncryptedContent  []byte
	EncryptedKey      []byte
	EncryptedKeyNonce storj.Nonce
}

func encryptMetadata(project *uplink.Project, bucket, key string, metadata uplink.CustomMetadata) (encryptedMetadata, error) {
	if len(metadata) == 0 {
		return encryptedMetadata{}, nil
	}

	metadataBytes, err := pb.Marshal(&pb.SerializableMeta{
		UserDefined: metadata.Clone(),
	})
	if err != nil {
		return encryptedMetadata{}, errs.Wrap(err)
	}

	streamInfo, err := pb.Marshal(&pb.StreamInfo{
		Metadata: metadataBytes,
	})
	if err != nil {
		return encryptedMetadata{}, errs.Wrap(err)
	}

	derivedKey, err := deriveContentKey(project, bucket, key)
	if err != nil {
		return encryptedMetadata{}, errs.Wrap(err)
	}

	var metadataKey storj.Key
	// generate random key for encrypting the segment's content
	_, err = rand.Read(metadataKey[:])
	if err != nil {
		return encryptedMetadata{}, errs.Wrap(err)
	}

	var encryptedKeyNonce storj.Nonce
	// generate random nonce for encrypting the metadata key
	_, err = rand.Read(encryptedKeyNonce[:])
	if err != nil {
		return encryptedMetadata{}, errs.Wrap(err)
	}

	encryptionParameters := encryptionParameters(project)
	encryptedKey, err := encryption.EncryptKey(&metadataKey, encryptionParameters.CipherSuite, derivedKey, &encryptedKeyNonce)
	if err != nil {
		return encryptedMetadata{}, errs.Wrap(err)
	}

	// encrypt metadata with the content encryption key and zero nonce.
	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, encryptionParameters.CipherSuite, &metadataKey, &storj.Nonce{})
	if err != nil {
		return encryptedMetadata{}, errs.Wrap(err)
	}

	// TODO should we commit StreamMeta or commit only encrypted StreamInfo
	streamMetaBytes, err := pb.Marshal(&pb.StreamMeta{
		EncryptedStreamInfo: encryptedStreamInfo,
	})
	if err != nil {
		return encryptedMetadata{}, errs.Wrap(err)
	}

	return encryptedMetadata{
		EncryptedContent:  streamMetaBytes,
		EncryptedKey:      encryptedKey,
		EncryptedKeyNonce: encryptedKeyNonce,
	}, nil
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoClient storj.io/uplink.dialMetainfoClient
func dialMetainfoClient(ctx context.Context, project *uplink.Project) (_ *metaclient.Client, err error)

//go:linkname encryptionParameters storj.io/uplink.encryptionParameters
func encryptionParameters(project *uplink.Project) storj.EncryptionParameters

//go:linkname encryptPath storj.io/uplink.encryptPath
func encryptPath(project *uplink.Project, bucket, key string) (paths.Encrypted, error)

//go:linkname deriveContentKey storj.io/uplink.deriveContentKey
func deriveContentKey(project *uplink.Project, bucket, key string) (*storj.Key, error)
