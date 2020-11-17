// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"crypto/rand"
	"errors"

	"github.com/btcsuite/btcutil/base58"

	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metainfo"
)

// MultipartInfo contains information about multipart upload.
type MultipartInfo struct {
	// StreamID multipart upload identifier encoded with base58.
	StreamID string
}

// MultipartUploadOptions contains additional options for multipart upload.
type MultipartUploadOptions UploadOptions

// MultipartObjectOptions options for committing object.
type MultipartObjectOptions struct {
	CustomMetadata CustomMetadata
}

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

// CompleteMultipartUpload commits object after uploading all parts.
// TODO should we accept parameter with info uploaded parts.
func (project *Project) CompleteMultipartUpload(ctx context.Context, bucket, key, streamID string, opts *MultipartObjectOptions) (obj *Object, err error) {
	defer mon.Func().RestartTrace(&ctx)(&err)

	if bucket == "" {
		return nil, errwrapf("%w (%q)", ErrBucketNameInvalid, bucket)
	}
	if key == "" {
		return nil, errwrapf("%w (%q)", ErrObjectKeyInvalid, key)
	}

	if streamID == "" {
		return nil, packageError.New("streamID is missing")
	}

	decodedStreamID, version, err := base58.CheckDecode(streamID)
	if err != nil || version != 1 {
		return nil, errors.New("invalid streamID format")
	}

	if opts == nil {
		opts = &MultipartObjectOptions{}
	}

	id, err := storj.StreamIDFromBytes(decodedStreamID)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	metadataBytes, err := pb.Marshal(&pb.SerializableMeta{
		UserDefined: opts.CustomMetadata.Clone(),
	})
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	streamInfo, err := pb.Marshal(&pb.StreamInfo{
		Metadata: metadataBytes,
	})
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	encStore := project.access.encAccess.Store
	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(key), encStore)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	var metadataKey storj.Key
	// generate random key for encrypting the segment's content
	_, err = rand.Read(metadataKey[:])
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	var encryptedKeyNonce storj.Nonce
	// generate random nonce for encrypting the content key
	_, err = rand.Read(encryptedKeyNonce[:])
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	encryptedKey, err := encryption.EncryptKey(&metadataKey, project.encryption.CipherSuite, derivedKey, &encryptedKeyNonce)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	// encrypt metadata with the content encryption key and zero nonce.
	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, project.encryption.CipherSuite, &metadataKey, &storj.Nonce{})
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	// TODO should we commit StreamMeta or commit only encrypted StreamInfo
	streamMetaBytes, err := pb.Marshal(&pb.StreamMeta{
		EncryptedStreamInfo: encryptedStreamInfo,
	})
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	err = project.metainfo.CommitObject(ctx, metainfo.CommitObjectParams{
		StreamID:                      id,
		EncryptedMetadata:             streamMetaBytes,
		EncryptedMetadataEncryptedKey: encryptedKey,
		EncryptedMetadataNonce:        encryptedKeyNonce,
	})
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}

	// TODO return object after committing
	return &Object{}, nil
}
