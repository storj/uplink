// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package object

import (
	"context"
	"crypto/rand"
	"time"
	_ "unsafe" // for go:linkname

	"github.com/zeebo/errs"

	"storj.io/common/base58"
	"storj.io/common/encryption"
	"storj.io/common/errs2"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/rpc/rpcstatus"
	"storj.io/common/storj"
	"storj.io/uplink"
	"storj.io/uplink/internal/privateprops"
	"storj.io/uplink/private/metaclient"
)

// Part contains the metadata of an object part.
type Part struct {
	uplink.Part
	Checksum []byte
}

// UploadInfo contains information about an upload.
type UploadInfo struct {
	UploadID string
	Key      string

	IsPrefix bool

	System   uplink.SystemMetadata
	UserData metaclient.ObjectUserData
}

// MultipartUploadOptions contains additional options for uploading multipart objects.
type MultipartUploadOptions struct {
	// When Expires is zero, there is no expiration.
	Expires time.Time

	UserData metaclient.ObjectUserData

	Retention metaclient.Retention
	LegalHold bool
}

// BeginUpload begins a new multipart upload to bucket and key.
//
// Use project.UploadPart to upload individual parts.
//
// Use CommitUpload to finish the upload.
//
// Use project.AbortUpload to cancel the upload at any time.
//
// UploadObject is a convenient way to upload single part objects.
func BeginUpload(ctx context.Context, project *uplink.Project, bucket, key string, options *MultipartUploadOptions) (info UploadInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	switch {
	case bucket == "":
		return UploadInfo{}, convertKnownErrors(metaclient.ErrNoBucket.New(""), bucket, key)
	case key == "":
		return UploadInfo{}, convertKnownErrors(metaclient.ErrNoPath.New(""), bucket, key)
	}

	if options == nil {
		options = &MultipartUploadOptions{}
	} else if err := options.UserData.Checksum.ValidateIncomplete(); err != nil {
		return UploadInfo{}, convertKnownErrors(metaclient.ErrObjectMetadata.Wrap(err), bucket, key)
	}

	encPath, err := encryptPath(project, bucket, key)
	if err != nil {
		return UploadInfo{}, convertKnownErrors(err, bucket, key)
	}

	metainfoClient, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return UploadInfo{}, convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	encryptedUserData, err := encryptUserData(project, bucket, key, options.UserData)
	if err != nil {
		return UploadInfo{}, convertKnownErrors(err, bucket, key)
	}

	response, err := metainfoClient.BeginObject(ctx, metaclient.BeginObjectParams{
		Bucket:               []byte(bucket),
		EncryptedObjectKey:   []byte(encPath.Raw()),
		ExpiresAt:            options.Expires,
		EncryptionParameters: encryptionParameters(project),

		EncryptedUserData: encryptedUserData,

		Retention: options.Retention,
		LegalHold: options.LegalHold,
	})
	if err != nil {
		return UploadInfo{}, packageConvertKnownErrors(err, bucket, key)
	}

	encodedStreamID := base58.CheckEncode(response.StreamID[:], 1)
	return UploadInfo{
		Key:      key,
		UploadID: encodedStreamID,
		System: uplink.SystemMetadata{
			Expires: options.Expires,
		},
		UserData: options.UserData,
	}, nil
}

// PartUpload is a part upload to started multipart upload.
type PartUpload struct {
	upload *uplink.PartUpload
}

// UploadPart uploads a part with partNumber to a multipart upload started with BeginUpload.
//
// uploadID is an upload identifier returned by BeginUpload.
func UploadPart(ctx context.Context, project *uplink.Project, bucket, key, uploadID string, partNumber uint32) (_ *PartUpload, err error) {
	defer mon.Task()(&ctx)(&err)

	upload, err := project.UploadPart(ctx, bucket, key, uploadID, partNumber)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	return &PartUpload{
		upload: upload,
	}, nil
}

// Write uploads len(p) bytes from p to the object's data stream.
// It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
func (upload *PartUpload) Write(p []byte) (int, error) {
	n, err := upload.upload.Write(p)
	return n, packageError.Wrap(err)
}

// SetETag sets the ETag for a part.
func (upload *PartUpload) SetETag(eTag []byte) error {
	return packageError.Wrap(upload.upload.SetETag(eTag))
}

// SetChecksum sets the checksum value for a part.
func (upload *PartUpload) SetChecksum(checksum []byte) error {
	return packageError.Wrap(partUpload_setChecksum(upload.upload, checksum))
}

// Commit commits a part.
//
// Returns ErrUploadDone when either Abort or Commit has already been called.
func (upload *PartUpload) Commit() error {
	err := upload.upload.Commit()
	if errs2.IsRPC(err, rpcstatus.ChecksumsUnsupported) {
		err = ErrChecksumsUnsupported
	}
	return packageError.Wrap(err)
}

// Abort aborts the part upload.
//
// Returns ErrUploadDone when either Abort or Commit has already been called.
func (upload *PartUpload) Abort() error {
	return packageError.Wrap(upload.upload.Abort())
}

// Info returns the last information about the uploaded part.
func (upload *PartUpload) Info() *Part {
	info := upload.upload.Info()
	if info == nil {
		return nil
	}

	privateInfo := part_getPrivate(info)

	return &Part{
		Part:     *info,
		Checksum: privateInfo.Checksum,
	}
}

func encryptUserData(project *uplink.Project, bucket, key string, userData metaclient.ObjectUserData) (metaclient.EncryptedUserData, error) {
	if !userData.RequiresEncryption() {
		return metaclient.EncryptedUserData{
			ChecksumAlgorithm:   userData.Checksum.Algorithm,
			IsChecksumComposite: userData.Checksum.IsComposite,
		}, nil
	}

	metadataBytes, err := pb.Marshal(&pb.SerializableMeta{
		UserDefined: userData.Custom,
	})
	if err != nil {
		return metaclient.EncryptedUserData{}, errs.Wrap(err)
	}

	streamInfo, err := pb.Marshal(&pb.StreamInfo{
		Metadata: metadataBytes,
	})
	if err != nil {
		return metaclient.EncryptedUserData{}, errs.Wrap(err)
	}

	derivedKey, err := deriveContentKey(project, bucket, key)
	if err != nil {
		return metaclient.EncryptedUserData{}, errs.Wrap(err)
	}

	var metadataKey storj.Key
	// generate random key for encrypting the object's content
	_, err = rand.Read(metadataKey[:])
	if err != nil {
		return metaclient.EncryptedUserData{}, errs.Wrap(err)
	}

	var encryptedKeyNonce storj.Nonce
	// generate random nonce for encrypting the metadata key
	_, err = rand.Read(encryptedKeyNonce[:])
	if err != nil {
		return metaclient.EncryptedUserData{}, errs.Wrap(err)
	}

	encryptionParameters := encryptionParameters(project)
	encryptedKey, err := encryption.EncryptKey(&metadataKey, encryptionParameters.CipherSuite, derivedKey, &encryptedKeyNonce)
	if err != nil {
		return metaclient.EncryptedUserData{}, errs.Wrap(err)
	}

	// encrypt metadata with the content encryption key and zero nonce.
	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, encryptionParameters.CipherSuite, &metadataKey, &storj.Nonce{})
	if err != nil {
		return metaclient.EncryptedUserData{}, errs.Wrap(err)
	}

	// TODO should we commit StreamMeta or commit only encrypted StreamInfo
	streamMetaBytes, err := pb.Marshal(&pb.StreamMeta{
		EncryptedStreamInfo: encryptedStreamInfo,
	})
	if err != nil {
		return metaclient.EncryptedUserData{}, errs.Wrap(err)
	}

	encryptedETag, err := encryption.Encrypt(userData.ETag, encryptionParameters.CipherSuite, &metadataKey, &storj.Nonce{1})
	if err != nil {
		return metaclient.EncryptedUserData{}, errs.Wrap(err)
	}

	encryptedChecksum, err := encryption.Encrypt(userData.Checksum.Value, encryptionParameters.CipherSuite, &metadataKey, &storj.Nonce{2})
	if err != nil {
		return metaclient.EncryptedUserData{}, errs.Wrap(err)
	}

	return metaclient.EncryptedUserData{
		EncryptedMetadata:             streamMetaBytes,
		EncryptedMetadataEncryptedKey: encryptedKey,
		EncryptedMetadataNonce:        encryptedKeyNonce,
		EncryptedETag:                 encryptedETag,
		ChecksumAlgorithm:             userData.Checksum.Algorithm,
		IsChecksumComposite:           userData.Checksum.IsComposite,
		EncryptedChecksum:             encryptedChecksum,
	}, nil
}

//go:linkname dialMetainfoClient storj.io/uplink.dialMetainfoClient
func dialMetainfoClient(ctx context.Context, project *uplink.Project) (_ *metaclient.Client, err error)

//go:linkname encryptPath storj.io/uplink.encryptPath
func encryptPath(project *uplink.Project, bucket, key string) (paths.Encrypted, error)

//go:linkname deriveContentKey storj.io/uplink.deriveContentKey
func deriveContentKey(project *uplink.Project, bucket, key string) (*storj.Key, error)

//go:linkname partUpload_setChecksum storj.io/uplink.partUpload_setChecksum
func partUpload_setChecksum(upload *uplink.PartUpload, checksum []byte) error

//go:linkname part_getPrivate storj.io/uplink.part_getPrivate
func part_getPrivate(part *uplink.Part) privateprops.Part
