// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package object

import (
	"context"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/storj"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
	"storj.io/uplink/private/metaclient"
)

var mon = monkit.Package()

// Error is default error class for uplink.
var packageError = errs.Class("object")

// IPSummary contains information about the object IP-s.
type IPSummary = metaclient.GetObjectIPsResponse

// VersionedObject represents object with version.
// TODO find better place of name for this and related things.
type VersionedObject struct {
	uplink.Object
	Version []byte
}

// VersionedUpload represents upload which returnes object version at the end.
type VersionedUpload struct {
	upload *uplink.Upload
}

// Info returns the last information about the uploaded object.
func (upload *VersionedUpload) Info() *VersionedObject {
	info := upload.upload.Info()
	return convertUplinkObject(info)
}

// Write uploads len(p) bytes from p to the object's data stream.
// It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
func (upload *VersionedUpload) Write(p []byte) (n int, err error) {
	return upload.upload.Write(p)
}

// Commit commits data to the store.
//
// Returns ErrUploadDone when either Abort or Commit has already been called.
func (upload *VersionedUpload) Commit() error {
	return upload.upload.Commit()
}

// SetCustomMetadata updates custom metadata to be included with the object.
// If it is nil, it won't be modified.
func (upload *VersionedUpload) SetCustomMetadata(ctx context.Context, custom uplink.CustomMetadata) error {
	return upload.upload.SetCustomMetadata(ctx, custom)
}

// Abort aborts the upload.
//
// Returns ErrUploadDone when either Abort or Commit has already been called.
func (upload *VersionedUpload) Abort() error {
	return upload.upload.Abort()
}

// GetObjectIPs returns the IP-s for a given object.
//
// TODO: delete, once we have stopped using it.
func GetObjectIPs(ctx context.Context, config uplink.Config, access *uplink.Access, bucket, key string) (_ [][]byte, err error) {
	summary, err := GetObjectIPSummary(ctx, config, access, bucket, key)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	return summary.IPPorts, nil
}

// GetObjectIPSummary returns the object IP summary.
func GetObjectIPSummary(ctx context.Context, config uplink.Config, access *uplink.Access, bucket, key string) (_ *IPSummary, err error) {
	dialer, err := expose.ConfigGetDialer(config, ctx)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, dialer.Pool.Close()) }()

	metainfoClient, err := metaclient.DialNodeURL(ctx, dialer, access.SatelliteAddress(), expose.AccessGetAPIKey(access), config.UserAgent)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	db := metaclient.New(metainfoClient, expose.AccessGetEncAccess(access).Store)

	summary, err := db.GetObjectIPs(ctx, metaclient.Bucket{Name: bucket}, key)
	return summary, packageError.Wrap(err)
}

// StatObject returns information about an object at the specific key and version.
func StatObject(ctx context.Context, project *uplink.Project, bucket, key string, version []byte) (info *VersionedObject, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	obj, err := db.GetObject(ctx, bucket, key, version)
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}

	return convertObject(&obj), nil
}

// UploadObject starts an upload to the specific key.
//
// It is not guaranteed that the uncommitted object is visible through ListUploads while uploading.
func UploadObject(ctx context.Context, project *uplink.Project, bucket, key string, options *uplink.UploadOptions) (_ *VersionedUpload, err error) {
	defer mon.Task()(&ctx)(&err)

	upload, err := project.UploadObject(ctx, bucket, key, options)
	if err != nil {
		return
	}
	return &VersionedUpload{
		upload: upload,
	}, nil
}

// CommitUpload commits a multipart upload to bucket and key started with BeginUpload.
//
// uploadID is an upload identifier returned by BeginUpload.
func CommitUpload(ctx context.Context, project *uplink.Project, bucket, key, uploadID string, opts *uplink.CommitUploadOptions) (info *VersionedObject, err error) {
	defer mon.Task()(&ctx)(&err)

	obj, err := project.CommitUpload(ctx, bucket, key, uploadID, opts)
	if err != nil {
		return nil, err
	}

	return convertUplinkObject(obj), nil
}

// convertObject converts metainfo.Object to Version.
func convertObject(obj *metaclient.Object) *VersionedObject {
	if obj.Bucket.Name == "" { // zero object
		return nil
	}

	return &VersionedObject{
		Object: uplink.Object{
			Key: obj.Path,
			System: uplink.SystemMetadata{
				Created:       obj.Created,
				Expires:       obj.Expires,
				ContentLength: obj.Size,
			},
			Custom: obj.Metadata,
		},
		Version: obj.Version,
	}
}

// convertObject converts metainfo.Object to Version.
func convertUplinkObject(obj *uplink.Object) *VersionedObject {
	if obj == nil {
		return nil
	}

	return &VersionedObject{
		Object:  *obj,
		Version: objectVersion(obj),
	}
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoDB storj.io/uplink.dialMetainfoDB
func dialMetainfoDB(ctx context.Context, project *uplink.Project) (_ *metaclient.DB, err error)

//go:linkname encryptionParameters storj.io/uplink.encryptionParameters
func encryptionParameters(project *uplink.Project) storj.EncryptionParameters

//go:linkname objectVersion storj.io/uplink.objectVersion
func objectVersion(object *uplink.Object) []byte
