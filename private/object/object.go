// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package object

import (
	"context"
	"errors"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/errs2"
	"storj.io/common/rpc/rpcstatus"
	"storj.io/common/storj"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
	"storj.io/uplink/private/metaclient"
)

var mon = monkit.Package()

// Error is default error class for uplink.
var packageError = errs.Class("object")

// ErrMethodNotAllowed is returned when method is not allowed against specified entity (e.g. object).
var ErrMethodNotAllowed = errors.New("method not allowed")

// IPSummary contains information about the object IP-s.
type IPSummary = metaclient.GetObjectIPsResponse

// VersionedObject represents object with version.
// TODO find better place of name for this and related things.
type VersionedObject struct {
	uplink.Object
	Version        []byte
	IsDeleteMarker bool
}

// VersionedUpload represents upload which returnes object version at the end.
type VersionedUpload struct {
	upload *uplink.Upload
}

// ListObjectVersionsOptions defines listing options for versioned objects.
type ListObjectVersionsOptions struct {
	Prefix        string
	Cursor        string
	VersionCursor []byte
	Recursive     bool
	System        bool
	Custom        bool
	Limit         int
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

// VersionedDownload is a download from Storj Network.
type VersionedDownload struct {
	download *uplink.Download
}

// Info returns the last information about the object.
func (download *VersionedDownload) Info() *VersionedObject {
	info := download.download.Info()
	return convertUplinkObject(info)
}

// Read downloads up to len(p) bytes into p from the object's data stream.
// It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
func (download *VersionedDownload) Read(p []byte) (n int, err error) {
	return download.download.Read(p)
}

// Close closes the reader of the download.
func (download *VersionedDownload) Close() error {
	return download.download.Close()
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
	defer mon.Task()(&ctx)(&err)

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

	db := metaclient.New(metainfoClient, storj.EncryptionParameters{}, expose.AccessGetEncAccess(access).Store)

	summary, err := db.GetObjectIPs(ctx, metaclient.Bucket{Name: bucket}, key)
	return summary, packageError.Wrap(err)
}

// StatObject returns information about an object at the specific key and version.
func StatObject(ctx context.Context, project *uplink.Project, bucket, key string, version []byte) (info *VersionedObject, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, packageConvertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	obj, err := db.GetObject(ctx, bucket, key, version)
	if err != nil {
		return nil, packageConvertKnownErrors(err, bucket, key)
	}

	return convertObject(&obj), nil
}

// DeleteObject deletes the object at the specific key.
// Returned deleted is not nil when the access grant has read permissions and
// the object was deleted.
// TODO(ver) currently we are returning object that was returned by satellite
// if its regular object (status != delete marker) it means no delete marker
// was created.
func DeleteObject(ctx context.Context, project *uplink.Project, bucket, key string, version []byte) (info *VersionedObject, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, packageConvertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	obj, err := db.DeleteObject(ctx, bucket, key, version)
	if err != nil {
		return nil, packageConvertKnownErrors(err, bucket, key)
	}

	return convertObject(&obj), nil
}

// ListObjectVersions returns a list of objects and their versions.
func ListObjectVersions(ctx context.Context, project *uplink.Project, bucket string, options *ListObjectVersionsOptions) (_ []*VersionedObject, more bool, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, false, convertKnownErrors(err, bucket, "")
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	opts := metaclient.ListOptions{
		Direction:          metaclient.After,
		IncludeAllVersions: true,
	}

	if options != nil {
		opts.Prefix = options.Prefix
		opts.Cursor = options.Cursor
		opts.VersionCursor = options.VersionCursor
		opts.Recursive = options.Recursive
		opts.IncludeCustomMetadata = options.Custom
		opts.IncludeSystemMetadata = options.System
		opts.Limit = options.Limit
	}

	obj, err := db.ListObjects(ctx, bucket, opts)
	if err != nil {
		return nil, false, convertKnownErrors(err, bucket, "")
	}

	var versions []*VersionedObject
	for _, o := range obj.Items {
		versions = append(versions, convertObject(&o))
	}

	return versions, obj.More, nil
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

// DownloadObject starts a download from the specific key and version. If version is empty latest object will be downloaded.
func DownloadObject(ctx context.Context, project *uplink.Project, bucket, key string, version []byte, options *uplink.DownloadOptions) (_ *VersionedDownload, err error) {
	defer mon.Task()(&ctx)(&err)

	download, err := downloadObjectWithVersion(ctx, project, bucket, key, version, options)
	if err != nil {
		return nil, packageConvertKnownErrors(err, bucket, key)
	}
	return &VersionedDownload{
		download: download,
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

// CopyObject atomically copies object to a different bucket or/and key.
func CopyObject(ctx context.Context, project *uplink.Project, sourceBucket, sourceKey string, sourceVersion []byte, targetBucket, targetKey string, options *uplink.CopyObjectOptions) (_ *VersionedObject, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, packageConvertKnownErrors(err, sourceBucket, sourceKey)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	obj, err := db.CopyObject(ctx, sourceBucket, sourceKey, sourceVersion, targetBucket, targetKey)
	if err != nil {
		return nil, packageConvertKnownErrors(err, sourceBucket, sourceKey)
	}

	return convertObject(obj), nil
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
		Version:        obj.Version,
		IsDeleteMarker: obj.IsDeleteMarker,
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

func packageConvertKnownErrors(err error, bucket, key string) error {
	if errs2.IsRPC(err, rpcstatus.MethodNotAllowed) {
		return ErrMethodNotAllowed
	}
	return convertKnownErrors(err, bucket, key)
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoDB storj.io/uplink.dialMetainfoDB
func dialMetainfoDB(ctx context.Context, project *uplink.Project) (_ *metaclient.DB, err error)

//go:linkname encryptionParameters storj.io/uplink.encryptionParameters
func encryptionParameters(project *uplink.Project) storj.EncryptionParameters

//go:linkname objectVersion storj.io/uplink.objectVersion
func objectVersion(object *uplink.Object) []byte

//go:linkname downloadObjectWithVersion storj.io/uplink.downloadObjectWithVersion
func downloadObjectWithVersion(ctx context.Context, project *uplink.Project, bucket, key string, version []byte, options *uplink.DownloadOptions) (_ *uplink.Download, err error)
