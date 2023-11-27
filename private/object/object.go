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

// ErrMethodNotAllowed TBD
var ErrMethodNotAllowed = errors.New("method not allowed")

// IPSummary contains information about the object IP-s.
type IPSummary = metaclient.GetObjectIPsResponse

// VersionedObject represents object with version.
// TODO find better place of name for this and related things.
type VersionedObject struct {
	uplink.Object
	Version      []byte
	DeleteMarker bool
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
		if errs2.IsRPC(err, rpcstatus.MethodNotAllowed) {
			return nil, ErrMethodNotAllowed
		}
		return nil, convertKnownErrors(err, bucket, key)
	}

	return convertObject(&obj), nil
}

// DeleteObject deletes the object at the specific key.
// Returned deleted is not nil when the access grant has read permissions and
// the object was deleted.
func DeleteObject(ctx context.Context, project *uplink.Project, bucket, key string, version []byte) (info *VersionedObject, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	obj, err := db.DeleteObject(ctx, bucket, key, version)
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

// DownloadObject starts a download from the specific key and version. If version is empty latest object will be downloaded.
func DownloadObject(ctx context.Context, project *uplink.Project, bucket, key string, version []byte, options *uplink.DownloadOptions) (_ *VersionedDownload, err error) {
	defer mon.Task()(&ctx)(&err)

	download, err := downloadObjectWithVersion(ctx, project, bucket, key, version, options)
	if err != nil {
		if errs2.IsRPC(err, rpcstatus.MethodNotAllowed) {
			return nil, ErrMethodNotAllowed
		}
		return nil, err
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

// ListObjectsOptions defines object listing options.
type ListObjectVersionsOptions struct {
	// Prefix allows to filter objects by a key prefix.
	// If not empty, it must end with slash.
	Prefix string
	// Cursor sets the starting position of the iterator.
	// The first item listed will be the one after the cursor.
	// Cursor is relative to Prefix.
	Cursor        string
	VersionCursor []byte
	// Recursive iterates the objects without collapsing prefixes.
	Recursive bool

	// System includes SystemMetadata in the results.
	System bool
	// Custom includes CustomMetadata in the results.
	Custom bool

	Limit int
}

func ListObjectVersions(ctx context.Context, project *uplink.Project, bucket string, options *ListObjectVersionsOptions) (objects []*VersionedObject, more bool, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, false, convertKnownErrors(err, bucket, "")
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	opts := metaclient.ListOptions{
		IncludeAllVersions: true,
		Direction:          metaclient.After,
	}
	if options != nil {
		opts.Prefix = options.Prefix
		opts.Cursor = options.Cursor
		opts.VersionCursor = options.VersionCursor
		opts.Recursive = options.Recursive

		opts.IncludeSystemMetadata = options.System
		opts.IncludeCustomMetadata = options.Custom
		opts.Limit = options.Limit
	}

	result, err := db.ListObjects(ctx, bucket, opts)
	if err != nil {
		return nil, false, convertKnownErrors(err, bucket, "")
	}

	for _, item := range result.Items {
		obj := convertObject(&item)
		if obj != nil {
			obj.DeleteMarker = item.IsDeleteMarker
			objects = append(objects, obj)
		}
	}

	return objects, result.More, nil
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
			Custom:   obj.Metadata,
			IsPrefix: obj.IsPrefix,
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

//go:linkname downloadObjectWithVersion storj.io/uplink.downloadObjectWithVersion
func downloadObjectWithVersion(ctx context.Context, project *uplink.Project, bucket, key string, version []byte, options *uplink.DownloadOptions) (_ *uplink.Download, err error)
