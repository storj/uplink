// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package object

import (
	"context"
	"errors"
	"reflect"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/errs2"
	"storj.io/common/rpc/rpcstatus"
	"storj.io/common/storj"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
	privateBucket "storj.io/uplink/private/bucket"
	"storj.io/uplink/private/metaclient"
	privateProject "storj.io/uplink/private/project"
	"storj.io/uplink/private/storage/streams"
)

var mon = monkit.Package()

// Error is default error class for uplink.
var packageError = errs.Class("object")

var (
	// ErrMethodNotAllowed is returned when method is not allowed against specified entity (e.g. object).
	ErrMethodNotAllowed = errors.New("method not allowed")

	// ErrObjectKeyMissing is returned when an object key is expected but not provided.
	ErrObjectKeyMissing = errors.New("object key is missing")

	// ErrObjectKeyTooLong is returned when an object key is too long.
	ErrObjectKeyTooLong = errors.New("object key is too long")

	// ErrObjectVersionInvalid is returned when an object version is invalid.
	ErrObjectVersionInvalid = errors.New("object version is invalid")

	// ErrBucketNameMissing is returned when a bucket name is expected but not provided.
	ErrBucketNameMissing = errors.New("bucket name is missing")

	// ErrNoObjectLockConfiguration is returned when a locked object is copied to a bucket without object lock configuration.
	ErrNoObjectLockConfiguration = errors.New("destination bucket has no object lock configuration")

	// ErrRetentionNotFound is returned when getting object retention for an object that has none.
	ErrRetentionNotFound = errors.New("object has no retention configuration")

	// ErrObjectProtected is returned when object is protected due to applied Object Lock settings.
	ErrObjectProtected = errors.New("object is protected by Object Lock settings")

	// ErrObjectLockInvalidObjectState is returned when attempting to perform Object Lock operations on an object in an invalid state.
	ErrObjectLockInvalidObjectState = errors.New("object state is invalid for Object Lock")

	// ErrObjectLockUploadWithTTLAndDefaultRetention is returned when attempting to specify an object expiration time
	// when uploading into a bucket with default retention settings.
	ErrObjectLockUploadWithTTLAndDefaultRetention = errors.New("cannot specify an object expiration time when uploading into a bucket with default retention settings")

	// ErrObjectLockUploadWithTTLAPIKeyAndDefaultRetention is returned when attempting to upload into a bucket
	// with default retention settings using an API key that enforces an object expiration time.
	ErrObjectLockUploadWithTTLAPIKeyAndDefaultRetention = errors.New("cannot upload into a bucket with default retention settings using an API key that enforces an object expiration time")

	// ErrDeleteObjectsNoItems is returned when attempting to delete an empty list of objects from a bucket.
	ErrDeleteObjectsNoItems = errors.New("at least one object must be specified for deletion")

	// ErrDeleteObjectsTooManyItems is returned when a list of objects to delete from a bucket is too large.
	ErrDeleteObjectsTooManyItems = errors.New("too many objects specified for deletion")

	// ErrDeleteObjectsUnimplemented is returned when the satellite does not have a DeleteObjects implementation.
	ErrDeleteObjectsUnimplemented = errors.New("DeleteObjects is not implemented")

	// ErrUnsupportedDelimiter is returned when an unsupported delimiter is provided in a listing request.
	ErrUnsupportedDelimiter = errors.New("unsupported delimiter")

	// ErrFailedPrecondition is returned when requests cannot be completed due to failed preconditions.
	ErrFailedPrecondition = errs.New("failed precondition")

	// ErrUnimplemented is returned for requests with unimplemented options.
	ErrUnimplemented = errs.New("unimplemented")

	rpcCodeToError = map[rpcstatus.StatusCode]error{
		rpcstatus.MethodNotAllowed:                                 ErrMethodNotAllowed,
		rpcstatus.ObjectLockBucketRetentionConfigurationMissing:    ErrNoObjectLockConfiguration,
		rpcstatus.ObjectLockInvalidBucketState:                     privateBucket.ErrBucketInvalidStateObjectLock,
		rpcstatus.ObjectLockObjectProtected:                        ErrObjectProtected,
		rpcstatus.ObjectLockInvalidObjectState:                     ErrObjectLockInvalidObjectState,
		rpcstatus.ObjectLockUploadWithTTLAndDefaultRetention:       ErrObjectLockUploadWithTTLAndDefaultRetention,
		rpcstatus.ObjectLockUploadWithTTLAPIKeyAndDefaultRetention: ErrObjectLockUploadWithTTLAPIKeyAndDefaultRetention,
		rpcstatus.ObjectKeyMissing:                                 ErrObjectKeyMissing,
		rpcstatus.ObjectKeyTooLong:                                 ErrObjectKeyTooLong,
		rpcstatus.ObjectVersionInvalid:                             ErrObjectVersionInvalid,
		rpcstatus.BucketNotFound:                                   uplink.ErrBucketNotFound,
		rpcstatus.BucketNameMissing:                                ErrBucketNameMissing,
		rpcstatus.BucketNameInvalid:                                uplink.ErrBucketNameInvalid,
		rpcstatus.DeleteObjectsNoItems:                             ErrDeleteObjectsNoItems,
		rpcstatus.DeleteObjectsTooManyItems:                        ErrDeleteObjectsTooManyItems,
		rpcstatus.FailedPrecondition:                               ErrFailedPrecondition,
		rpcstatus.Unimplemented:                                    ErrUnimplemented,
	}
)

// IPSummary contains information about the object IP-s.
type IPSummary = metaclient.GetObjectIPsResponse

// DeleteObjectsOptions contains additional options for deleting objects.
type DeleteObjectsOptions = metaclient.DeleteObjectsOptions

// DeleteObjectsItem describes the location of an object in a bucket to be deleted.
type DeleteObjectsItem = metaclient.DeleteObjectsItem

// DeleteObjectsResultItem represents the result of an individual DeleteObjects deletion.
type DeleteObjectsResultItem = metaclient.DeleteObjectsResultItem

// VersionedObject represents object with version.
// TODO find better place of name for this and related things.
type VersionedObject struct {
	uplink.Object
	Version []byte
	ETag    []byte
	// IsVersioned reports whether VersionedObject is a truly versioned
	// object, such as an object uploaded to a bucket with versioning
	// active. VersionedObject not being versioned can happen when it's
	// an object uploaded to a bucket with versioning disabled or
	// suspended.
	IsVersioned    bool
	IsDeleteMarker bool
	IsLatest       bool
	LegalHold      *bool
	Retention      *metaclient.Retention
}

// VersionedUpload represents upload which returnes object version at the end.
type VersionedUpload struct {
	upload *uplink.Upload
}

// UploadOptions contains additional options for uploading objects.
type UploadOptions = metaclient.UploadOptions

// DeleteObjectOptions contains additional options for deleting objects.
type DeleteObjectOptions = metaclient.DeleteObjectOptions

// SetObjectRetentionOptions contains additional options for setting an object's retention.
type SetObjectRetentionOptions = metaclient.SetObjectRetentionOptions

// ListObjectVersionsOptions defines listing options for versioned objects.
type ListObjectVersionsOptions struct {
	Prefix        string
	Delimiter     string
	Cursor        string
	VersionCursor []byte
	Recursive     bool
	System        bool
	Custom        bool
	Limit         int
}

// ListObjectsOptions defines listing options for objects.
type ListObjectsOptions struct {
	Prefix    string
	Delimiter string
	Cursor    string
	Recursive bool
	System    bool
	Custom    bool
	Limit     int
}

// CopyObjectOptions options for CopyObject method.
type CopyObjectOptions struct {
	Retention metaclient.Retention
	LegalHold bool

	IfNoneMatch []string
}

// Info returns the last information about the uploaded object.
func (upload *VersionedUpload) Info() *VersionedObject {
	metaObj := upload_getMetaclientObject(upload.upload)
	if metaObj == nil {
		return nil
	}
	obj := convertObject(metaObj)
	if meta := upload_getStreamMeta(upload.upload); meta != nil {
		obj.System.ContentLength = meta.Size
		obj.System.Created = meta.Modified
		obj.Version = meta.Version
		obj.IsVersioned = meta.IsVersioned
		obj.Retention = meta.Retention
		obj.LegalHold = meta.LegalHold
	}
	return obj
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
	return packageConvertKnownErrors(upload.upload.Commit(), "", "")
}

// SetCustomMetadata updates custom metadata to be included with the object.
// If it is nil, it won't be modified.
func (upload *VersionedUpload) SetCustomMetadata(ctx context.Context, custom uplink.CustomMetadata) error {
	return upload.upload.SetCustomMetadata(ctx, custom)
}

// SetETag updates etag metadata to be included with the object.
// If it is nil, it won't be modified.
func (upload *VersionedUpload) SetETag(ctx context.Context, etag []byte) error {
	return upload_setETag(ctx, upload.upload, etag)
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
	return convertObject(download_getMetaclientObject(download.download))
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
func DeleteObject(ctx context.Context, project *uplink.Project, bucket, key string, version []byte, options *DeleteObjectOptions) (info *VersionedObject, err error) {
	defer mon.Task()(&ctx)(&err)

	if options == nil {
		options = &DeleteObjectOptions{}
	}

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, packageConvertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	obj, err := db.DeleteObject(ctx, bucket, key, version, *options)
	if err != nil {
		return nil, packageConvertKnownErrors(err, bucket, key)
	}

	return convertObject(&obj), nil
}

// DeleteObjects deletes multiple objects from a bucket.
func DeleteObjects(ctx context.Context, project *uplink.Project, bucket string, items []DeleteObjectsItem, options *DeleteObjectsOptions) (_ []DeleteObjectsResultItem, err error) {
	defer mon.Task()(&ctx)(&err)

	if options == nil {
		options = &DeleteObjectsOptions{}
	}

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, packageConvertKnownErrors(err, bucket, "")
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	resultItems, err := db.DeleteObjects(ctx, bucket, items, *options)
	if err != nil {
		switch {
		case errs2.IsRPC(err, rpcstatus.Unimplemented):
			err = ErrDeleteObjectsUnimplemented
		case metaclient.ErrNoBucket.Has(err):
			err = ErrBucketNameMissing
		case metaclient.ErrNoPath.Has(err):
			err = ErrObjectKeyMissing
		case metaclient.ErrDeleteObjectsNoItems.Has(err):
			err = ErrDeleteObjectsNoItems
		default:
			err = packageConvertKnownErrors(err, bucket, "")
		}
		return nil, err
	}

	return resultItems, nil
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
		Delimiter:          "/",
	}

	if options != nil {
		opts.Prefix = options.Prefix
		opts.Cursor = options.Cursor
		opts.VersionCursor = options.VersionCursor
		opts.Recursive = options.Recursive
		opts.IncludeCustomMetadata = options.Custom
		opts.IncludeSystemMetadata = options.System
		opts.Limit = options.Limit

		if options.Delimiter != "" {
			opts.Delimiter = options.Delimiter
		}
	}

	obj, err := db.ListObjects(ctx, bucket, opts)
	if err != nil {
		return nil, false, convertKnownErrors(convertErrors(err), bucket, "")
	}

	var versions []*VersionedObject
	for _, o := range obj.Items {
		versions = append(versions, convertObject(&o))
	}

	return versions, obj.More, nil
}

// ListObjects returns a list of objects.
func ListObjects(ctx context.Context, project *uplink.Project, bucket string, options *ListObjectsOptions) (_ []*VersionedObject, more bool, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, false, convertKnownErrors(err, bucket, "")
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	opts := metaclient.ListOptions{
		Direction: metaclient.After,
		Delimiter: "/",
	}

	if options != nil {
		opts.Prefix = options.Prefix
		opts.Cursor = options.Cursor
		opts.Recursive = options.Recursive
		opts.IncludeCustomMetadata = options.Custom
		opts.IncludeSystemMetadata = options.System
		opts.Limit = options.Limit

		if options.Delimiter != "" {
			opts.Delimiter = options.Delimiter
		}
	}

	obj, err := db.ListObjects(ctx, bucket, opts)
	if err != nil {
		return nil, false, convertKnownErrors(convertErrors(err), bucket, "")
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
func UploadObject(ctx context.Context, project *uplink.Project, bucket, key string, options *UploadOptions) (_ *VersionedUpload, err error) {
	defer mon.Task()(&ctx)(&err)

	upload, err := uploadObjectWithRetention(ctx, project, bucket, key, options)
	if err != nil {
		return nil, packageConvertKnownErrors(err, bucket, key)
	}
	return &VersionedUpload{
		upload: upload,
	}, nil
}

// DownloadObjectOptions contains options for downloading an object.
type DownloadObjectOptions struct {
	// When Offset is negative it will read the suffix of the blob.
	// Combining negative offset and positive length is not supported.
	Offset int64
	// When Length is negative it will read until the end of the blob.
	Length int64

	ServerSideCopy bool
}

// DownloadObject starts a download from the specific key and version. If version is empty latest object will be downloaded.
func DownloadObject(ctx context.Context, project *uplink.Project, bucket, key string, version []byte, options *DownloadObjectOptions) (_ *VersionedDownload, err error) {
	defer mon.Task()(&ctx)(&err)

	var opts *metaclient.DownloadOptions
	if options != nil {
		sr, err := metaclient.NewStreamRange(options.Offset, options.Length)
		if err != nil {
			return nil, packageError.Wrap(err)
		}
		opts = &metaclient.DownloadOptions{
			Range:          sr,
			ServerSideCopy: options.ServerSideCopy,
		}
	}

	download, err := downloadObjectWithVersion(ctx, project, bucket, key, version, opts)
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
func CommitUpload(ctx context.Context, project *uplink.Project, bucket, key, uploadID string, opts *metaclient.CommitUploadOptions) (info *VersionedObject, err error) {
	defer mon.Task()(&ctx)(&err)

	obj, err := commitUpload(ctx, project, bucket, key, uploadID, opts)
	if err != nil {
		return nil, packageConvertKnownErrors(err, bucket, key)
	}

	return convertUplinkObject(obj), nil
}

// CopyObject atomically copies object to a different bucket or/and key.
func CopyObject(ctx context.Context, project *uplink.Project, sourceBucket, sourceKey string, sourceVersion []byte, targetBucket, targetKey string, options CopyObjectOptions) (_ *VersionedObject, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, packageConvertKnownErrors(err, sourceBucket, sourceKey)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	metaOpts := metaclient.CopyObjectOptions{}
	if !reflect.DeepEqual(options, (CopyObjectOptions{})) {
		metaOpts.Retention = options.Retention
		metaOpts.LegalHold = options.LegalHold
		metaOpts.IfNoneMatch = options.IfNoneMatch
	}
	obj, err := db.CopyObject(ctx, sourceBucket, sourceKey, sourceVersion, targetBucket, targetKey, metaOpts)
	if err != nil {
		return nil, packageConvertKnownErrors(err, sourceBucket, sourceKey)
	}

	return convertObject(obj), nil
}

// SetObjectLegalHold sets a legal hold configuration for the object at the specific key and version ID.
func SetObjectLegalHold(ctx context.Context, project *uplink.Project, bucket, key string, version []byte, enabled bool) (err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	err = db.SetObjectLegalHold(ctx, bucket, key, version, enabled)
	if err != nil {
		err = convertErrors(err)
	}

	return convertKnownErrors(err, bucket, key)
}

// GetObjectLegalHold retrieves a legal hold configuration for the object at the specific key and version ID.
func GetObjectLegalHold(ctx context.Context, project *uplink.Project, bucket, key string, version []byte) (enabled bool, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return false, convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	enabled, err = db.GetObjectLegalHold(ctx, bucket, key, version)
	if err != nil {
		err = convertErrors(err)
	}

	return enabled, convertKnownErrors(err, bucket, key)
}

// SetObjectRetention sets the retention for the object at the specific key and version ID.
func SetObjectRetention(ctx context.Context, project *uplink.Project, bucket, key string, version []byte, retention metaclient.Retention, opts *SetObjectRetentionOptions) (err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	if opts == nil {
		opts = &SetObjectRetentionOptions{}
	}

	err = db.SetObjectRetention(ctx, bucket, key, version, retention, *opts)
	if err != nil {
		err = convertErrors(err)
	}

	return convertKnownErrors(err, bucket, key)
}

// GetObjectRetention retrieves the retention for the object at the specific key and version ID.
func GetObjectRetention(ctx context.Context, project *uplink.Project, bucket, key string, version []byte) (retention *metaclient.Retention, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := dialMetainfoDB(ctx, project)
	if err != nil {
		return nil, convertKnownErrors(err, bucket, key)
	}
	defer func() { err = errs.Combine(err, db.Close()) }()

	retention, err = db.GetObjectRetention(ctx, bucket, key, version)
	if err != nil {
		err = convertErrors(err)
	}

	return retention, convertKnownErrors(err, bucket, key)
}

// convertObject converts metainfo.Object to Version.
func convertObject(obj *metaclient.Object) *VersionedObject {
	if obj == nil || obj.Bucket.Name == "" { // nil or zero object
		return nil
	}

	object := &VersionedObject{
		Object: uplink.Object{
			Key:      obj.Path,
			IsPrefix: obj.IsPrefix,
			System: uplink.SystemMetadata{
				Created:       obj.Created,
				Expires:       obj.Expires,
				ContentLength: obj.Size,
			},
			Custom: obj.Metadata,
		},
		ETag:           obj.ETag,
		Version:        obj.Version,
		IsVersioned:    obj.IsVersioned,
		IsDeleteMarker: obj.IsDeleteMarker,
		IsLatest:       obj.IsLatest,
		LegalHold:      obj.LegalHold,
		Retention:      obj.Retention,
	}

	if object.Custom == nil {
		object.Custom = uplink.CustomMetadata{}
	}

	return object
}

// convertObject converts metainfo.Object to Version.
func convertUplinkObject(obj *uplink.Object) *VersionedObject {
	if obj == nil {
		return nil
	}

	return &VersionedObject{
		Object:      *obj,
		ETag:        objectETag(obj),
		Version:     objectVersion(obj),
		IsVersioned: objectIsVersioned(obj),
		IsLatest:    objectIsLatest(obj),
	}
}

func packageConvertKnownErrors(err error, bucket, key string) error {
	if convertedErr, ok := rpcCodeToError[rpcstatus.Code(err)]; ok {
		return convertedErr
	}
	return convertKnownErrors(err, bucket, key)
}

func convertErrors(err error) error {
	switch {
	case metaclient.ErrProjectNoLock.Has(err):
		err = privateProject.ErrProjectNoLock
	case metaclient.ErrBucketNoLock.Has(err):
		err = privateBucket.ErrBucketNoLock
	case metaclient.ErrRetentionNotFound.Has(err):
		err = ErrRetentionNotFound
	case metaclient.ErrLockNotEnabled.Has(err):
		err = privateProject.ErrLockNotEnabled
	case metaclient.ErrMethodNotAllowed.Has(err):
		err = ErrMethodNotAllowed
	case metaclient.ErrObjectProtected.Has(err):
		err = ErrObjectProtected
	case metaclient.ErrObjectLockInvalidObjectState.Has(err):
		err = ErrObjectLockInvalidObjectState
	case metaclient.ErrUnsupportedDelimiter.Has(err):
		err = ErrUnsupportedDelimiter
	}

	return err
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoDB storj.io/uplink.dialMetainfoDB
func dialMetainfoDB(ctx context.Context, project *uplink.Project) (_ *metaclient.DB, err error)

//go:linkname encryptionParameters storj.io/uplink.encryptionParameters
func encryptionParameters(project *uplink.Project) storj.EncryptionParameters

//go:linkname objectETag storj.io/uplink.objectETag
func objectETag(object *uplink.Object) []byte

//go:linkname objectVersion storj.io/uplink.objectVersion
func objectVersion(object *uplink.Object) []byte

//go:linkname objectIsVersioned storj.io/uplink.objectIsVersioned
func objectIsVersioned(object *uplink.Object) bool

//go:linkname objectIsLatest storj.io/uplink.objectIsLatest
func objectIsLatest(object *uplink.Object) bool

//go:linkname downloadObjectWithVersion storj.io/uplink.downloadObjectWithVersion
func downloadObjectWithVersion(ctx context.Context, project *uplink.Project, bucket, key string, version []byte, options *metaclient.DownloadOptions) (_ *uplink.Download, err error)

//go:linkname download_getMetaclientObject storj.io/uplink.download_getMetaclientObject
func download_getMetaclientObject(dl *uplink.Download) *metaclient.Object

//go:linkname uploadObjectWithRetention storj.io/uplink.uploadObjectWithRetention
func uploadObjectWithRetention(ctx context.Context, project *uplink.Project, bucket, key string, options *metaclient.UploadOptions) (_ *uplink.Upload, err error)

//go:linkname commitUpload storj.io/uplink.commitUpload
func commitUpload(ctx context.Context, project *uplink.Project, bucket, key, uploadID string, opts *metaclient.CommitUploadOptions) (_ *uplink.Object, err error)

//go:linkname upload_getMetaclientObject storj.io/uplink.upload_getMetaclientObject
func upload_getMetaclientObject(u *uplink.Upload) *metaclient.Object

//go:linkname upload_getStreamMeta storj.io/uplink.upload_getStreamMeta
func upload_getStreamMeta(u *uplink.Upload) *streams.Meta

//go:linkname upload_setETag storj.io/uplink.upload_setETag
func upload_setETag(ctx context.Context, u *uplink.Upload, etag []byte) error
