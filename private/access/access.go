// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package access

import (
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/grant"
	"storj.io/common/macaroon"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
)

var (
	// packageError is default error class for this package.
	packageError = errs.Class("access")

	// ErrPermissionInvalid is returned when an invalid Permission is encountered.
	ErrPermissionInvalid = errs.Class("invalid permission")

	allPermissions = []Permission{
		Download,
		Upload,
		List,
		Delete,
		PutObjectRetention,
		GetObjectRetention,
		PutObjectLegalHold,
		GetObjectLegalHold,
		BypassGovernanceRetention,
		PutBucketObjectLockConfiguration,
		GetBucketObjectLockConfiguration,
	}
)

// Permission represents an access grant permission.
type Permission uint16

const (
	// Download represents the permission to download an object's content. It
	// allows getting object metadata, but it does not allow listing buckets.
	Download = Permission(iota + 1)
	// Upload represents the permission to create buckets and upload new objects.
	// It does not allow overwriting existing objects unless Delete is granted too.
	Upload
	// List represents the permission to list buckets. It allows getting object
	// metadata, but it does not allow downloading the object's content.
	List
	// Delete represents the permission to delete buckets and objects. Unless
	// either AllowDownload or AllowList is granted too, no object metadata and
	// no error info will be returned for deleted objects.
	Delete
	// PutObjectRetention represents the permission for retention periods to be
	// placed on and retrieved from objects.
	PutObjectRetention
	// GetObjectRetention represents the permission for retention periods to be
	// retrieved from objects.
	GetObjectRetention
	// PutObjectLegalHold represents the permission for legal hold status to be
	// placed on objects.
	PutObjectLegalHold
	// GetObjectLegalHold represents the permission for legal hold status to be
	// retrieved from objects.
	GetObjectLegalHold
	// BypassGovernanceRetention represents the permission for governance retention
	// to be bypassed on objects.
	BypassGovernanceRetention
	// PutBucketObjectLockConfiguration represents the permission for an Object Lock
	// configuration to be placed on buckets.
	PutBucketObjectLockConfiguration
	// GetBucketObjectLockConfiguration represents the permission for an Object Lock
	// configuration to be retrieved from buckets.
	GetBucketObjectLockConfiguration
)

func (p Permission) applyToGrantPermission(grantPerm *grant.Permission) error {
	switch p {
	case Download:
		grantPerm.AllowDownload = true
	case Upload:
		grantPerm.AllowUpload = true
	case List:
		grantPerm.AllowList = true
	case Delete:
		grantPerm.AllowDelete = true
	case PutObjectRetention:
		grantPerm.AllowPutObjectRetention = true
	case GetObjectRetention:
		grantPerm.AllowGetObjectRetention = true
	case PutObjectLegalHold:
		grantPerm.AllowPutObjectLegalHold = true
	case GetObjectLegalHold:
		grantPerm.AllowGetObjectLegalHold = true
	case BypassGovernanceRetention:
		grantPerm.AllowBypassGovernanceRetention = true
	case PutBucketObjectLockConfiguration:
		grantPerm.AllowPutBucketObjectLockConfiguration = true
	case GetBucketObjectLockConfiguration:
		grantPerm.AllowGetBucketObjectLockConfiguration = true
	default:
		return ErrPermissionInvalid.New("%d", p)
	}
	return nil
}

type restrictParams struct {
	permission grant.Permission
	prefixes   []grant.SharePrefix
}

// ShareOption represents an option for sharing an access grant.
type ShareOption func(*restrictParams) error

// WithPrefix returns a ShareOption that restricts an access grant to the provided bucket name and object key prefix.
func WithPrefix(bucketName string, prefix string) ShareOption {
	return func(params *restrictParams) error {
		params.prefixes = append(params.prefixes, grant.SharePrefix{
			Bucket: bucketName,
			Prefix: prefix,
		})
		return nil
	}
}

// WithPermissions returns a ShareOption that grants the access grant the provided permissions.
func WithPermissions(permissions ...Permission) ShareOption {
	return func(params *restrictParams) error {
		for _, perm := range permissions {
			if err := perm.applyToGrantPermission(&params.permission); err != nil {
				return err
			}
		}
		return nil
	}
}

// WithAllPermissions returns a ShareOption that grants the access grant all permissions.
func WithAllPermissions() ShareOption {
	return WithPermissions(allPermissions...)
}

// NotAfter returns a ShareOption that sets an expiration time for the access grant.
func NotAfter(t time.Time) ShareOption {
	return func(params *restrictParams) error {
		params.permission.NotAfter = t
		return nil
	}
}

// NotBefore returns a ShareOption that sets a start time for the access grant.
func NotBefore(t time.Time) ShareOption {
	return func(params *restrictParams) error {
		params.permission.NotBefore = t
		return nil
	}
}

// WithMaxObjectTTL returns a ShareOption that sets the maximum lifetime of objects uploaded by the access grant.
func WithMaxObjectTTL(t time.Duration) ShareOption {
	return func(params *restrictParams) error {
		params.permission.MaxObjectTTL = &t
		return nil
	}
}

// Share creates a new access grant with specific permissions.
//
// Access grants can only have their existing permissions restricted,
// and the resulting access grant will only allow for the intersection of all previous
// Share calls in the access grant construction chain.
//
// Prefixes, if provided, restrict the access grant (and internal encryption information)
// to only contain enough information to allow access to just those prefixes.
//
// To revoke an access grant see the (*uplink.Project).RevokeAccess method.
func Share(access *uplink.Access, opts ...ShareOption) (*uplink.Access, error) {
	var params restrictParams
	for _, opt := range opts {
		if err := opt(&params); err != nil {
			return nil, packageError.Wrap(err)
		}
	}

	internal := expose.AccessToInternal(access)
	restricted, err := internal.Restrict(params.permission, params.prefixes...)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	return expose.AccessFromInternal(restricted), nil
}

// EnablePathEncryptionBypass enables path encryption bypass for embedded encryption access.
func EnablePathEncryptionBypass(access *uplink.Access) error {
	encAccess := expose.AccessGetEncAccess(access)
	encAccess.Store.EncryptionBypass = true
	return nil
}

// APIKey returns the API key.
func APIKey(access *uplink.Access) *macaroon.APIKey {
	return expose.AccessGetAPIKey(access)
}

// DisableObjectKeyEncryption disables the encryption of object keys for newly
// uploaded objects.
//
// Disabling the encryption of object keys means that the object keys are
// stored in plain text in the satellite database. This allows object listings
// to be returned in lexicographically sorted order.
//
// Object content is still encrypted as usual.
func DisableObjectKeyEncryption(config *uplink.Config) {
	expose.ConfigDisableObjectKeyEncryption(config)
}
