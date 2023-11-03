// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package object

import (
	"context"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

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

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoDB storj.io/uplink.dialMetainfoDB
func dialMetainfoDB(ctx context.Context, project *uplink.Project) (_ *metaclient.DB, err error)
