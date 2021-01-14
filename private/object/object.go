// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package object

import (
	"context"
	"net"

	"github.com/zeebo/errs"

	"storj.io/common/storj"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
	"storj.io/uplink/private/metainfo"
)

// Error is default error class for uplink.
var packageError = errs.Class("object")

// GetObjectIPs exposes the GetObjectIPs method in the internal expose package.
func GetObjectIPs(ctx context.Context, config uplink.Config, access *uplink.Access, bucket, key string) (ips []net.IP, err error) {
	dialer, err := expose.ConfigGetDialer(config, ctx)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, dialer.Pool.Close()) }()

	metainfoClient, err := metainfo.DialNodeURL(ctx, dialer, access.SatelliteAddress(), expose.AccessGetAPIKey(access), config.UserAgent)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, metainfoClient.Close()) }()

	db := metainfo.New(metainfoClient, expose.AccessGetEncAccess(access).Store)

	return db.GetObjectIPs(ctx, storj.Bucket{Name: bucket}, key)
}
