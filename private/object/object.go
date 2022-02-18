// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package object

import (
	"context"
	_ "unsafe" // for go:linkname

	"github.com/zeebo/errs"

	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
	"storj.io/uplink/private/metaclient"
)

// Error is default error class for uplink.
var packageError = errs.Class("object")

// IPSummary contains information about the object IP-s.
type IPSummary = metaclient.GetObjectIPsResponse

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
