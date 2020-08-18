// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package object

import (
	"context"
	"net"

	"github.com/zeebo/errs"

	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
)

// GetObjectIPs exposes the GetObjectIPs method in the internal expose package.
func GetObjectIPs(ctx context.Context, config uplink.Config, access *uplink.Access, bucket, key string) (ips []net.IP, err error) {
	fn, ok := expose.GetObjectIPs.(func(ctx context.Context, config uplink.Config, access *uplink.Access, bucket, key string) (ips []net.IP, err error))
	if !ok {
		return nil, errs.New("invalid type %T", fn)
	}
	return fn(ctx, config, access, bucket, key)
}
