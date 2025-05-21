// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package edge

import (
	"context"
	"time"
	_ "unsafe" // for go:linkname

	"storj.io/uplink"
	publicEdge "storj.io/uplink/edge"
)

// Credentials represents S3-compatible credentials for use with the multi-tenant gateway.
type Credentials struct {
	publicEdge.Credentials

	// FreeTierRestrictedExpiration is the restricted expiration date of the
	// access grant. It is set if the original expiration date surpassed the
	// free-tier limit.
	FreeTierRestrictedExpiration *time.Time
}

// RegisterAccess gets credentials for the Storj-hosted Gateway and linkshare service.
// All files accessible under the Access are then also accessible via those services.
// If you call this function a lot, and the use case allows it,
// please limit the lifetime of the credentials
// by setting Permission.NotAfter when creating the Access.
func RegisterAccess(
	ctx context.Context,
	config *publicEdge.Config,
	access *uplink.Access,
	options *publicEdge.RegisterAccessOptions,
) (*Credentials, error) {
	creds, err := config.RegisterAccess(ctx, access, options)
	if err != nil {
		return nil, err
	}
	return &Credentials{
		Credentials:                  *creds,
		FreeTierRestrictedExpiration: credentialsFreeTierExpiration(creds),
	}, nil
}

//go:linkname credentialsFreeTierExpiration storj.io/uplink/edge.credentialsFreeTierExpiration
func credentialsFreeTierExpiration(credentials *publicEdge.Credentials) *time.Time
