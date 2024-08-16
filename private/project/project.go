// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package project

import (
	"context"
	"errors"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/uuid"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
	"storj.io/uplink/private/metaclient"
)

var (
	mon          = monkit.Package()
	packageError = errs.Class("project")
)

// ErrProjectNoLock is returned when a project has object lock disabled.
var ErrProjectNoLock = errors.New("object lock is not enabled for this project")

// GetPublicID gets the public project ID for the given access grant.
func GetPublicID(ctx context.Context, config uplink.Config, access *uplink.Access) (id uuid.UUID, err error) {
	defer mon.Task()(&ctx)(&err)

	dialer, err := expose.ConfigGetDialer(config, ctx)
	if err != nil {
		return id, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, dialer.Pool.Close()) }()

	client, err := metaclient.DialNodeURL(ctx, dialer, access.SatelliteAddress(), expose.AccessGetAPIKey(access), config.UserAgent)
	if err != nil {
		return id, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, client.Close()) }()

	info, err := client.GetProjectInfo(ctx)
	if err != nil {
		return id, convertKnownErrors(err, "", "")
	}
	copy(id[:], info.ProjectPublicId)

	return id, nil
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error
