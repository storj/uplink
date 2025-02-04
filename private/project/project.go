// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package project

import (
	"context"
	"errors"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/signing"
	"storj.io/common/uuid"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
	"storj.io/uplink/private/metaclient"
)

var (
	mon          = monkit.Package()
	packageError = errs.Class("project")
)

var (
	// ErrProjectNoLock is returned when a project has object lock disabled.
	ErrProjectNoLock = errors.New("object lock is not enabled for this project")

	// ErrLockNotEnabled is returned when object lock is disabled as a feature.
	ErrLockNotEnabled = errors.New("object lock is not enabled")
)

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

//go:linkname setSatelliteSigner storj.io/uplink.setSatelliteSigner
func setSatelliteSigner(project *uplink.Project, signer signing.Signer)

// SetSatelliteSigner will enable an uplink Project's configuration to
// better coordinate with the Satellite by providing the Uplink the Satellite's
// ability to sign messages.
func SetSatelliteSigner(project *uplink.Project, signer signing.Signer) {
	setSatelliteSigner(project, signer)
}
