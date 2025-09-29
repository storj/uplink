// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package project

import (
	"context"
	"errors"
	"time"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/pb"
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

// EdgeURLOverrides contains the URL overrides for the edge service.
type EdgeURLOverrides struct {
	AuthService         string
	PublicLinksharing   string
	InternalLinksharing string
}

// Info contains information of a project.
type Info struct {
	PublicId         uuid.UUID
	CreatedAt        time.Time
	Salt             []byte
	EdgeUrlOverrides *EdgeURLOverrides
}

func getProjectInfo(ctx context.Context, config uplink.Config, access *uplink.Access) (_ *pb.ProjectInfoResponse, err error) {
	dialer, err := expose.ConfigGetDialer(config, ctx)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, dialer.Pool.Close()) }()

	client, err := metaclient.DialNodeURL(ctx, dialer, access.SatelliteAddress(), expose.AccessGetAPIKey(access), config.UserAgent)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, client.Close()) }()

	info, err := client.GetProjectInfo(ctx)
	return info, err
}

// GetPublicID gets the public project ID for the given access grant.
func GetPublicID(ctx context.Context, config uplink.Config, access *uplink.Access) (id uuid.UUID, err error) {
	defer mon.Task()(&ctx)(&err)

	info, err := getProjectInfo(ctx, config, access)
	if err != nil {
		return id, convertKnownErrors(err, "", "")
	}
	copy(id[:], info.ProjectPublicId)

	return id, nil
}

// GetProjectInfo gets the project info for the given access grant.
func GetProjectInfo(ctx context.Context, config uplink.Config, access *uplink.Access) (_ Info, err error) {
	defer mon.Task()(&ctx)(&err)

	info, err := getProjectInfo(ctx, config, access)
	if err != nil {
		return Info{}, convertKnownErrors(err, "", "")
	}
	var id uuid.UUID
	copy(id[:], info.ProjectPublicId)

	projectInfo := Info{
		PublicId:  id,
		CreatedAt: info.ProjectCreatedAt,
		Salt:      info.ProjectSalt,
	}

	if info.EdgeUrlOverrides != nil {
		projectInfo.EdgeUrlOverrides = &EdgeURLOverrides{
			AuthService:         string(info.EdgeUrlOverrides.AuthService),
			PublicLinksharing:   string(info.EdgeUrlOverrides.PublicLinksharing),
			InternalLinksharing: string(info.EdgeUrlOverrides.PrivateLinksharing),
		}
	}

	return projectInfo, nil
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
