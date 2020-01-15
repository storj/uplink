// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"storj.io/common/rpc"
	"storj.io/uplink/metainfo"
	"storj.io/uplink/metainfo/kvmetainfo"
)

// Project provides access to managing buckets.
type Project struct {
	config   Config
	dialer   rpc.Dialer
	metainfo *metainfo.Client
	project  *kvmetainfo.Project
}

// Open opens a project with the specified access.
func Open(ctx context.Context, access *Access) (*Project, error) {
	return (Config{}).Open(ctx, access)
}

// Open opens a project with the specified access.
func (config Config) Open(ctx context.Context, access *Access) (_ *Project, err error) {
	defer mon.Task()(&ctx)(&err)

	if access == nil {
		return nil, Error.New("access is nil")
	}

	if err := access.IsValid(); err != nil {
		return nil, Error.Wrap(err)
	}

	metainfo, dialer, _, err := config.dial(ctx, access.satelliteNodeURL, access.apiKey)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	project, err := kvmetainfo.SetupProject(metainfo)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return &Project{
		config:   config,
		dialer:   dialer,
		metainfo: metainfo,
		project:  project,
	}, nil
}

// Close closes the project and all associated resources.
func (project *Project) Close() error {
	return Error.Wrap(project.metainfo.Close())
}
