// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"go.uber.org/zap"

	"storj.io/common/encryption"
	"storj.io/common/memory"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/private/ecclient"
	"storj.io/uplink/private/metainfo"
	"storj.io/uplink/private/metainfo/kvmetainfo"
	"storj.io/uplink/private/storage/segments"
	"storj.io/uplink/private/storage/streams"
)

// Project provides access to managing buckets and objects.
type Project struct {
	config   Config
	access   *Access
	dialer   rpc.Dialer
	metainfo *metainfo.Client
	project  *kvmetainfo.Project
	db       *kvmetainfo.DB
	streams  streams.Store
}

// OpenProject opens a project with the specific access.
func OpenProject(ctx context.Context, access *Access) (*Project, error) {
	return (Config{}).OpenProject(ctx, access)
}

// OpenProject opens a project with the specific access.
func (config Config) OpenProject(ctx context.Context, access *Access) (project *Project, err error) {
	defer mon.Task()(&ctx)(&err)

	if access == nil {
		return nil, Error.New("access is nil")
	}

	metainfo, dialer, _, err := config.dial(ctx, access.satelliteAddress, access.apiKey)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	proj, err := kvmetainfo.SetupProject(metainfo)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	// TODO: All these should be controlled by the satellite and not configured by the uplink.
	// For now we need to have these hard coded values that match the satellite configuration
	// to be able to create the underlying ecclient, segement store and stream store.
	var (
		segmentsSize  = 64 * memory.MiB.Int64()
		maxInlineSize = 4 * memory.KiB.Int()
	)

	// TODO: This should come from the EncryptionAccess. For now it's hardcoded to twice the
	// stripe size of the default redundancy scheme on the satellite.
	encBlockSize := 29 * 256 * memory.B.Int32()

	// TODO: What is the correct way to derive a named zap.Logger from config.Log?
	ec := ecclient.NewClient(zap.L().Named("ecclient"), dialer, 0)
	segmentStore := segments.NewSegmentStore(metainfo, ec)

	encryptionParameters := storj.EncryptionParameters{
		// TODO: the cipher should be provided by the Access, but we don't store it there yet.
		CipherSuite: storj.EncAESGCM,
		BlockSize:   encBlockSize,
	}

	maxEncryptedSegmentSize, err := encryption.CalcEncryptedSize(segmentsSize, encryptionParameters)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	streamStore, err := streams.NewStreamStore(metainfo, segmentStore, segmentsSize, access.encAccess.Store(), int(encryptionParameters.BlockSize), encryptionParameters.CipherSuite, maxInlineSize, maxEncryptedSegmentSize)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	db := kvmetainfo.New(proj, metainfo, streamStore, segmentStore, access.encAccess.Store())

	return &Project{
		config:   config,
		access:   access,
		dialer:   dialer,
		metainfo: metainfo,
		project:  proj,
		db:       db,
		streams:  streamStore,
	}, nil
}

// Close closes the project and all associated resources.
func (project *Project) Close() error {
	return Error.Wrap(project.metainfo.Close())
}
