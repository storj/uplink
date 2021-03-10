// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"storj.io/common/memory"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/internal/telemetryclient"
	"storj.io/uplink/private/ecclient"
	"storj.io/uplink/private/metainfo"
	"storj.io/uplink/private/storage/streams"
	"storj.io/uplink/private/testuplink"
	"storj.io/uplink/private/version"
)

// TODO we need find a way how to pass it from satellite to client.
const maxInlineSize = 4096 // 4KiB

// maxSegmentSize can be used to override max segment size with ldflags build parameter.
// Example: go build -ldflags "-X 'storj.io/uplink.maxSegmentSize=1MiB'" storj.io/storj/cmd/uplink.
var maxSegmentSize string

// Project provides access to managing buckets and objects.
type Project struct {
	config               Config
	access               *Access
	dialer               rpc.Dialer
	ec                   ecclient.Client
	segmentSize          int64
	encryptionParameters storj.EncryptionParameters

	eg        *errgroup.Group
	telemetry telemetryclient.Client
}

// OpenProject opens a project with the specific access grant.
func OpenProject(ctx context.Context, access *Access) (*Project, error) {
	return (Config{}).OpenProject(ctx, access)
}

// OpenProject opens a project with the specific access grant.
func (config Config) OpenProject(ctx context.Context, access *Access) (project *Project, err error) {
	defer mon.Task()(&ctx)(&err)

	if access == nil {
		return nil, packageError.New("access grant is nil")
	}

	if err := config.validateUserAgent(ctx); err != nil {
		return nil, packageError.New("invalid user agent: %w", err)
	}

	config.UserAgent, err = version.AppendVersionToUserAgent(config.UserAgent)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	var telemetry telemetryclient.Client
	if ctor, ok := telemetryclient.ConstructorFrom(ctx); ok {
		telemetry, err = ctor(access.satelliteURL.String())
		if err != nil {
			return nil, err
		}

		defer func() {
			if err != nil {
				telemetry.Stop()
			}
		}()
	}

	dialer, err := config.getDialer(ctx)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	var eg errgroup.Group
	if telemetry != nil {
		eg.Go(func() error {
			telemetry.Run(ctx)
			return nil
		})
	}

	// TODO: This should come from the EncryptionAccess. For now it's hardcoded to twice the
	// stripe size of the default redundancy scheme on the satellite.
	encBlockSize := 29 * 256 * memory.B.Int32()

	encryptionParameters := storj.EncryptionParameters{
		// TODO: the cipher should be provided by the Access, but we don't store it there yet.
		CipherSuite: storj.EncAESGCM,
		BlockSize:   encBlockSize,
	}

	// TODO: All these should be controlled by the satellite and not configured by the uplink.
	// For now we need to have these hard coded values that match the satellite configuration
	// to be able to create the underlying stream store.
	var (
		segmentsSize = 64 * memory.MiB.Int64()
	)

	if maxSegmentSize != "" {
		segmentsSize, err = memory.ParseString(maxSegmentSize)
		if err != nil {
			return nil, packageError.Wrap(err)
		}
	} else {
		s, ok := testuplink.GetMaxSegmentSize(ctx)
		if ok {
			segmentsSize = s.Int64()
		}
	}

	// TODO: What is the correct way to derive a named zap.Logger from config.Log?
	ec := ecclient.NewClient(zap.L().Named("ecclient"), dialer, 0)

	return &Project{
		config:               config,
		access:               access,
		dialer:               dialer,
		ec:                   ec,
		segmentSize:          segmentsSize,
		encryptionParameters: encryptionParameters,

		eg:        &eg,
		telemetry: telemetry,
	}, nil
}

// Close closes the project and all associated resources.
func (project *Project) Close() (err error) {
	if project.telemetry != nil {
		project.telemetry.Stop()
		err = errs.Combine(
			project.eg.Wait(),
			project.telemetry.Report(context.Background()),
		)
	}

	// only close the connection pool if it's created through OpenProject
	if project.config.pool == nil {
		err = errs.Combine(err, project.dialer.Pool.Close())
	}

	return packageError.Wrap(err)
}

func (project *Project) getStreamsStore(ctx context.Context) (_ *streams.Store, err error) {
	defer mon.Task()(&ctx)(&err)

	metainfoClient, err := project.dialMetainfoClient(ctx)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, metainfoClient.Close())
		}
	}()

	streamStore, err := streams.NewStreamStore(
		metainfoClient,
		project.ec,
		project.segmentSize,
		project.access.encAccess.Store,
		project.encryptionParameters,
		maxInlineSize)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	return streamStore, nil
}

func (project *Project) dialMetainfoDB(ctx context.Context) (_ *metainfo.DB, err error) {
	defer mon.Task()(&ctx)(&err)

	metainfoClient, err := project.dialMetainfoClient(ctx)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	return metainfo.New(metainfoClient, project.access.encAccess.Store), nil
}

func (project *Project) dialMetainfoClient(ctx context.Context) (_ *metainfo.Client, err error) {
	defer mon.Task()(&ctx)(&err)

	metainfoClient, err := metainfo.DialNodeURL(ctx,
		project.dialer,
		project.access.satelliteURL.String(),
		project.access.apiKey,
		project.config.UserAgent)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	return metainfoClient, nil
}
