// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"storj.io/common/encryption"
	"storj.io/common/memory"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/internal/telemetryclient"
	"storj.io/uplink/private/ecclient"
	"storj.io/uplink/private/metainfo"
	"storj.io/uplink/private/storage/streams"
	"storj.io/uplink/private/testuplink"
)

// maxSegmentSize can be used to override max segment size with ldflags build parameter.
// Example: go build -ldflags "-X 'storj.io/uplink.maxSegmentSize=1MiB'" storj.io/storj/cmd/uplink.
var maxSegmentSize string

// Project provides access to managing buckets and objects.
type Project struct {
	config Config
	access *Access
	dialer rpc.Dialer

	eg        *errgroup.Group
	telemetry telemetryclient.Client
}

// OpenProject opens a project with the specific access grant.
func OpenProject(ctx context.Context, access *Access) (*Project, error) {
	return (Config{}).OpenProject(ctx, access)
}

// OpenProject opens a project with the specific access grant.
func (config Config) OpenProject(ctx context.Context, access *Access) (project *Project, err error) {
	defer mon.Func().RestartTrace(&ctx)(&err)

	if access == nil {
		return nil, packageError.New("access grant is nil")
	}

	var telemetry telemetryclient.Client
	if ctor, ok := telemetryclient.ConstructorFrom(ctx); ok {
		telemetry, err = ctor(access.satelliteAddress)
		if err != nil {
			return nil, err
		}

		defer func() {
			if err != nil {
				telemetry.Stop()
			}
		}()
	}

	dialer, _, err := config.getDialer(ctx, access.satelliteAddress, access.apiKey)
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

	return &Project{
		config: config,
		access: access,
		dialer: dialer,

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

	err = errs.Combine(err, project.dialer.Pool.Close())

	return packageError.Wrap(err)
}

func (project *Project) getStreamsStore(ctx context.Context) (_ *streams.Store, _ func() error, err error) {
	defer mon.Task()(&ctx)(&err)

	// TODO: this is redoing too much work every time

	// TODO: This should come from the EncryptionAccess. For now it's hardcoded to twice the
	// stripe size of the default redundancy scheme on the satellite.
	encBlockSize := 29 * 256 * memory.B.Int32()

	// TODO: What is the correct way to derive a named zap.Logger from config.Log?
	ec := ecclient.NewClient(zap.L().Named("ecclient"), project.dialer, 0)

	encryptionParameters := storj.EncryptionParameters{
		// TODO: the cipher should be provided by the Access, but we don't store it there yet.
		CipherSuite: storj.EncAESGCM,
		BlockSize:   encBlockSize,
	}

	// TODO: All these should be controlled by the satellite and not configured by the uplink.
	// For now we need to have these hard coded values that match the satellite configuration
	// to be able to create the underlying ecclient, segement store and stream store.
	var (
		segmentsSize  = 64 * memory.MiB.Int64()
		maxInlineSize = 4 * memory.KiB.Int()
	)

	if maxSegmentSize != "" {
		segmentsSize, err = memory.ParseString(maxSegmentSize)
		if err != nil {
			return nil, nil, packageError.Wrap(err)
		}
	} else {
		s, ok := testuplink.GetMaxSegmentSize(ctx)
		if ok {
			segmentsSize = s.Int64()
		}
	}

	maxEncryptedSegmentSize, err := encryption.CalcEncryptedSize(segmentsSize, encryptionParameters)
	if err != nil {
		return nil, nil, packageError.Wrap(err)
	}

	metainfoClient, err := project.getMetainfoClient(ctx)
	if err != nil {
		return nil, nil, packageError.Wrap(err)
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, metainfoClient.Close())
		}
	}()

	streamStore, err := streams.NewStreamStore(
		metainfoClient,
		ec,
		segmentsSize,
		project.access.encAccess.Store,
		encryptionParameters,
		maxInlineSize,
		maxEncryptedSegmentSize)
	if err != nil {
		return nil, nil, packageError.Wrap(err)
	}

	return streamStore, metainfoClient.Close, err
}

func (project *Project) getMetainfoDB(ctx context.Context) (_ *metainfo.DB, _ func() error, err error) {
	defer mon.Task()(&ctx)(&err)

	metainfoClient, err := project.getMetainfoClient(ctx)
	if err != nil {
		return nil, nil, packageError.Wrap(err)
	}

	return metainfo.New(metainfoClient, project.access.encAccess.Store), metainfoClient.Close, nil
}

func (project *Project) getMetainfoClient(ctx context.Context) (_ *metainfo.Client, err error) {
	defer mon.Task()(&ctx)(&err)

	metainfoClient, err := metainfo.DialNodeURL(ctx,
		project.dialer,
		project.access.satelliteAddress,
		project.access.apiKey,
		project.config.UserAgent)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	return metainfoClient, nil
}
