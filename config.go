// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"time"

	"storj.io/common/identity"
	"storj.io/common/macaroon"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/metainfo"
)

// Logger defines the minimal logger that uplink can use.
// TODO: how complicated should be this logger?
type Logger interface {
	Println(...interface{})
}

// Config defines configuration for using uplink library.
type Config struct {
	Log       Logger
	UserAgent string

	// DialTimeout defines how long client should wait for establishing
	// a connection to peers.
	DialTimeout time.Duration
}

func (config Config) dial(ctx context.Context, satelliteNodeURL string, apiKey *macaroon.APIKey) (_ *metainfo.Client, _ rpc.Dialer, fullNodeURL string, err error) {
	ident, err := identity.NewFullIdentity(ctx, identity.NewCAOptions{
		Difficulty:  9,
		Concurrency: 1,
	})
	if err != nil {
		return nil, rpc.Dialer{}, "", Error.Wrap(err)
	}

	tlsConfig := tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}

	tlsOptions, err := tlsopts.NewOptions(ident, tlsConfig, nil)
	if err != nil {
		return nil, rpc.Dialer{}, "", Error.Wrap(err)
	}

	dialer := rpc.NewDefaultDialer(tlsOptions)
	dialer.DialTimeout = config.DialTimeout

	nodeURL, err := storj.ParseNodeURL(satelliteNodeURL)
	if err != nil {
		return nil, rpc.Dialer{}, "", Error.Wrap(err)
	}

	// Node id is required in satelliteNodeID for all unknown (non-storj) satellites.
	// For known satellite it will be automatically prepended.
	if nodeURL.ID.IsZero() {
		nodeID, found := rpc.KnownNodeID(nodeURL.Address)
		if !found {
			return nil, rpc.Dialer{}, "", Error.New("node id is required in satelliteNodeURL")
		}
		satelliteNodeURL = storj.NodeURL{
			ID:      nodeID,
			Address: nodeURL.Address,
		}.String()
	}

	metainfo, err := metainfo.DialNodeURL(ctx, dialer, satelliteNodeURL, apiKey, config.UserAgent)

	return metainfo, dialer, satelliteNodeURL, Error.Wrap(err)
}
