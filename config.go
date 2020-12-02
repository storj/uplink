// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"net"
	"time"

	"storj.io/common/identity"
	"storj.io/common/macaroon"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/socket"
	"storj.io/common/storj"
)

// Config defines configuration for using uplink library.
type Config struct {
	UserAgent string

	// DialTimeout defines how long client should wait for establishing
	// a connection to peers.
	DialTimeout time.Duration

	// DialContext is how sockets are opened and is called to establish
	// a connection. If DialContext is nil, it'll try to use an implementation with background congestion control.
	DialContext func(ctx context.Context, network, address string) (net.Conn, error)
}

func (config Config) getDialer(ctx context.Context, satelliteAddress string, apiKey *macaroon.APIKey) (_ rpc.Dialer, fullNodeURL string, err error) {
	ident, err := identity.NewFullIdentity(ctx, identity.NewCAOptions{
		Difficulty:  0,
		Concurrency: 1,
	})
	if err != nil {
		return rpc.Dialer{}, "", packageError.Wrap(err)
	}

	tlsConfig := tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}

	tlsOptions, err := tlsopts.NewOptions(ident, tlsConfig, nil)
	if err != nil {
		return rpc.Dialer{}, "", packageError.Wrap(err)
	}

	dialer := rpc.NewDefaultPooledDialer(tlsOptions)
	dialer.DialTimeout = config.DialTimeout
	dialContext := config.DialContext
	if dialContext == nil {
		dialContext = socket.BackgroundDialer().DialContext
	}
	dialer.Connector = rpc.NewDefaultTCPConnector(&rpc.ConnectorAdapter{DialContext: dialContext})

	nodeURL, err := storj.ParseNodeURL(satelliteAddress)
	if err != nil {
		return rpc.Dialer{}, "", packageError.Wrap(err)
	}

	// Node id is required in satelliteNodeID for all unknown (non-storj) satellites.
	// For known satellite it will be automatically prepended.
	if nodeURL.ID.IsZero() {
		nodeID, found := rpc.KnownNodeID(nodeURL.Address)
		if !found {
			return rpc.Dialer{}, "", packageError.New("node id is required in satelliteNodeURL")
		}
		satelliteAddress = storj.NodeURL{
			ID:      nodeID,
			Address: nodeURL.Address,
		}.String()
	}

	return dialer, satelliteAddress, packageError.Wrap(err)
}
