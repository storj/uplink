// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"net"
	"time"

	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/rpc/rpcpool"
	"storj.io/common/socket"
	"storj.io/common/useragent"
	"storj.io/uplink/internal/expose"
)

func init() {
	expose.SetConnectionPool = setConnectionPool
}

// Config defines configuration for using uplink library.
type Config struct {
	UserAgent string

	// DialTimeout defines how long client should wait for establishing
	// a connection to peers.
	DialTimeout time.Duration

	// DialContext is how sockets are opened and is called to establish
	// a connection. If DialContext is nil, it'll try to use an implementation with background congestion control.
	DialContext func(ctx context.Context, network, address string) (net.Conn, error)

	pool *rpcpool.Pool
}

func (config Config) getDialer(ctx context.Context) (_ rpc.Dialer, err error) {
	ident, err := identity.NewFullIdentity(ctx, identity.NewCAOptions{
		Difficulty:  0,
		Concurrency: 1,
	})
	if err != nil {
		return rpc.Dialer{}, packageError.Wrap(err)
	}

	tlsConfig := tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}

	tlsOptions, err := tlsopts.NewOptions(ident, tlsConfig, nil)
	if err != nil {
		return rpc.Dialer{}, packageError.Wrap(err)
	}

	dialer := rpc.NewDefaultDialer(tlsOptions)
	if config.pool != nil {
		dialer.Pool = config.pool
	} else {
		dialer.Pool = rpc.NewDefaultConnectionPool()
	}
	dialer.DialTimeout = config.DialTimeout
	dialContext := config.DialContext
	if dialContext == nil {
		dialContext = socket.BackgroundDialer().DialContext
	}
	dialer.Connector = rpc.NewDefaultTCPConnector(&rpc.ConnectorAdapter{DialContext: dialContext})

	return dialer, nil
}

func setConnectionPool(ctx context.Context, config *Config, pool *rpcpool.Pool) error {
	if config == nil {
		return packageError.New("config is nil")
	}

	config.pool = pool
	return nil
}

func (config Config) validateUserAgent(ctx context.Context) error {
	if len(config.UserAgent) == 0 {
		return nil
	}

	if _, err := useragent.ParseEntries([]byte(config.UserAgent)); err != nil {
		return err
	}

	return nil
}
