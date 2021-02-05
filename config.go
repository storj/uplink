// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"net"
	"time"

	"storj.io/common/rpc"
	"storj.io/common/rpc/rpcpool"
	"storj.io/common/socket"
	"storj.io/common/useragent"
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

	pool      *rpcpool.Pool
	connector rpc.Connector
}

// getDialer returns a new rpc.Dialer corresponding to the config.
//
// NB: this is used with linkname in internal/expose.
// It needs to be updated when this is updated.
func (config Config) getDialer(ctx context.Context) (_ rpc.Dialer, err error) {
	tlsOptions, err := getProcessTLSOptions(ctx)
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

	if config.connector != nil {
		dialer.Connector = config.connector
	} else {
		dialContext := config.DialContext
		if dialContext == nil {
			dialContext = socket.BackgroundDialer().DialContext
		}

		dialer.Connector = rpc.NewDefaultTCPConnector(&rpc.ConnectorAdapter{DialContext: dialContext})
	}

	return dialer, nil
}

// setConnectionPool exposes setting connection pool.
//
// NB: this is used with linkname in internal/expose.
// It needs to be updated when this is updated.
//
//lint:ignore U1000, used with linkname
//nolint: unused
func (config *Config) setConnectionPool(pool *rpcpool.Pool) { config.pool = pool }

// setConnector exposes setting a connector used by the dialer.
//
// NB: this is used with linkname in internal/expose.
// It needs to be updated when this is updated.
//
//lint:ignore U1000, used with linkname
//nolint: unused
func (config *Config) setConnector(connector rpc.Connector) {
	config.connector = connector
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
