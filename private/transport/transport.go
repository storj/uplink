// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package transport

import (
	"context"

	"storj.io/common/rpc"
	"storj.io/common/rpc/rpcpool"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
)

// SetConnectionPoolFactory configures connection pool on the passed in config. If
// argument pool is nil, it will clear the pool on the config.
func SetConnectionPoolFactory(ctx context.Context, config *uplink.Config, factory func(name string) *rpcpool.Pool) error {
	expose.ConfigSetConnectionPoolFactory(config, factory)
	return nil
}

// SetConnector overrides the default connector with the provided connector argument.
// If provided connector is nil, the default value will be used.
func SetConnector(config *uplink.Config, connector rpc.Connector) {
	expose.ConfigSetConnector(config, connector)
}

// SetMaximumBufferSize sets maximumBufferSize in config.
func SetMaximumBufferSize(config *uplink.Config, maximumBufferSize int) {
	expose.ConfigSetMaximumBufferSize(config, maximumBufferSize)
}
