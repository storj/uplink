// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package transport

import (
	"context"

	"github.com/zeebo/errs"

	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc/rpcpool"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
)

// SetConnectionPool configures connection pool on the passed in config. If
// argument pool is nil, it will clear the pool on the config.
func SetConnectionPool(ctx context.Context, config *uplink.Config, pool *rpcpool.Pool) error {
	fn, ok := expose.SetConnectionPool.(func(ctx context.Context, config *uplink.Config, pool *rpcpool.Pool) error)
	if !ok {
		return errs.New("invalid type %T", fn)
	}
	return fn(ctx, config, pool)
}

// SetTLSOptions configures TLS configuration on the passed in config. If
// argument tlsOptions is nil, it will clear the TLS configuration on the config.
func SetTLSOptions(ctx context.Context, config *uplink.Config, tlsOptions *tlsopts.Options) error {
	fn, ok := expose.SetTLSOptions.(func(ctx context.Context, config *uplink.Config, tlsOptions *tlsopts.Options) error)
	if !ok {
		return errs.New("invalid type %T", fn)
	}
	return fn(ctx, config, tlsOptions)
}
