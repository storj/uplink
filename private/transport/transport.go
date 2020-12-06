// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package transport

import (
	"context"

	"github.com/zeebo/errs"

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
