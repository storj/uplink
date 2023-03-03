// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package expose

import (
	"context"
	_ "unsafe" // for go:linkname

	"storj.io/common/grant"
	"storj.io/common/macaroon"
	"storj.io/common/rpc"
	"storj.io/common/rpc/rpcpool"
	"storj.io/uplink"
)

// ConfigSetConnectionPool exposes Config.setConnectionPool.
//
//go:linkname ConfigSetConnectionPool storj.io/uplink.config_setConnectionPool
func ConfigSetConnectionPool(*uplink.Config, *rpcpool.Pool)

// ConfigSetSatelliteConnectionPool exposes Config.setSatelliteConnectionPool.
//
//go:linkname ConfigSetSatelliteConnectionPool storj.io/uplink.config_setSatelliteConnectionPool
func ConfigSetSatelliteConnectionPool(*uplink.Config, *rpcpool.Pool)

// ConfigGetDialer exposes Config.getDialer.
//
//go:linkname ConfigGetDialer storj.io/uplink.config_getDialer
//nolint:revive
func ConfigGetDialer(uplink.Config, context.Context) (rpc.Dialer, error)

// ConfigSetMaximumBufferSize exposes Config.setMaximumBufferSize.
//
//go:linkname ConfigSetMaximumBufferSize storj.io/uplink.config_setMaximumBufferSize
func ConfigSetMaximumBufferSize(*uplink.Config, int)

// AccessGetAPIKey exposes Access.getAPIKey.
//
//go:linkname AccessGetAPIKey storj.io/uplink.access_getAPIKey
func AccessGetAPIKey(*uplink.Access) *macaroon.APIKey

// AccessGetEncAccess exposes Access.getEncAccess.
//
//go:linkname AccessGetEncAccess storj.io/uplink.access_getEncAccess
func AccessGetEncAccess(*uplink.Access) *grant.EncryptionAccess

// ConfigRequestAccessWithPassphraseAndConcurrency exposes Config.requestAccessWithPassphraseAndConcurrency.
//
//nolint:revive
//go:linkname ConfigRequestAccessWithPassphraseAndConcurrency storj.io/uplink.config_requestAccessWithPassphraseAndConcurrency
func ConfigRequestAccessWithPassphraseAndConcurrency(config uplink.Config, ctx context.Context, satelliteAddress, apiKey, passphrase string, concurrency uint8) (*uplink.Access, error)
