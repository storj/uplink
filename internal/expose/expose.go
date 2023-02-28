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

// ConfigSetConnectionPoolFactory exposes Config.setConnectionPoolFactory
//
//go:linkname ConfigSetConnectionPoolFactory storj.io/uplink.config_setConnectionPoolFactory
func ConfigSetConnectionPoolFactory(*uplink.Config, func(string) *rpcpool.Pool)

// ConfigGetDialer exposes Config.getDialer.
//
//go:linkname ConfigGetDialer storj.io/uplink.config_getDialer
//nolint:revive
func ConfigGetDialer(uplink.Config, context.Context) (rpc.Dialer, error)

// ConfigSetConnector exposes Config.setConnector.
//
//go:linkname ConfigSetConnector storj.io/uplink.config_setConnector
func ConfigSetConnector(*uplink.Config, rpc.Connector)

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
