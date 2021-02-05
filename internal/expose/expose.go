// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package expose

import (
	"context"
	_ "unsafe" // for go:linkname

	"storj.io/common/macaroon"
	"storj.io/common/rpc"
	"storj.io/common/rpc/rpcpool"
	"storj.io/uplink"
	"storj.io/uplink/private/access2"
)

// ConfigSetConnectionPool exposes Config.setConnectionPool.
//
//go:linkname ConfigSetConnectionPool storj.io/uplink.(*Config).setConnectionPool
func ConfigSetConnectionPool(*uplink.Config, *rpcpool.Pool)

// ConfigGetDialer exposes Config.getDialer.
//
//nolint: golint
//go:linkname ConfigGetDialer storj.io/uplink.Config.getDialer
func ConfigGetDialer(uplink.Config, context.Context) (rpc.Dialer, error)

//  ConfigSetConnector exposes Config.setConnector
//
//nolint: golint
//go:linkname ConfigSetConnector storj.io/uplink.(*Config).setConnector
func ConfigSetConnector(*uplink.Config, rpc.Connector)

// AccessGetAPIKey exposes Access.getAPIKey.
//
//go:linkname AccessGetAPIKey storj.io/uplink.(*Access).getAPIKey
func AccessGetAPIKey(*uplink.Access) *macaroon.APIKey

// AccessGetEncAccess exposes Access.getEncAccess.
//
//go:linkname AccessGetEncAccess storj.io/uplink.(*Access).getEncAccess
func AccessGetEncAccess(*uplink.Access) *access2.EncryptionAccess

// ConfigRequestAccessWithPassphraseAndConcurrency exposes Config.requestAccessWithPassphraseAndConcurrency.
//
//nolint: golint
//go:linkname ConfigRequestAccessWithPassphraseAndConcurrency storj.io/uplink.Config.requestAccessWithPassphraseAndConcurrency
func ConfigRequestAccessWithPassphraseAndConcurrency(config uplink.Config, ctx context.Context, satelliteAddress, apiKey, passphrase string, concurrency uint8) (*uplink.Access, error)
