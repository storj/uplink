// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

// +build quic

package connector

import (
	"storj.io/common/rpc"
	"storj.io/storj/pkg/quic"
)

// Get returns a quic connector.
func Get(dialAdapter *rpc.ConnectorAdapter) rpc.Connector {
	return quic.NewDefaultConnector(nil)
}
