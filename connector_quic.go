// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

// +build quic

package uplink

import (
	"storj.io/common/rpc"
	"storj.io/storj/pkg/quic"
)

// newDefaultConnector returns a quic connector.
func newDefaultConnector(dialAdapter *rpc.ConnectorAdapter) rpc.Connector {
	return quic.NewDefaultConnector(nil)
}
