// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

// +build !quic

package uplink

import (
	"storj.io/common/rpc"
)

// newDefaultConnector returns a tcp connector.
func newDefaultConnector(dialAdapter *rpc.ConnectorAdapter) rpc.Connector {
	return rpc.NewDefaultTCPConnector(dialAdapter)
}
