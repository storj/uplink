// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

// +build !quic

package connector

import (
	"storj.io/common/rpc"
)

// Get returns a tcp connector.
func Get(dialAdapter *rpc.ConnectorAdapter) rpc.Connector {
	return rpc.NewDefaultTCPConnector(dialAdapter)
}
