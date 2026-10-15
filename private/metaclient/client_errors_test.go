// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package metaclient

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/errs2"
	"storj.io/common/rpc/rpcstatus"
	"storj.io/drpc"
	"storj.io/drpc/drpcwire"
)

func TestConvertErrorsUnknownRPC(t *testing.T) {
	// drpc reports an RPC unknown to the server as a protocol error without a status code.
	err := convertErrors(drpcwire.UnmarshalError(drpcwire.MarshalError(
		drpc.ProtocolError.New("unknown rpc: %q", "/metainfo.Metainfo/GetPendingObjectMetadata"))))
	require.True(t, ErrUnimplemented.Has(err))
	require.True(t, errs2.IsRPC(err, rpcstatus.Unimplemented))

	err = convertErrors(errs.New("something else"))
	require.False(t, ErrUnimplemented.Has(err))
}
