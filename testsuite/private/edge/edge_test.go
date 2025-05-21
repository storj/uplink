// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package edge

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/pb"
	"storj.io/common/testcontext"
	"storj.io/uplink"
	"storj.io/uplink/edge"
	privateEdge "storj.io/uplink/private/edge"
	"storj.io/uplink/testsuite/internal"
)

type drpcServerMock struct {
	pb.DRPCEdgeAuthServer

	freeTierRestrictedExpiration *time.Time
}

func (server *drpcServerMock) RegisterAccess(context.Context, *pb.EdgeRegisterAccessRequest) (*pb.EdgeRegisterAccessResponse, error) {
	return &pb.EdgeRegisterAccessResponse{
		FreeTierRestrictedExpiration: server.freeTierRestrictedExpiration,
	}, nil
}

func TestFreeTierAccessExpiration(t *testing.T) {
	ctx := testcontext.NewWithTimeout(t, 10*time.Second)
	defer ctx.Cleanup()

	expiration := time.Now().Add(time.Hour).UTC()

	cancelCtx, authCancel := context.WithCancel(ctx)
	port := internal.StartAuthServiceUnencrypted(cancelCtx, ctx, t, &drpcServerMock{
		freeTierRestrictedExpiration: &expiration,
	})
	defer authCancel()

	remoteAddr := "localhost:" + strconv.Itoa(port)

	access, err := uplink.ParseAccess(internal.MinimalSerializedAccess)
	require.NoError(t, err)

	edgeConfig := edge.Config{
		AuthServiceAddress:            remoteAddr,
		InsecureUnencryptedConnection: true,
	}
	credentials, err := privateEdge.RegisterAccess(ctx, &edgeConfig, access, nil)
	require.NoError(t, err)

	require.NotNil(t, credentials.FreeTierRestrictedExpiration)
	require.Equal(t, expiration, *credentials.FreeTierRestrictedExpiration)
}
