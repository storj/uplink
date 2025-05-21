// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"context"
	"crypto/tls"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/pb"
	"storj.io/common/testcontext"
	"storj.io/uplink"
	"storj.io/uplink/edge"
	"storj.io/uplink/testsuite/internal"
)

type DRPCServerMock struct {
	pb.DRPCEdgeAuthServer
}

func (g *DRPCServerMock) RegisterAccess(context.Context, *pb.EdgeRegisterAccessRequest) (*pb.EdgeRegisterAccessResponse, error) {
	return &pb.EdgeRegisterAccessResponse{
		AccessKeyId: "l5pucy3dmvzxgs3fpfewix27l5pq",
		SecretKey:   "l5pvgzldojsxis3fpfpv6x27l5pv6x27l5pv6x27l5pv6",
		Endpoint:    "https://gateway.example",
	}, nil
}

func TestRegisterAccessUnencrypted(t *testing.T) {
	ctx := testcontext.NewWithTimeout(t, 10*time.Second)
	defer ctx.Cleanup()

	cancelCtx, authCancel := context.WithCancel(ctx)
	port := internal.StartAuthServiceUnencrypted(cancelCtx, ctx, t, &DRPCServerMock{})
	defer authCancel()

	remoteAddr := "localhost:" + strconv.Itoa(port)

	access, err := uplink.ParseAccess(internal.MinimalSerializedAccess)
	require.NoError(t, err)

	edgeConfig := edge.Config{
		AuthServiceAddress:            remoteAddr,
		InsecureUnencryptedConnection: true,
	}
	credentials, err := edgeConfig.RegisterAccess(ctx, access, nil)
	require.NoError(t, err)

	require.Equal(
		t,
		&edge.Credentials{
			AccessKeyID: "l5pucy3dmvzxgs3fpfewix27l5pq",
			SecretKey:   "l5pvgzldojsxis3fpfpv6x27l5pv6x27l5pv6x27l5pv6",
			Endpoint:    "https://gateway.example",
		},
		credentials,
	)
}

func TestRegisterAccessTLS(t *testing.T) {
	ctx := testcontext.NewWithTimeout(t, 10*time.Second)
	defer ctx.Cleanup()

	certificatePEM, privateKeyPEM := internal.CreateSelfSignedCertificate(t, "localhost")

	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	require.NoError(t, err)

	cancelCtx, authCancel := context.WithCancel(ctx)
	port := internal.StartAuthServiceTLS(cancelCtx, ctx, t, &DRPCServerMock{}, certificate)
	defer authCancel()

	remoteAddr := "localhost:" + strconv.Itoa(port)

	access, err := uplink.ParseAccess(internal.MinimalSerializedAccess)
	require.NoError(t, err)

	edgeConfig := edge.Config{
		AuthServiceAddress: remoteAddr,
		CertificatePEM:     certificatePEM,
	}
	credentials, err := edgeConfig.RegisterAccess(ctx, access, nil)
	require.NoError(t, err)

	require.Equal(
		t,
		&edge.Credentials{
			AccessKeyID: "l5pucy3dmvzxgs3fpfewix27l5pq",
			SecretKey:   "l5pvgzldojsxis3fpfpv6x27l5pv6x27l5pv6x27l5pv6",
			Endpoint:    "https://gateway.example",
		},
		credentials,
	)
}
