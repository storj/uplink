// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/pb"
	"storj.io/common/testcontext"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
	"storj.io/uplink"
	"storj.io/uplink/edge"
)

const minimalAccess = "13J4Upun87ATb3T5T5sDXVeQaCzWFZeF9Ly4ELfxS5hUwTL8APEkwahTEJ1wxZjyErimiDs3kgid33kDLuYPYtwaY7Toy32mCTapfrUB814X13RiA844HPWK3QLKZb9cAoVceTowmNZXWbcUMKNbkMHCURE4hn8ZrdHPE3S86yngjvDxwKmarfGx"

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
	port := startMockAuthServiceUnencrypted(cancelCtx, ctx, t)
	defer authCancel()

	remoteAddr := "localhost:" + strconv.Itoa(port)

	access, err := uplink.ParseAccess(minimalAccess)
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

	certificatePEM, privateKeyPEM := createSelfSignedCertificate(t, "localhost")

	certificate, err := tls.X509KeyPair(certificatePEM, privateKeyPEM)
	require.NoError(t, err)

	cancelCtx, authCancel := context.WithCancel(ctx)
	port := startMockAuthServiceTLS(cancelCtx, ctx, t, certificate)
	defer authCancel()

	remoteAddr := "localhost:" + strconv.Itoa(port)

	access, err := uplink.ParseAccess(minimalAccess)
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

func startMockAuthServiceUnencrypted(cancelCtx context.Context, testCtx *testcontext.Context, t *testing.T) (port int) {
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port = tcpListener.Addr().(*net.TCPAddr).Port

	mux := drpcmux.New()
	err = pb.DRPCRegisterEdgeAuth(mux, &DRPCServerMock{})
	require.NoError(t, err)

	server := drpcserver.New(mux)
	testCtx.Go(func() error {
		return server.Serve(cancelCtx, tcpListener)
	})

	return port
}

func startMockAuthServiceTLS(cancelCtx context.Context, testCtx *testcontext.Context, t *testing.T, certificate tls.Certificate) (port int) {
	serverTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{certificate},
	}

	// start a server with the certificate
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port = tcpListener.Addr().(*net.TCPAddr).Port

	tlsListener := tls.NewListener(tcpListener, serverTLSConfig)
	mux := drpcmux.New()
	err = pb.DRPCRegisterEdgeAuth(mux, &DRPCServerMock{})
	require.NoError(t, err)

	server := drpcserver.New(mux)
	testCtx.Go(func() error {
		return server.Serve(cancelCtx, tlsListener)
	})

	return port
}

func createSelfSignedCertificate(t *testing.T, hostname string) (certificatePEM []byte, privateKeyPEM []byte) {
	notAfter := time.Now().Add(1 * time.Minute)

	// first create a server certificate
	template := x509.Certificate{
		Subject: pkix.Name{
			CommonName: hostname,
		},
		DNSNames:              []string{hostname},
		SerialNumber:          big.NewInt(1337),
		BasicConstraintsValid: false,
		IsCA:                  true,
		NotAfter:              notAfter,
	}

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	certificateDERBytes, err := x509.CreateCertificate(
		rand.Reader,
		&template,
		&template,
		&privateKey.PublicKey,
		privateKey,
	)
	require.NoError(t, err)

	certificatePEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDERBytes})

	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	privateKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyBytes})

	return certificatePEM, privateKeyPEM
}
