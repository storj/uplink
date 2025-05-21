// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package internal

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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/pb"
	"storj.io/common/testcontext"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

// MinimalSerializedAccess is the serialized form of a minimal access grant.
const MinimalSerializedAccess = "13J4Upun87ATb3T5T5sDXVeQaCzWFZeF9Ly4ELfxS5hUwTL8APEkwahTEJ1wxZjyErimiDs3kgid33kDLuYPYtwaY7Toy32mCTapfrUB814X13RiA844HPWK3QLKZb9cAoVceTowmNZXWbcUMKNbkMHCURE4hn8ZrdHPE3S86yngjvDxwKmarfGx"

// StartAuthServiceUnencrypted starts a DRPC auth server that accepts unencrypted connections.
func StartAuthServiceUnencrypted(
	cancelCtx context.Context,
	testCtx *testcontext.Context,
	t *testing.T,
	auth pb.DRPCEdgeAuthServer,
) (port int) {
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port = tcpListener.Addr().(*net.TCPAddr).Port

	mux := drpcmux.New()
	err = pb.DRPCRegisterEdgeAuth(mux, auth)
	require.NoError(t, err)

	server := drpcserver.New(mux)
	testCtx.Go(func() error {
		return server.Serve(cancelCtx, tcpListener)
	})

	return port
}

// StartAuthServiceTLS starts a DRPC auth server that accepts TLS-encrypted connections.
func StartAuthServiceTLS(
	cancelCtx context.Context,
	testCtx *testcontext.Context,
	t *testing.T,
	auth pb.DRPCEdgeAuthServer,
	certificate tls.Certificate,
) (port int) {
	serverTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{certificate},
	}

	// start a server with the certificate
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port = tcpListener.Addr().(*net.TCPAddr).Port

	tlsListener := tls.NewListener(tcpListener, serverTLSConfig)
	mux := drpcmux.New()
	err = pb.DRPCRegisterEdgeAuth(mux, auth)
	require.NoError(t, err)

	server := drpcserver.New(mux)
	testCtx.Go(func() error {
		return server.Serve(cancelCtx, tlsListener)
	})

	return port
}

// CreateSelfSignedCertificate creates a self-signed certificate.
// The certificate expires 1 minute after its creation.
func CreateSelfSignedCertificate(t *testing.T, hostname string) (certificatePEM []byte, privateKeyPEM []byte) {
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
