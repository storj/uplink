// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package edge

import (
	"crypto/tls"
	"crypto/x509"

	"storj.io/common/rpc"
)

// Config contains configuration on how to access edge services.
type Config struct {
	// AuthServiceAddress sets a fixed DRPC server including port.
	// Valid is auth.storjshare.io:7777 or a third party hosted alternative.
	AuthServiceAddress string

	// CertificatePEM contains the root certificate(s) or chain(s) against which
	// Uplink checks the auth service.
	// In PEM format.
	// Intended to test against a self-hosted auth service or to improve security.
	CertificatePEM []byte
}

func (config *Config) createDialer() rpc.Dialer {
	//lint:ignore SA1019 deprecated okay,
	//nolint:staticcheck // deprecated okay.
	connector := rpc.NewDefaultTCPConnector(nil)
	connector.SetSendDRPCMuxHeader(false)

	dialer := rpc.NewDefaultDialer(nil)
	dialer.Connector = connector
	dialer.HostnameTLSConfig = &tls.Config{}

	if len(config.CertificatePEM) > 0 {
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(config.CertificatePEM)

		dialer.HostnameTLSConfig.RootCAs = certPool
	}

	return dialer
}
