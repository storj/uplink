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
	// AuthServiceAddress: set a fixed DRPC server including port.
	// valid is auth.[eu|ap|us]1.storjshare.io:443
	// or a thirdparty-hosted alternative.
	//
	// Theoretically we can select the address based on the region
	// of the satellite in the access grant but there is no consensus
	// on the choice of mechanism to achieve that.
	AuthServiceAddress string

	// Root certificate(s) or chain(s) against which Uplink checks
	// the auth service.
	// In PEM format.
	// Intended to test against a self-hosted auth service
	// or to improve security
	CertificatePEM []byte
}

func (config *Config) createDialer() rpc.Dialer {
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(config.CertificatePEM)

	dialer := rpc.NewDefaultDialer(nil)
	dialer.HostnameTLSConfig = &tls.Config{
		RootCAs: certPool,
	}

	return dialer
}
