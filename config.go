// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"crypto/x509"
	"time"

	"storj.io/common/identity"
	"storj.io/common/macaroon"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/metainfo"
)

// Logger defines the minimal logger that uplink can use.
// TODO: how complicated should be this logger?
type Logger interface {
	Println(...interface{})
}

// Config defines configuration for using uplink library.
type Config struct {
	Log       Logger
	UserAgent string

	// When whitelist is not defined then it uses system defaults.
	Whitelist CAWhitelist

	// DialTimeout defines how long client should wait for establishing
	// a connection to peers.
	DialTimeout time.Duration
}

// CAWhitelist defines which peers can be contacted.
// TODO: should this be called TLSCAWhitelist or something?
type CAWhitelist interface {
	InsecureSkipVerify() bool
	Certificates() []*x509.Certificate
}

// InsecureSkipConnectionVerify returns a whitelist that allows to connecting untrusted nodes.
func InsecureSkipConnectionVerify() CAWhitelist {
	return &caWhitelist{skip: true}
}

type caWhitelist struct {
	skip         bool
	certificates []*x509.Certificate
}

func (whitelist *caWhitelist) InsecureSkipVerify() bool {
	return whitelist.skip
}

func (whitelist *caWhitelist) Certificates() []*x509.Certificate {
	return whitelist.certificates
}

func (config Config) dial(ctx context.Context, satelliteNodeURL string, apiKey *macaroon.APIKey) (_ *metainfo.Client, _ rpc.Dialer, fullNodeURL string, err error) {
	ident, err := identity.NewFullIdentity(ctx, identity.NewCAOptions{
		Difficulty:  9,
		Concurrency: 1,
	})
	if err != nil {
		return nil, rpc.Dialer{}, "", Error.Wrap(err)
	}

	if config.Whitelist == nil {
		config.Whitelist = &caWhitelist{skip: false}
	}

	tlsConfig := tlsopts.Config{
		UsePeerCAWhitelist: !config.Whitelist.InsecureSkipVerify(),
		PeerIDVersions:     "0",
	}

	tlsOptions, err := tlsopts.NewOptions(ident, tlsConfig, nil)
	if err != nil {
		return nil, rpc.Dialer{}, "", Error.Wrap(err)
	}

	dialer := rpc.NewDefaultDialer(tlsOptions)
	dialer.DialTimeout = config.DialTimeout

	nodeURL, err := storj.ParseNodeURL(satelliteNodeURL)
	if err != nil {
		return nil, rpc.Dialer{}, "", Error.Wrap(err)
	}

	// Node id is required in satelliteNodeID for all unknown (non-storj) satellites.
	// For known satellite it will be automatically prepended.
	if nodeURL.ID.IsZero() {
		nodeID, found := rpc.KnownNodeID(nodeURL.Address)
		if !found {
			return nil, rpc.Dialer{}, "", Error.New("node id is required in satelliteNodeURL")
		}
		satelliteNodeURL = storj.NodeURL{
			ID:      nodeID,
			Address: nodeURL.Address,
		}.String()
	}

	metainfo, err := metainfo.Dial(ctx, dialer, satelliteNodeURL, apiKey, config.UserAgent)
	return metainfo, dialer, satelliteNodeURL, Error.Wrap(err)
}
