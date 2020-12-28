// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package expose

// RequestAccessWithPassphraseAndConcurrency exposes uplink.requestAccessWithPassphraseAndConcurrency.
//
// func RequestAccessWithPassphraseAndConcurrency(ctx context.Context, config uplink.Config, satelliteNodeURL, apiKey, passphrase string, concurrency uint8) (_ *uplink.Access, err error).
var RequestAccessWithPassphraseAndConcurrency interface{}

// EnablePathEncryptionBypass exposes uplink.enablePathEncryptionBypass.
//
// func EnablePathEncryptionBypass(access *Access) error.
var EnablePathEncryptionBypass interface{}

// GetObjectIPs exposes uplink.getObjectIPs.
//
// func GetObjectIPs(ctx context.Context, config Config, access *Access, bucket, key string) (ips [][]byte, err error).
var GetObjectIPs interface{}

// SetConnectionPool exposes uplink.setConnectionPool.
//
// func SetConnectionPool(ctx context.Context, config *uplink.Config, pool *rpcpool.Pool) error.
var SetConnectionPool interface{}

// SetTLSOptions exposes uplink.setTLSOptions.
//
// func SetTLSOptions(ctx context.Context, config *uplink.Config, tlsOptions *tlsopts.Options) error {
var SetTLSOptions interface{}
