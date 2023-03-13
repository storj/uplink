// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package access

import (
	"storj.io/common/macaroon"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
)

// EnablePathEncryptionBypass enables path encryption bypass for embedded encryption access.
func EnablePathEncryptionBypass(access *uplink.Access) error {
	encAccess := expose.AccessGetEncAccess(access)
	encAccess.Store.EncryptionBypass = true
	return nil
}

// APIKey returns the API key.
func APIKey(access *uplink.Access) *macaroon.APIKey {
	return expose.AccessGetAPIKey(access)
}

// DisableObjectKeyEncryption disables the encryption of object keys for newly
// uploaded objects.
//
// Disabling the encryption of object keys means that the object keys are
// stored in plain text in the satellite database. This allows object listings
// to be returned in lexicographically sorted order.
//
// Object content is still encrypted as usual.
func DisableObjectKeyEncryption(config *uplink.Config) {
	expose.ConfigDisableObjectKeyEncryption(config)
}
