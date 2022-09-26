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
