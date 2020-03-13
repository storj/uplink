// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package access

import (
	"github.com/zeebo/errs"

	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
)

// EnablePathEncryptionBypass enables path encryption bypass for embedded encryption access.
func EnablePathEncryptionBypass(access *uplink.Access) error {
	fn, ok := expose.EnablePathEncryptionBypass.(func(access *uplink.Access) error)
	if !ok {
		return errs.New("invalid type %T", fn)
	}
	return fn(access)
}
