// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"github.com/zeebo/errs"
	monkit "gopkg.in/spacemonkeygo/monkit.v2"
)

var (
	mon = monkit.Package()

	// Error is default error class for uplink.
	Error = errs.Class("uplink")
)
