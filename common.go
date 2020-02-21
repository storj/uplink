// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
)

var mon = monkit.Package()

// Error is default error class for uplink.
var Error = errs.Class("uplink")
