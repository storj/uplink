// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package eestream

import (
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
)

var (
	// Error is the default eestream errs class.
	Error = errs.Class("eestream")

	mon = monkit.Package()
)
