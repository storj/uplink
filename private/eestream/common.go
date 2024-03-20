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

	// QuiescentError is the class of errors returned when a stream is quiescent
	// and should be restarted.
	QuiescentError = errs.Class("quiescence")

	mon = monkit.Package()
)
