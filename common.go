// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"fmt"

	"github.com/spacemonkeygo/errors"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/common/errs2"
	"storj.io/common/rpc/rpcstatus"
)

var mon = monkit.Package()

// Error is default error class for uplink.
var packageError = errs.Class("uplink")

// ErrTooManyRequests is returned when user has sent too many requests in a given amount of time.
var ErrTooManyRequests = errors.New("too many requests")

// ErrBandwidthLimitExceeded is returned when project will exceeded bandwidth limit.
var ErrBandwidthLimitExceeded = errors.New("bandwidth limit exceeded")

func convertKnownErrors(err error) error {
	if errs2.IsRPC(err, rpcstatus.ResourceExhausted) {
		// TODO is a better way to do this?
		reErr := errs.Unwrap(err)
		if reErr.Error() == "Exceeded Usage Limit" {
			return packageError.Wrap(ErrBandwidthLimitExceeded)
		} else if reErr.Error() == "Too Many Requests" {
			return packageError.Wrap(ErrTooManyRequests)
		}
	}

	return packageError.Wrap(err)
}

func errwrapf(format string, err error, args ...interface{}) error {
	var all []interface{}
	all = append(all, err)
	all = append(all, args...)
	return packageError.Wrap(fmt.Errorf(format, all...))
}
