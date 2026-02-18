// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package metaclient_test

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/errs2"
	"storj.io/uplink/private/metaclient"
)

func TestWithRetry(t *testing.T) {
	ctx := context.Background()

	numberOfExecutions := 0
	err := metaclient.WithRetry(ctx, func(cxt context.Context) error {
		numberOfExecutions++ //check-retry:ignore
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 1, numberOfExecutions)

	numberOfExecutions = 0
	err = metaclient.WithRetry(ctx, func(cxt context.Context) error {
		numberOfExecutions++ //check-retry:ignore
		return syscall.ECONNRESET
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, syscall.ECONNRESET))
	require.Greater(t, numberOfExecutions, 1)

	numberOfExecutions = 0
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	err = metaclient.WithRetry(cancelCtx, func(cxt context.Context) error {
		numberOfExecutions++ //check-retry:ignore
		return nil
	})
	require.Error(t, err)
	require.True(t, errs2.IsCanceled(err))
	require.Equal(t, numberOfExecutions, 0)
}
