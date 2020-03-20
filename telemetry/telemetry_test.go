// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package telemetry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/uplink/telemetry"
)

func TestEnable(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	options := telemetry.ExtractOptions(ctx)
	assert.Nil(t, options)

	tctx, cancel := telemetry.Enable(ctx)
	defer cancel()

	options = telemetry.ExtractOptions(tctx)
	require.NotNil(t, options)
	assert.Equal(t, "collectora.storj.io:9000", options.Endpoint)
	assert.Equal(t, "uplink", options.Application)
}

func TestEnableWith(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	tctx, cancel := telemetry.EnableWith(ctx, &telemetry.Options{
		Endpoint:    "example.test",
		Application: "jones",
	})
	defer cancel()

	options := telemetry.ExtractOptions(tctx)
	require.NotNil(t, options)
	assert.Equal(t, "example.test", options.Endpoint)
	assert.Equal(t, "jones", options.Application)
}
