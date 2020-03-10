// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package telemetry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/uplink/telemetry"
)

func TestEnable(t *testing.T) {
	ctx := context.Background()
	options := telemetry.ExtractOptions(ctx)
	assert.Nil(t, options)

	ctx, cancel := telemetry.Enable(ctx)
	defer cancel()

	options = telemetry.ExtractOptions(ctx)
	require.NotNil(t, options)
	assert.Equal(t, "collectora.storj.io:9000", options.Endpoint)
	assert.Equal(t, "uplink", options.Application)
}

func TestEnableWith(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := telemetry.EnableWith(ctx, &telemetry.Options{
		Endpoint:    "bob.com",
		Application: "jones",
	})
	defer cancel()

	options := telemetry.ExtractOptions(ctx)
	require.NotNil(t, options)
	assert.Equal(t, "bob.com", options.Endpoint)
	assert.Equal(t, "jones", options.Application)
}
