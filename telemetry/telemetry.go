// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package telemetry

import (
	"context"

	"storj.io/common/telemetry"
	"storj.io/uplink/internal/telemetryclient"
)

type telemetryKey int

const telemetryOptsKey telemetryKey = iota

// Options specify telemetry client options.
type Options struct {
	Endpoint    string
	Application string
}

// createClient creates a new client using the options.
func (options *Options) createClient(satelliteAddress string) (telemetryclient.Client, error) {
	client, err := telemetry.NewClient(options.Endpoint, telemetry.ClientOpts{
		Application: options.Application,
		Headers: map[string]string{
			"sat": satelliteAddress,
		},
	})
	return client, err
}

// Enable will attach default telemetry information to the ctx so telemetry
// will be reported during the project lifecycle.
func Enable(ctx context.Context) (context.Context, context.CancelFunc) {
	return EnableWith(ctx, &Options{
		Endpoint:    "collectora.storj.io:9000",
		Application: "uplink",
	})
}

// EnableWith will attach custom telemetry information to the ctx.
func EnableWith(ctx context.Context, opts *Options) (context.Context, context.CancelFunc) {
	ctx = context.WithValue(ctx, telemetryOptsKey, opts)
	ctx = telemetryclient.WithConstructor(ctx, opts.createClient)
	return context.WithCancel(ctx)
}

// ExtractOptions returns a bool whether the context has telemetry enabled
// and the options set.
func ExtractOptions(ctx context.Context) *Options {
	v := ctx.Value(telemetryOptsKey)
	if v == nil {
		return nil
	}

	opts, ok := v.(*Options)
	if !ok {
		return nil
	}

	return opts
}
