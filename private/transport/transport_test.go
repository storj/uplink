// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package transport

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/sudo"

	"storj.io/uplink"
)

func TestExpose(t *testing.T) {
	var cfg uplink.Config

	require.False(t, sudo.Sudo(reflect.ValueOf(cfg).FieldByName("disableBackgroundQoS")).Interface().(bool))

	DisableBackgroundQoS(&cfg, true)
	require.True(t, sudo.Sudo(reflect.ValueOf(cfg).FieldByName("disableBackgroundQoS")).Interface().(bool))

	DisableBackgroundQoS(&cfg, false)
	require.False(t, sudo.Sudo(reflect.ValueOf(cfg).FieldByName("disableBackgroundQoS")).Interface().(bool))
}
