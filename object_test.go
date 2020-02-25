// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/uplink"
)

func TestCustomMetadata_Verify_Valid(t *testing.T) {
	metas := []uplink.CustomMetadata{
		{"content-type": "application/json"},
		{"hellö": "wörld"},
		{"hellö": "世界"},
		{"世界": "hellö"},
	}
	for _, meta := range metas {
		require.NoError(t, meta.Verify(), meta)
	}
}

func TestCustomMetadata_Verify_Invalid(t *testing.T) {
	metas := []uplink.CustomMetadata{
		{"": "application/json"},
		{"A\x00B": "no zero byte"},
		{"no zero byte": "A\x00B"},
		{"\x00": "no zero byte"},
		{"no zero byte": "\x00"},
		{"A\xff\xff\xff\xff\xffB": "no invalid rune"},
		{"no invalid rune": "A\xff\xff\xff\xff\xffB"},
	}
	for _, meta := range metas {
		require.Error(t, meta.Verify(), meta)
	}
}
