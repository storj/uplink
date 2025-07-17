// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package metaclient_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/uplink/private/metaclient"
)

func TestListOptions(t *testing.T) {
	opts := metaclient.ListOptions{
		Prefix:                "alpha/",
		Cursor:                "a",
		Delimiter:             "/",
		Recursive:             true,
		Direction:             metaclient.After,
		Limit:                 30,
		IncludeCustomMetadata: true,
		IncludeSystemMetadata: true,
		IncludeETag:           true,
		Status:                2,
	}

	list := metaclient.ObjectList{
		Bucket: "hello",
		Prefix: "alpha/",
		More:   true,
		Cursor: []byte("encrypted_path"),
		Items: []metaclient.Object{
			{Path: "alpha/xyz"},
		},
	}

	newopts := opts.NextPage(list)
	require.Equal(t, metaclient.ListOptions{
		Prefix:                "alpha/",
		Cursor:                "",
		CursorEnc:             []byte("encrypted_path"),
		Delimiter:             "/",
		Recursive:             true,
		Direction:             metaclient.After,
		Limit:                 30,
		IncludeCustomMetadata: true,
		IncludeSystemMetadata: true,
		IncludeETag:           true,
		Status:                2,
	}, newopts)
}
