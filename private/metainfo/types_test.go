// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/uplink/private/metainfo"
)

func TestListOptions(t *testing.T) {
	opts := metainfo.ListOptions{
		Prefix:    "alpha/",
		Cursor:    "a",
		Delimiter: '/',
		Recursive: true,
		Direction: metainfo.After,
		Limit:     30,
	}

	list := metainfo.ObjectList{
		Bucket: "hello",
		Prefix: "alpha/",
		More:   true,
		Items: []metainfo.Object{
			{Path: "alpha/xyz"},
		},
	}

	newopts := opts.NextPage(list)
	require.Equal(t, metainfo.ListOptions{
		Prefix:    "alpha/",
		Cursor:    "alpha/xyz",
		Delimiter: '/',
		Recursive: true,
		Direction: metainfo.After,
		Limit:     30,
	}, newopts)
}
