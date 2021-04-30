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
		Prefix:    "alpha/",
		Cursor:    "a",
		Delimiter: '/',
		Recursive: true,
		Direction: metaclient.After,
		Limit:     30,
	}

	list := metaclient.ObjectList{
		Bucket: "hello",
		Prefix: "alpha/",
		More:   true,
		Items: []metaclient.Object{
			{Path: "alpha/xyz"},
		},
	}

	newopts := opts.NextPage(list)
	require.Equal(t, metaclient.ListOptions{
		Prefix:    "alpha/",
		Cursor:    "alpha/xyz",
		Delimiter: '/',
		Recursive: true,
		Direction: metaclient.After,
		Limit:     30,
	}, newopts)
}
