// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package edge_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/uplink/edge"
)

func TestCreateURL(t *testing.T) {
	testValidCase := func(description, baseURL, accessID, bucket, key string, options *edge.ShareURLOptions, expected string) {
		t.Run(description, func(t *testing.T) {
			url, err := edge.JoinShareURL(baseURL, accessID, bucket, key, options)
			require.Nil(t, err)
			require.Equal(t, expected, url)
		})
	}

	testInvalidCase := func(description, baseURL, accessID, bucket, key string, options *edge.ShareURLOptions, expectedErr string) {
		t.Run(description, func(t *testing.T) {
			url, err := edge.JoinShareURL(baseURL, accessID, bucket, key, options)
			require.NotNil(t, err)
			require.Equal(t, expectedErr, err.Error())
			require.Equal(t, "", url)
		})
	}

	testValidCase(
		"share entire project",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		"",
		nil,
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)

	testValidCase(
		"share entire bucket",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"",
		nil,
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa/mybucket",
	)

	testValidCase(
		"share entire prefix",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"my/prefix/",
		nil,
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa/mybucket/my/prefix/",
	)

	testValidCase(
		"share object nested in prefix",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"my/object",
		nil,
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa/mybucket/my/object",
	)

	testValidCase(
		"base url ends in slash",
		"https://linksharing.test/",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		"",
		nil,
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)

	testValidCase(
		"escaping of binary object key",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"a\x00a",
		nil,
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa/mybucket/a%00a",
	)

	testValidCase(
		"raw download link",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"myobject",
		&edge.ShareURLOptions{
			Raw: true,
		},
		"https://linksharing.test/raw/aaaaaaaaaaaaaaaaaaaaaaaaaaaa/mybucket/myobject",
	)

	testInvalidCase(
		"base url missing",
		"",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		"",
		nil,
		"uplink: invalid base url: \"\"",
	)

	testInvalidCase(
		"base url has no protocol",
		"linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		"",
		nil,
		"uplink: invalid base url: \"linksharing.test\"",
	)

	testInvalidCase(
		"bucket missing",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		"myobject",
		nil,
		"uplink: bucket is required if key is specified",
	)

	testInvalidCase(
		"access key ID missing",
		"https://linksharing.test",
		"",
		"",
		"",
		nil,
		"uplink: accessKeyID is required",
	)

	testInvalidCase(
		"missing key for raw download link",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"",
		&edge.ShareURLOptions{
			Raw: true,
		},
		"uplink: key is required for a raw download link",
	)

	testInvalidCase(
		"prefix instead of object for raw download link",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"myprefix/",
		&edge.ShareURLOptions{
			Raw: true,
		},
		"uplink: a raw download link can not be a prefix",
	)
}
