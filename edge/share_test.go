// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package edge_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/uplink/edge"
)

func TestCreateURL(t *testing.T) {
	testValidCase := func(description, baseURL, accessID, bucket, key string, expected string) {
		t.Run(description, func(t *testing.T) {
			url, err := edge.JoinShareURL(baseURL, accessID, bucket, key)
			require.Nil(t, err)
			require.Equal(t, expected, url)
		})
	}

	testInvalidCase := func(description, baseURL, accessID, bucket, key string, expectedErr string) {
		t.Run(description, func(t *testing.T) {
			url, err := edge.JoinShareURL(baseURL, accessID, bucket, key)
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
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)

	testValidCase(
		"share entire bucket",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"",
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa/mybucket",
	)

	testValidCase(
		"share entire prefix",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"my/prefix/",
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa/mybucket/my/prefix/",
	)

	testValidCase(
		"share object nested in prefix",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"my/object",
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa/mybucket/my/object",
	)

	testValidCase(
		"base url ends in slash",
		"https://linksharing.test/",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		"",
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)

	testValidCase(
		"escaping of binary object key",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"mybucket",
		"a\x00a",
		"https://linksharing.test/s/aaaaaaaaaaaaaaaaaaaaaaaaaaaa/mybucket/a%00a",
	)

	testInvalidCase(
		"base url missing",
		"",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		"",
		"uplink: invalid base url: \"\"",
	)

	testInvalidCase(
		"base url has no protocol",
		"linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		"",
		"uplink: invalid base url: \"linksharing.test\"",
	)

	testInvalidCase(
		"bucket missing",
		"https://linksharing.test",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"",
		"myobject",
		"uplink: bucket is required if key is specified",
	)

	testInvalidCase(
		"access key ID missing",
		"https://linksharing.test",
		"",
		"",
		"",
		"uplink: accessKeyID is required",
	)
}
