// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package etag_test

import (
	"bytes"
	"crypto/sha256"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testrand"
	"storj.io/uplink/private/etag"
)

func TestHashReader(t *testing.T) {
	inputData := testrand.Bytes(1 * memory.KiB)
	expectedETag := sha256.Sum256(inputData)

	reader := etag.NewHashReader(bytes.NewReader(inputData), sha256.New())
	readData, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, inputData, readData)
	require.Equal(t, expectedETag[:], reader.CurrentETag())
}
