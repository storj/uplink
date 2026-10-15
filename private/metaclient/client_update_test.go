// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package metaclient

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/pb"
)

func TestUpdateObjectMetadataParamsNilIncludes(t *testing.T) {
	params := UpdateObjectMetadataParams{}
	req := params.toRequest(nil)
	require.Equal(t, &pb.ObjectMetadataIncludes{Custom: true}, req.Includes)
	require.False(t, req.SetEncryptedEtag)
}
