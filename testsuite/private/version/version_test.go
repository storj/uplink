// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package version_test

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
)

func TestAppendVersionToUserAgent(t *testing.T) {
	ctx := testcontext.New(t)

	{
		cmd := exec.CommandContext(ctx, "go", "run", "testbuild.go", "")

		data, err := cmd.CombinedOutput()
		require.NoError(t, err)

		require.Contains(t, string(data), "uplink/")
		require.Greater(t, len(data), len("uplink/"))
	}

	{
		cmd := exec.CommandContext(ctx, "go", "run", "testbuild.go", "zenko")

		data, err := cmd.CombinedOutput()
		require.NoError(t, err)

		require.Contains(t, string(data), "zenko uplink/")
		require.Greater(t, len(data), len("zenko uplink/"))
	}
}
