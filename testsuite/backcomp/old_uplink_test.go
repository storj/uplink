// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package backcomp_test

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/pb"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
	"storj.io/uplink/private/piecestore"
)

const storjrelease = "v1.0.0" // uses storj.io/uplink v1.0.0-rc.5.0.20200311190324-aee82d3f05aa

func TestOldUplink(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		// TODO add different kinds of files: inline, multi segment, multipart

		cmd := exec.Command("go", "install", "storj.io/storj/cmd/uplink@"+storjrelease)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, "GOBIN="+ctx.Dir("binary"))
		output, err := cmd.CombinedOutput()
		t.Log(string(output))
		require.NoError(t, err)

		err = planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], "bucket")
		require.NoError(t, err)

		oldExpectedData := testrand.Bytes(5 * memory.KiB)
		newExpectedData := testrand.Bytes(5 * memory.KiB)
		srcOldFile := ctx.File("src-old")
		dstOldFile := ctx.File("dst-old")
		dstNewFile := ctx.File("dst-new")

		err = os.WriteFile(srcOldFile, oldExpectedData, 0644)
		require.NoError(t, err)

		access, err := planet.Uplinks[0].Access[planet.Satellites[0].ID()].Serialize()
		require.NoError(t, err)

		runBinary := func(args ...string) string {
			output, err := exec.Command(ctx.File("binary", "uplink"), args...).CombinedOutput()
			t.Log(string(output))
			require.NoError(t, err)
			return string(output)
		}

		// upload with old uplink
		runBinary("cp", srcOldFile, "sj://bucket/old-uplink", "--access="+access)

		// upload with new uplink (using SHA-256 piece hash algorithm)
		err = planet.Uplinks[0].Upload(piecestore.WithPieceHashAlgo(ctx, pb.PieceHashAlgorithm_SHA256), planet.Satellites[0], "bucket", "new-uplink-sha256", newExpectedData)
		require.NoError(t, err)

		// upload with new uplink (using BLAKE3 piece hash algorithm)
		err = planet.Uplinks[0].Upload(piecestore.WithPieceHashAlgo(ctx, pb.PieceHashAlgorithm_BLAKE3), planet.Satellites[0], "bucket", "new-uplink-blake3", newExpectedData)
		require.NoError(t, err)

		// uploaded with old uplink and downloaded with old uplink
		runBinary("cp", "sj://bucket/old-uplink", dstOldFile, "--access="+access)

		oldData, err := os.ReadFile(dstOldFile)
		require.NoError(t, err)
		require.Equal(t, oldExpectedData, oldData)

		// uploaded with old uplink and downloaded with latest uplink
		data, err := planet.Uplinks[0].Download(ctx, planet.Satellites[0], "bucket", "old-uplink")
		require.NoError(t, err)
		require.Equal(t, oldExpectedData, data)

		// uploaded with new uplink and downloaded with old uplink (sha256)
		runBinary("cp", "sj://bucket/new-uplink-sha256", dstNewFile, "--access="+access)
		newData, err := os.ReadFile(dstNewFile)
		require.NoError(t, err)
		require.Equal(t, newExpectedData, newData)

		// uploaded with new uplink and downloaded with old uplink (blake3)
		runBinary("cp", "sj://bucket/new-uplink-blake3", dstNewFile, "--access="+access)
		newData, err = os.ReadFile(dstNewFile)
		require.NoError(t, err)
		require.Equal(t, newExpectedData, newData)

		cmdResult := runBinary("ls", "sj://bucket/", "--access", access)
		require.Contains(t, cmdResult, "5120 old-uplink")
		require.Contains(t, cmdResult, "5120 new-uplink")
	})
}

func TestMove(t *testing.T) {
	if _, err := exec.LookPath("go1.17.13"); err != nil {
		// uplink@v1.40.4 reuqires an older compiler due to quic dependency.
		t.Fatalf("missing suitable compiler go1.17.13: %v", err)
	}

	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		// old upling is uploading object and moving it
		// new uplink should be able to list it

		cmd := exec.Command("go1.17.13", "install", "storj.io/storj/cmd/uplink@v1.40.4")
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, "GOBIN="+ctx.Dir("binary"))
		output, err := cmd.CombinedOutput()
		t.Log(string(output))
		require.NoError(t, err)

		err = planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], "bucket")
		require.NoError(t, err)

		expectedData := testrand.Bytes(1 * memory.KiB)
		srcFile := ctx.File("src")

		err = os.WriteFile(srcFile, expectedData, 0644)
		require.NoError(t, err)

		access, err := planet.Uplinks[0].Access[planet.Satellites[0].ID()].Serialize()
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		runBinary := func(args ...string) {
			output, err = exec.Command(ctx.File("binary", "uplink"), args...).CombinedOutput()
			t.Log(string(output))
			require.NoError(t, err)
		}

		// upload with old uplink
		runBinary("cp", srcFile, "sj://bucket/move/old-uplink", "--access="+access)

		// move with old uplink
		runBinary("mv", "sj://bucket/move/old-uplink", "sj://bucket/move/old-uplink-moved", "--access="+access)

		testit := func(key string) {
			cases := []uplink.ListObjectsOptions{
				{System: false, Custom: false},
				{System: true, Custom: false},
				{System: false, Custom: true},
				{System: true, Custom: true},
			}

			for _, tc := range cases {
				tc.Prefix = "move/"
				iterator := project.ListObjects(ctx, "bucket", &tc)
				require.True(t, iterator.Next())
				require.Equal(t, key, iterator.Item().Key)
				require.NoError(t, iterator.Err())
			}
		}

		testit("move/old-uplink-moved")

		// move with old uplink second time
		runBinary("mv", "sj://bucket/move/old-uplink-moved", "sj://bucket/move/old-uplink-moved-second", "--access="+access)

		testit("move/old-uplink-moved-second")
	})
}
