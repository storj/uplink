// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/uplink"
)

func TestErrRateLimitExceeded(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.RateLimiter.Rate = 0
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]

		apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
		_, err := uplink.RequestAccessWithPassphrase(ctx, satellite.URL().String(), apiKey.Serialize(), "mypassphrase")
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrTooManyRequests))

		// TODO add check for other methods but currently we are not able to manipulate
		// rate limit when test planet is started
	})
}

func TestErrResourceExhausted(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Rollup.MaxAlphaUsage = 0
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]

		// set project limit to 0
		allProjects, err := satellite.DB.Console().Projects().GetAll(ctx)
		require.NoError(t, err)
		err = satellite.DB.ProjectAccounting().UpdateProjectUsageLimit(ctx, allProjects[0].ID, 0)
		require.NoError(t, err)

		apiKey := planet.Uplinks[0].APIKey[satellite.ID()]
		access, err := uplink.RequestAccessWithPassphrase(ctx, satellite.URL().String(), apiKey.Serialize(), "mypassphrase")
		require.NoError(t, err)

		project, err := uplink.OpenProject(ctx, access)
		require.NoError(t, err)

		_, err = project.CreateBucket(ctx, "test-bucket")
		require.NoError(t, err)

		upload, err := project.UploadObject(ctx, "test-bucket", "file", nil)
		require.NoError(t, err)

		randData := testrand.Bytes(1 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)

		err = upload.Commit()
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBandwidthLimitExceeded))
	})
}

func TestUploadDownloadParamsValidation(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		ctx.Check(project.Close)

		_, err := project.UploadObject(ctx, "", "", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

		_, err = project.UploadObject(ctx, "testbucket", "", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))

		_, err = project.DownloadObject(ctx, "", "", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNameInvalid))

		_, err = project.DownloadObject(ctx, "testbucket", "", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrObjectKeyInvalid))
	})
}
