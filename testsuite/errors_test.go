// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
				config.Metainfo.RateLimiter.CacheExpiration = 500 * time.Millisecond
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		project := planet.Uplinks[0].Projects[0]

		// TODO find a way to reset limiter before test is executed, currently
		// testplanet is doing one additional request to get access
		time.Sleep(1 * time.Second)

		err := satellite.DB.Console().Projects().UpdateRateLimit(ctx, project.ID, 1)
		require.NoError(t, err)

		apiKey := project.APIKey
		_, err = uplink.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "mypassphrase")
		assert.NoError(t, err)

		_, err = uplink.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "mypassphrase")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, uplink.ErrTooManyRequests))

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
				config.Console.UsageLimits.Storage.Free = 0
				config.Console.UsageLimits.Bandwidth.Free = 0
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		projectInfo := planet.Uplinks[0].Projects[0]

		// set project limit to 0
		err := satellite.DB.ProjectAccounting().UpdateProjectUsageLimit(ctx, projectInfo.ID, 0)
		require.NoError(t, err)

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

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
		defer ctx.Check(project.Close)

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

func TestBucketNotFoundError(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = project.StatBucket(ctx, "non-existing-bucket")
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		_, err = project.DeleteBucket(ctx, "non-existing-bucket")
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		// TODO this is still not implemented on satellite side
		// _, err = project.StatObject(ctx, "non-existing-bucket", "key")
		// require.True(t, errors.Is(err, uplink.ErrBucketNotFound))
		// _, err = project.DownloadObject(ctx, "non-existing-bucket", "key", nil)
		// require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		upload, err := project.UploadObject(ctx, "non-existing-bucket", "key", nil)
		require.NoError(t, err)

		_, err = io.Copy(upload, bytes.NewBuffer(testrand.Bytes(1*memory.KiB)))
		require.NoError(t, err)

		err = upload.Commit()
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound), err.Error())
	})
}

func TestPermissionDenied(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = project.CreateBucket(ctx, "test-bucket")
		require.NoError(t, err)

		upload, err := project.UploadObject(ctx, "test-bucket", "file", nil)
		require.NoError(t, err)

		randData := testrand.Bytes(1 * memory.KiB)
		source := bytes.NewBuffer(randData)
		_, err = io.Copy(upload, source)
		require.NoError(t, err)

		err = upload.Commit()
		require.NoError(t, err)

		accessRestricted, err := planet.Uplinks[0].Access[planet.Satellites[0].ID()].Share(
			uplink.Permission{
				AllowDownload: true,
			})
		require.NoError(t, err)

		projectRestricted, err := uplink.OpenProject(ctx, accessRestricted)
		require.NoError(t, err)
		defer ctx.Check(projectRestricted.Close)

		it := projectRestricted.ListObjects(ctx, "test-bucket", nil)
		require.False(t, it.Next())
		err = it.Err()
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrPermissionDenied), err.Error())
	})

}
