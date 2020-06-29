// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
)

func TestParallelUploadDownload(t *testing.T) {
	const concurrency = 3

	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: testplanet.MaxSegmentSize(13 * memory.KiB),
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		ctx.Check(project.Close)

		_, err := project.EnsureBucket(ctx, "test")
		require.NoError(t, err)

		// upload multiple objects concurrently
		expectedData := make([][]byte, concurrency)
		for i := range expectedData {
			expectedData[i] = testrand.Bytes(10 * memory.KiB)
		}

		group, gctx := errgroup.WithContext(ctx)
		for p := 0; p < concurrency; p++ {
			p := p
			group.Go(func() error {
				upload, err := project.UploadObject(gctx, "test", strconv.Itoa(p), nil)
				if err != nil {
					return fmt.Errorf("starting upload failed %d: %w", p, err)
				}

				_, err = upload.Write(expectedData[p])
				if err != nil {
					return fmt.Errorf("writing data failed %d: %w", p, err)
				}

				err = upload.Commit()
				if err != nil {
					return fmt.Errorf("committing data failed %d: %w", p, err)
				}

				return nil
			})
		}
		require.NoError(t, group.Wait())

		// download multiple objects concurrently

		group, gctx = errgroup.WithContext(ctx)
		downloadedData := make([][]byte, concurrency)
		for p := 0; p < concurrency; p++ {
			p := p
			group.Go(func() error {
				download, err := project.DownloadObject(gctx, "test", strconv.Itoa(p), nil)
				if err != nil {
					return fmt.Errorf("starting download failed %d: %w", p, err)
				}

				data, err := ioutil.ReadAll(download)
				if err != nil {
					return fmt.Errorf("downloading data failed %d: %w", p, err)
				}

				err = download.Close()
				if err != nil {
					return fmt.Errorf("closing download failed %d: %w", p, err)
				}

				downloadedData[p] = data
				return nil
			})
		}
		require.NoError(t, group.Wait())

		for i, expected := range expectedData {
			require.Equal(t, expected, downloadedData[i])
		}
	})
}
