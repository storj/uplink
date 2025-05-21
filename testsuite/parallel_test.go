// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"storj.io/common/errs2"
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
		defer ctx.Check(project.Close)

		_, err := project.EnsureBucket(ctx, "test")
		require.NoError(t, err)

		// upload multiple objects concurrently
		expectedData := make([][]byte, concurrency)
		for i := range expectedData {
			expectedData[i] = testrand.Bytes(10 * memory.KiB)
		}

		group := errs2.Group{}
		for p := range concurrency {
			group.Go(func() error {
				upload, err := project.UploadObject(ctx, "test", strconv.Itoa(p), nil)
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
		require.Empty(t, group.Wait())

		// download multiple objects concurrently

		group = errs2.Group{}
		downloadedData := make([][]byte, concurrency)
		for p := range concurrency {
			group.Go(func() error {
				download, err := project.DownloadObject(ctx, "test", strconv.Itoa(p), nil)
				if err != nil {
					return fmt.Errorf("starting download failed %d: %w", p, err)
				}

				data, err := io.ReadAll(download)
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
		require.Empty(t, group.Wait())

		for i, expected := range expectedData {
			require.Equal(t, expected, downloadedData[i])
		}
	})
}

func TestUplinksParallel(t *testing.T) {
	const uplinkCount = 2
	const parallelCount = 2

	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 6, UplinkCount: uplinkCount,
		Reconfigure: testplanet.DisableQUIC,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]

		var group errgroup.Group
		for i := range planet.Uplinks {
			uplink := planet.Uplinks[i]

			for p := range parallelCount {
				suffix := fmt.Sprintf("-%d-%d", i, p)
				group.Go(func() error {
					data := testrand.Bytes(memory.Size(100+testrand.Intn(500)) * memory.KiB)

					err := uplink.Upload(ctx, satellite, "testbucket"+suffix, "test/path"+suffix, data)
					if err != nil {
						return err
					}

					downloaded, err := uplink.Download(ctx, satellite, "testbucket"+suffix, "test/path"+suffix)
					if err != nil {
						return err
					}

					if !bytes.Equal(data, downloaded) {
						return fmt.Errorf("upload != download data: %s", suffix)
					}

					return nil
				})
			}
		}
		err := group.Wait()
		require.NoError(t, err)
	})
}
