// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/errs2"
	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
)

func TestParallelUploadDownload(t *testing.T) {
	t.Skip("drpc does not handle concurrent method invocations")
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
		for p := 0; p < concurrency; p++ {
			p := p
			group.Go(func() error {
				upload, err := project.UploadObject(ctx, "test", strconv.Itoa(p), nil)
				if err != nil {
					return fmt.Errorf("starting upload failed %d: %+v", p, err)
				}

				_, err = upload.Write(expectedData[p])
				if err != nil {
					return fmt.Errorf("writing data failed %d: %+v", p, err)
				}

				err = upload.Commit()
				if err != nil {
					return fmt.Errorf("committing data failed %d: %+v", p, err)
				}

				return nil
			})
		}
		require.Empty(t, group.Wait())

		// download multiple objects concurrently

		group = errs2.Group{}
		downloadedData := make([][]byte, concurrency)
		for p := 0; p < concurrency; p++ {
			p := p
			group.Go(func() error {
				download, err := project.DownloadObject(ctx, "test", strconv.Itoa(p), nil)
				if err != nil {
					return fmt.Errorf("starting download failed %d: %+v", p, err)
				}

				data, err := ioutil.ReadAll(download)
				if err != nil {
					return fmt.Errorf("downloading data failed %d: %+v", p, err)
				}

				err = download.Close()
				if err != nil {
					return fmt.Errorf("closing download failed %d: %+v", p, err)
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
