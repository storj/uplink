// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/rpc/rpcpool"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
	"storj.io/uplink/private/object"
	privateProject "storj.io/uplink/private/project"
	"storj.io/uplink/private/transport"
)

func TestRequestAccess(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project := openProject(t, ctx, planet)
		defer ctx.Check(project.Close)

		t.Run("satellite url without id", func(t *testing.T) {
			// try connecting without a proper satellite url
			projectInfo := planet.Uplinks[0].Projects[0]
			_, err := uplink.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.Addr(), projectInfo.APIKey, "mypassphrase")
			require.Error(t, err)
		})
	})
}

func openProject(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) *uplink.Project {
	project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
	require.NoError(t, err)

	return project
}

func TestUserAgent(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 1,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		config := uplink.Config{
			UserAgent: "Zenko",
		}

		satellite, uplink := planet.Satellites[0], planet.Uplinks[0]
		apiKey := uplink.Projects[0].APIKey

		access, err := config.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "mypassphrase")
		require.NoError(t, err)

		project, err := config.OpenProject(ctx, access)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = project.EnsureBucket(ctx, "bucket")
		require.NoError(t, err)

		upload, err := project.UploadObject(ctx, "bucket", "alpha", nil)
		require.NoError(t, err)

		_, err = upload.Write(testrand.Bytes(5 * memory.KiB))
		require.NoError(t, err)
		require.NoError(t, upload.Commit())

		bucketInfo, err := satellite.DB.Buckets().GetBucket(ctx, []byte("bucket"), uplink.Projects[0].ID)
		require.NoError(t, err)
		assert.Contains(t, string(bucketInfo.UserAgent), "Zenko")

		attribution, err := satellite.DB.Attribution().Get(ctx, uplink.Projects[0].ID, []byte("bucket"))
		require.NoError(t, err)
		assert.Contains(t, string(attribution.UserAgent), "Zenko")
	})
}

func TestCustomDialContext(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		{
			config := uplink.Config{
				//lint:ignore SA1019 deprecated okay,
				//nolint:staticcheck // deprecated okay.
				DialContext: badDialContext,
			}

			project, err := config.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			_, err = project.EnsureBucket(ctx, "bucket")
			require.Error(t, err)
		}

		{
			config := uplink.Config{
				//lint:ignore SA1019 deprecated okay,
				//nolint:staticcheck // deprecated okay.
				DialContext: (new(net.Dialer)).DialContext,
			}

			project, err := config.OpenProject(ctx, planet.Uplinks[0].Access[planet.Satellites[0].ID()])
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			_, err = project.EnsureBucket(ctx, "bucket")
			require.NoError(t, err)
		}
	})
}

func badDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return nil, errors.New("dial error")
}

// TestSharedConnectionPoolStaysOpen checks that one-off satellite requests made
// with shared connection pools dial through the pool a project uses for the
// satellite, and leave it open for the rest of its users.
func TestSharedConnectionPoolStaysOpen(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		require.NoError(t, planet.Uplinks[0].Upload(ctx, satellite, "bucket", "object", testrand.BytesInt(100)))

		access := planet.Uplinks[0].Access[satellite.ID()]
		apiKey := planet.Uplinks[0].APIKey[satellite.ID()].Serialize()

		newPool := func() *rpcpool.Pool {
			return rpcpool.New(rpcpool.Options{Capacity: 10, KeyCapacity: 2, IdleExpiration: time.Minute})
		}

		for _, tc := range []struct {
			name          string
			satellitePool bool
		}{
			{name: "shared pool"},
			{name: "separate satellite pool", satellitePool: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var dials atomic.Int64
				config := uplink.Config{
					//lint:ignore SA1019 deprecated okay,
					//nolint:staticcheck // deprecated okay.
					DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
						dials.Add(1)
						return new(net.Dialer).DialContext(ctx, network, address)
					},
				}

				pool := newPool()
				defer ctx.Check(pool.Close)
				require.NoError(t, transport.SetConnectionPool(ctx, &config, pool))

				if tc.satellitePool {
					satellitePool := newPool()
					defer ctx.Check(satellitePool.Close)
					require.NoError(t, transport.SetSatelliteConnectionPool(ctx, &config, satellitePool))
				}

				// Every request returns its satellite connection to the pool
				// when it is done, so as long as they all use the same pool and
				// it stays open, they share one dial.
				_, err := config.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "passphrase")
				require.NoError(t, err)

				_, err = object.GetObjectIPSummary(ctx, config, access, "bucket", "object")
				require.NoError(t, err)

				_, err = privateProject.GetProjectInfo(ctx, config, access)
				require.NoError(t, err)

				project, err := config.OpenProject(ctx, access)
				require.NoError(t, err)
				defer ctx.Check(project.Close)

				_, err = project.StatObject(ctx, "bucket", "object")
				require.NoError(t, err)

				require.EqualValues(t, 1, dials.Load())
			})
		}
	})
}

// TestOneOffRequestsCloseConnections checks that one-off satellite requests
// made without a shared connection pool close the connections they dial.
func TestOneOffRequestsCloseConnections(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		require.NoError(t, planet.Uplinks[0].Upload(ctx, satellite, "bucket", "object", testrand.BytesInt(100)))

		access := planet.Uplinks[0].Access[satellite.ID()]
		apiKey := planet.Uplinks[0].APIKey[satellite.ID()].Serialize()

		var dials, closes atomic.Int64
		config := uplink.Config{
			//lint:ignore SA1019 deprecated okay,
			//nolint:staticcheck // deprecated okay.
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				conn, err := new(net.Dialer).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				dials.Add(1)
				return &closeCountingConn{Conn: conn, closes: &closes}, nil
			},
		}

		for name, request := range map[string]func() error{
			"request access": func() error {
				_, err := config.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "passphrase")
				return err
			},
			"object ip summary": func() error {
				_, err := object.GetObjectIPSummary(ctx, config, access, "bucket", "object")
				return err
			},
			"project info": func() error {
				_, err := privateProject.GetProjectInfo(ctx, config, access)
				return err
			},
		} {
			dialed := dials.Load()
			require.NoError(t, request(), name)
			require.Greater(t, dials.Load(), dialed, name)
			require.Equal(t, dials.Load(), closes.Load(), name)
		}
	})
}

// closeCountingConn counts the first Close of a connection, since TLS can
// close the underlying connection again after the caller does.
type closeCountingConn struct {
	net.Conn
	once   sync.Once
	closes *atomic.Int64
}

func (c *closeCountingConn) Close() error {
	c.once.Do(func() { c.closes.Add(1) })
	return c.Conn.Close()
}
