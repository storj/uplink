// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package project_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"storj.io/common/macaroon"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/console"
	"storj.io/uplink"
	privateProject "storj.io/uplink/private/project"
)

func TestGetPublicID(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]

		user, err := satellite.AddUser(ctx, console.CreateUser{
			FullName: "test user",
			Email:    "test-email@test",
			Password: "password",
		}, 4)
		require.NoError(t, err)

		project, err := satellite.AddProject(ctx, user.ID, "test project")
		require.NoError(t, err)

		secret, err := macaroon.NewSecret()
		require.NoError(t, err)

		apiKey, err := macaroon.NewAPIKey(secret)
		require.NoError(t, err)

		_, err = satellite.DB.Console().APIKeys().Create(ctx, apiKey.Head(), console.APIKeyInfo{
			Name:      "testkey",
			ProjectID: project.ID,
			Secret:    secret,
		})
		require.NoError(t, err)

		access, err := uplink.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey.Serialize(), "test")
		require.NoError(t, err)

		id, err := privateProject.GetPublicID(ctx, uplink.Config{}, access)
		require.NoError(t, err)

		require.Equal(t, project.PublicID, id)
	})
}

func TestGetProjectInfo(t *testing.T) {
	var (
		authUrl             = "auth.storj.io"
		publicLinksharing   = "public-link.storj.io"
		internalLinksharing = "link.storj.io"
	)

	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.SendEdgeUrlOverrides = true
				err := config.Console.PlacementEdgeURLOverrides.Set(
					fmt.Sprintf(`{
						"1": {
							"authService": "%s",
							"publicLinksharing": "%s",
							"internalLinksharing": "%s"
						}
					}`, authUrl, publicLinksharing, internalLinksharing),
				)
				require.NoError(t, err)
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]

		user, err := sat.AddUser(ctx, console.CreateUser{
			FullName: "test user", Email: "test-email@test", Password: "password",
		}, 4)
		require.NoError(t, err)

		project, err := sat.AddProject(ctx, user.ID, "test project")
		require.NoError(t, err)

		secret, err := macaroon.NewSecret()
		require.NoError(t, err)

		apiKey, err := macaroon.NewAPIKey(secret)
		require.NoError(t, err)

		_, err = sat.DB.Console().APIKeys().Create(ctx, apiKey.Head(), console.APIKeyInfo{
			Name: "testkey", ProjectID: project.ID, Secret: secret,
		})
		require.NoError(t, err)

		access, err := uplink.RequestAccessWithPassphrase(ctx, sat.URL(), apiKey.Serialize(), "test")
		require.NoError(t, err)

		info, err := privateProject.GetProjectInfo(ctx, uplink.Config{}, access)
		require.NoError(t, err)
		require.NotNil(t, info.Salt)
		require.Equal(t, project.PublicID, info.PublicId)
		require.WithinDuration(t, project.CreatedAt, info.CreatedAt, time.Nanosecond)
		require.Nil(t, info.EdgeUrlOverrides)

		err = sat.API.DB.Console().Projects().UpdateDefaultPlacement(ctx, project.ID, storj.PlacementConstraint(1))
		require.NoError(t, err)

		info, err = privateProject.GetProjectInfo(ctx, uplink.Config{}, access)
		require.NoError(t, err)
		require.NotNil(t, info.EdgeUrlOverrides)
		require.Equal(t, authUrl, info.EdgeUrlOverrides.AuthService)
		require.Equal(t, publicLinksharing, info.EdgeUrlOverrides.PublicLinksharing)
		require.Equal(t, internalLinksharing, info.EdgeUrlOverrides.InternalLinksharing)
	})
}
