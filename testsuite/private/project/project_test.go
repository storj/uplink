// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package project_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/macaroon"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
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
