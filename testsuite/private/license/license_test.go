// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package license_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite/entitlements"
	"storj.io/uplink/private/license"
)

func TestList(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		uplink := planet.Uplinks[0]
		publicID := uplink.Projects[0].PublicID
		apiKey := uplink.APIKey[sat.ID()]

		// Get the user who owns this API key.
		apiKeyInfo, err := sat.DB.Console().APIKeys().GetByHead(ctx, apiKey.Head())
		require.NoError(t, err)
		userID := apiKeyInfo.CreatedBy

		entSvc := sat.API.Entitlements.Service
		now := time.Now().UTC().Truncate(time.Second)

		bucketName := "test-bucket"
		require.NoError(t, planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], bucketName))

		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		expectedKey := testrand.Bytes(32)
		// Set up various licenses for testing.
		licenses := entitlements.AccountLicenses{
			Licenses: []entitlements.AccountLicense{
				{
					// should match all user projects/buckets
					Type:      "pro",
					ExpiresAt: now.Add(30 * 24 * time.Hour),
				},
				{
					// should match only specific bucket
					Type:       "enterprise",
					PublicID:   publicID.String(),
					BucketName: "test-bucket",
					ExpiresAt:  now.Add(60 * 24 * time.Hour),
					Key:        expectedKey,
				},
				{
					// should match all buckets in project
					Type:       "basic",
					PublicID:   publicID.String(),
					BucketName: "",
					ExpiresAt:  now.Add(90 * 24 * time.Hour),
				},
				{
					// should match all user projects/buckets (if not expired)
					Type:      "expired",
					ExpiresAt: now.Add(-24 * time.Hour),
				},
			},
		}

		require.NoError(t, entSvc.Licenses().Set(ctx, userID, licenses))

		t.Run("query all licenses for bucket", func(t *testing.T) {
			result, err := license.List(ctx, project, "", bucketName)
			require.NoError(t, err)
			require.Len(t, result, 3)

			types := []string{}
			for _, l := range result {
				types = append(types, l.Type)
			}
			require.ElementsMatch(t, []string{"pro", "enterprise", "basic"}, types)
		})

		t.Run("query specific license type pro", func(t *testing.T) {
			result, err := license.List(ctx, project, "pro", bucketName)
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.Equal(t, "pro", result[0].Type)
		})

		t.Run("query specific license type enterprise", func(t *testing.T) {
			result, err := license.List(ctx, project, "enterprise", bucketName)
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.Equal(t, license.Entry{
				Type:      "enterprise",
				ExpiresAt: now.Add(60 * 24 * time.Hour),
				Key:       expectedKey,
			}, result[0])
		})

		t.Run("query license for different bucket", func(t *testing.T) {
			otherBucket := "other-bucket"
			require.NoError(t, planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], otherBucket))

			result, err := license.List(ctx, project, "", otherBucket)
			require.NoError(t, err)
			require.Len(t, result, 2)

			types := []string{}
			for _, l := range result {
				types = append(types, l.Type)
			}
			require.ElementsMatch(t, []string{"pro", "basic"}, types)
		})

		t.Run("query expired license type", func(t *testing.T) {
			result, err := license.List(ctx, project, "expired", bucketName)
			require.NoError(t, err)
			require.Empty(t, result)
		})

		t.Run("query non-existent license type", func(t *testing.T) {
			result, err := license.List(ctx, project, "nonexistent", bucketName)
			require.NoError(t, err)
			require.Empty(t, result)
		})
	})
}
