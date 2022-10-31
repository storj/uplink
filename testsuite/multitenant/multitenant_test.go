// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package multitenant_test

import (
	"bytes"
	"crypto/sha256"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func TestMultiTenant(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		const (
			userID     = "user@example.com"
			passphrase = "abc123"
			appBucket  = "appbucket"
			fileName   = "test.dat"
		)

		fileData := testrand.Bytes(1 * memory.KiB)

		{ // authenticate and upload a file
			serializedUserAccess, userSalt, err := authenticate(ctx, planet, userID, appBucket)
			require.NoError(t, err)

			userAccess, err := addPassphraseToAccess(serializedUserAccess, userID, passphrase, appBucket, userSalt)
			require.NoError(t, err)

			err = uploadFile(ctx, userAccess, userID, appBucket, fileName, fileData)
			require.NoError(t, err)
		}

		{ // authenticate again to request new access grant and download the file
			serializedUserAccess, userSalt, err := authenticate(ctx, planet, userID, appBucket)
			require.NoError(t, err)

			userAccess, err := addPassphraseToAccess(serializedUserAccess, userID, passphrase, appBucket, userSalt)
			require.NoError(t, err)

			downloaded, err := downloadFile(ctx, userAccess, userID, appBucket, fileName)
			require.NoError(t, err)
			require.Equal(t, fileData, downloaded)
		}

		{ // authenticate the same user, but use a different passphrase to download the file
			serializedUserAccess, userSalt, err := authenticate(ctx, planet, userID, appBucket)
			require.NoError(t, err)

			userAccess, err := addPassphraseToAccess(serializedUserAccess, userID, "fakepassphrase", appBucket, userSalt)
			require.NoError(t, err)

			_, err = downloadFile(ctx, userAccess, userID, appBucket, fileName)
			require.Error(t, err)
		}

		{ // authenticate another user and check it cannot download the file uploaded from the first user
			cheaterID := "cheater@example.com"
			serializedUserAccess, userSalt, err := authenticate(ctx, planet, cheaterID, appBucket)
			require.NoError(t, err)

			userAccess, err := addPassphraseToAccess(serializedUserAccess, cheaterID, passphrase, appBucket, userSalt)
			require.NoError(t, err)

			_, err = downloadFile(ctx, userAccess, userID, appBucket, fileName)
			require.Error(t, err)
		}

		{ // ensure authenticated users cannot list the root of the app bucket
			serializedUserAccess, userSalt, err := authenticate(ctx, planet, userID, appBucket)
			require.NoError(t, err)

			userAccess, err := addPassphraseToAccess(serializedUserAccess, userID, passphrase, appBucket, userSalt)
			require.NoError(t, err)

			project, err := uplink.OpenProject(ctx, userAccess)
			require.NoError(t, err)
			defer ctx.Check(project.Close)

			list := project.ListObjects(ctx, appBucket, nil)
			require.False(t, list.Next())
			require.Error(t, list.Err())
		}
	})
}

func authenticate(ctx *testcontext.Context, planet *testplanet.Planet, userID, appBucket string) (serializedUserAccess string, userSalt []byte, err error) {
	satellite := planet.Satellites[0]
	apiKey := planet.Uplinks[0].APIKey[satellite.ID()].Serialize()
	rootAccess, err := uplink.RequestAccessWithPassphrase(ctx, satellite.URL(), apiKey, "")
	if err != nil {
		return "", nil, err
	}

	project, err := uplink.OpenProject(ctx, rootAccess)
	if err != nil {
		return "", nil, err
	}
	defer ctx.Check(project.Close)

	_, err = project.EnsureBucket(ctx, appBucket)
	if err != nil {
		return "", nil, err
	}

	now := time.Now()
	permission := uplink.FullPermission()
	permission.NotBefore = now
	permission.NotAfter = now.Add(8 * time.Hour)

	userAccess, err := rootAccess.Share(
		permission,
		uplink.SharePrefix{
			Bucket: appBucket,
			Prefix: userID + "/",
		},
	)
	if err != nil {
		return "", nil, err
	}

	serializedUserAccess, err = userAccess.Serialize()
	if err != nil {
		return "", nil, err
	}

	userSalt = saltForUser(userID)

	return serializedUserAccess, userSalt, nil
}

func saltForUser(userID string) []byte {
	// This is just an example for creating a salt for user.
	// The actual implementation on the authentication server
	// may (and perhaps should) differ.
	salt := sha256.Sum256([]byte(userID))
	return salt[:]
}

func addPassphraseToAccess(serializedUserAccess, userID, passphrase, appBucket string, userSalt []byte) (userAccess *uplink.Access, err error) {
	userAccess, err = uplink.ParseAccess(serializedUserAccess)
	if err != nil {
		return nil, err
	}

	saltedUserKey, err := uplink.DeriveEncryptionKey(passphrase, userSalt)
	if err != nil {
		return nil, err
	}

	err = userAccess.OverrideEncryptionKey(appBucket, userID+"/", saltedUserKey)
	if err != nil {
		return nil, err
	}

	return userAccess, nil
}

func uploadFile(ctx *testcontext.Context, userAccess *uplink.Access, userID, appBucket, fileName string, fileData []byte) error {
	project, err := uplink.OpenProject(ctx, userAccess)
	if err != nil {
		return err
	}
	defer ctx.Check(project.Close)

	upload, err := project.UploadObject(ctx, appBucket, userID+"/"+fileName, nil)
	if err != nil {
		return err
	}

	source := bytes.NewBuffer(fileData)
	_, err = io.Copy(upload, source)
	if err != nil {
		return err
	}

	return upload.Commit()
}

func downloadFile(ctx *testcontext.Context, userAccess *uplink.Access, userID, appBucket, fileName string) (downloaded []byte, err error) {
	project, err := uplink.OpenProject(ctx, userAccess)
	if err != nil {
		return nil, err
	}
	defer ctx.Check(project.Close)

	download, err := project.DownloadObject(ctx, appBucket, userID+"/"+fileName, nil)
	if err != nil {
		return nil, err
	}
	defer ctx.Check(download.Close)

	return io.ReadAll(download)
}
