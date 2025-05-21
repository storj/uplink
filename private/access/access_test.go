// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package access_test

import (
	"context"
	"maps"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/grant"
	"storj.io/common/macaroon"
	"storj.io/common/storj"
	"storj.io/common/testrand"
	"storj.io/uplink"
	"storj.io/uplink/internal/expose"
	privateAccess "storj.io/uplink/private/access"
)

var permissionActionTypes = map[privateAccess.Permission]macaroon.ActionType{
	privateAccess.Download:                         macaroon.ActionRead,
	privateAccess.Upload:                           macaroon.ActionWrite,
	privateAccess.List:                             macaroon.ActionList,
	privateAccess.Delete:                           macaroon.ActionDelete,
	privateAccess.PutObjectRetention:               macaroon.ActionPutObjectRetention,
	privateAccess.GetObjectRetention:               macaroon.ActionGetObjectRetention,
	privateAccess.PutObjectLegalHold:               macaroon.ActionPutObjectLegalHold,
	privateAccess.GetObjectLegalHold:               macaroon.ActionGetObjectLegalHold,
	privateAccess.BypassGovernanceRetention:        macaroon.ActionBypassGovernanceRetention,
	privateAccess.PutBucketObjectLockConfiguration: macaroon.ActionPutBucketObjectLockConfiguration,
	privateAccess.GetBucketObjectLockConfiguration: macaroon.ActionGetBucketObjectLockConfiguration,
}

func TestShare(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	unrestrictedAccess, secret, err := newAccessGrant()
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		bucketName := testrand.BucketName()
		prefix := testrand.Path()
		permissions := []privateAccess.Permission{privateAccess.Download, privateAccess.Upload}
		notAfter := now.Add(time.Minute)
		notBefore := now.Add(-time.Minute)
		expectedMaxObjectTTL := time.Duration(rand.Int63n(int64(time.Hour)))

		access, err := privateAccess.Share(unrestrictedAccess,
			privateAccess.WithPrefix(bucketName, prefix),
			privateAccess.WithPermissions(permissions...),
			privateAccess.NotAfter(notAfter),
			privateAccess.NotBefore(notBefore),
			privateAccess.WithMaxObjectTTL(expectedMaxObjectTTL),
		)
		require.NoError(t, err)

		apiKey := expose.AccessGetAPIKey(access)

		t.Run("Prefix", func(t *testing.T) {
			err := apiKey.Check(ctx, secret, macaroon.APIKeyVersionLatest, macaroon.Action{
				Op:            permissionActionTypes[permissions[0]],
				Bucket:        []byte(testrand.BucketName()),
				EncryptedPath: []byte(testrand.Path()),
				Time:          now,
			}, nil)
			require.ErrorIs(t, err, macaroon.ErrUnauthorized.Instance())
		})

		allowedAction := macaroon.Action{
			Op:            permissionActionTypes[permissions[0]],
			Bucket:        []byte(bucketName),
			EncryptedPath: []byte(prefix + "/object"),
			Time:          now,
		}

		t.Run("Permissions", func(t *testing.T) {
			disallowedActionTypes := cloneWithoutKeys(permissionActionTypes, permissions)

			action := allowedAction
			for perm, actionType := range disallowedActionTypes {
				action.Op = actionType
				err := apiKey.Check(ctx, secret, macaroon.APIKeyVersionLatest, action, nil)
				assert.ErrorIs(t, err, macaroon.ErrUnauthorized.Instance(), "expected permission %q to not be granted", perm)
			}

			for _, perm := range permissions {
				action.Op = permissionActionTypes[perm]
				err := apiKey.Check(ctx, secret, macaroon.APIKeyVersionLatest, action, nil)
				assert.NoError(t, err, "expected permission %q to be granted", perm)
			}
		})

		t.Run("All permissions", func(t *testing.T) {
			access, err := privateAccess.Share(unrestrictedAccess,
				privateAccess.WithPrefix(bucketName, prefix),
				privateAccess.WithAllPermissions(),
				privateAccess.NotAfter(notAfter),
				privateAccess.NotBefore(notBefore),
				privateAccess.WithMaxObjectTTL(expectedMaxObjectTTL),
			)
			require.NoError(t, err)

			apiKey := expose.AccessGetAPIKey(access)

			action := allowedAction
			for perm, actionType := range permissionActionTypes {
				action.Op = actionType
				err := apiKey.Check(ctx, secret, macaroon.APIKeyVersionLatest, action, nil)
				assert.NoError(t, err, "expected permission %q to be granted", perm)
			}
		})

		t.Run("NotAfter", func(t *testing.T) {
			action := allowedAction
			action.Time = notAfter.Add(time.Second)
			err := apiKey.Check(ctx, secret, macaroon.APIKeyVersionLatest, action, nil)
			require.ErrorIs(t, err, macaroon.ErrUnauthorized.Instance())
		})

		t.Run("NotBefore", func(t *testing.T) {
			action := allowedAction
			action.Time = notBefore.Add(-time.Second)
			err := apiKey.Check(ctx, secret, macaroon.APIKeyVersionLatest, action, nil)
			require.ErrorIs(t, err, macaroon.ErrUnauthorized.Instance())
		})

		t.Run("MaxObjectTTL", func(t *testing.T) {
			maxObjectTTL, err := apiKey.GetMaxObjectTTL(ctx)
			require.NoError(t, err)
			require.NotNil(t, maxObjectTTL)
			require.Equal(t, expectedMaxObjectTTL, *maxObjectTTL)
		})
	})

	t.Run("Invalid permission", func(t *testing.T) {
		_, err := privateAccess.Share(unrestrictedAccess, privateAccess.WithPermissions(12345))
		require.ErrorIs(t, err, privateAccess.ErrPermissionInvalid.Instance())
	})
}

func newAccessGrant() (access *uplink.Access, secret []byte, err error) {
	secret, err = macaroon.NewSecret()
	if err != nil {
		return nil, nil, errs.Wrap(err)
	}

	apiKey, err := macaroon.NewAPIKey(secret)
	if err != nil {
		return nil, nil, errs.Wrap(err)
	}

	encAccess := grant.NewEncryptionAccessWithDefaultKey(&storj.Key{})
	encAccess.SetDefaultPathCipher(storj.EncNull)
	encAccess.LimitTo(apiKey)

	grantAccess := grant.Access{
		SatelliteAddress: storj.NodeURL{ID: testrand.NodeID()}.String(),
		APIKey:           apiKey,
		EncAccess:        encAccess,
	}
	serialized, err := grantAccess.Serialize()
	if err != nil {
		return nil, nil, errs.Wrap(err)
	}

	access, err = uplink.ParseAccess(serialized)
	if err != nil {
		return nil, nil, errs.Wrap(err)
	}

	return access, secret, nil
}

func cloneWithoutKeys[K comparable, V any](base map[K]V, keysToRemove []K) map[K]V {
	result := make(map[K]V, len(base))
	maps.Copy(result, base)
	for _, k := range keysToRemove {
		delete(result, k)
	}
	return result
}
