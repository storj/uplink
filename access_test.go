// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/grant"
	"storj.io/common/macaroon"
	"storj.io/common/storj"
	"storj.io/uplink"
	privateAccess "storj.io/uplink/private/access"
)

func TestRequestAccessWithPassphraseAndConcurrency_KnownAddress(t *testing.T) {
	const apiKey = "13Yqe3oHi5dcnGhMu2ru3cmePC9iEYv6nDrYMbLRh4wre1KtVA9SFwLNAuuvWwc43b9swRsrfsnrbuTHQ6TJKVt4LjGnaARN9PhxJEu"

	ctx := context.Background()
	sentinelError := errs.Class("test sentinel")
	knownAddresses := []string{
		"us-central-1.tardigrade.io:7777",
		"mars.tardigrade.io:7777",
		"asia-east-1.tardigrade.io:7777",
		"saturn.tardigrade.io:7777",
		"europe-west-1.tardigrade.io:7777",
		"jupiter.tardigrade.io:7777",
		"satellite.stefan-benten.de:7777",
		"saltlake.tardigrade.io:7777",
	}

	{ // check that known addresses gain ids and make it to dialing
		config := uplink.Config{
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				return nil, sentinelError.New("%s", address)
			},
		}

		for _, address := range knownAddresses {
			_, err := config.RequestAccessWithPassphrase(ctx, address, apiKey, "password")
			require.Error(t, err)
			require.Contains(t, err.Error(), address)
			require.True(t, sentinelError.Has(err))
		}
	}

	{ // check that unknown addresses do not gain an id
		config := uplink.Config{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				t.Fail()
				return nil, errs.New("should not be called")
			},
		}

		_, err := config.RequestAccessWithPassphrase(ctx, "someaddr.example:7777", apiKey, "password")
		require.Error(t, err)
		require.Contains(t, err.Error(), "node id is required")
	}
}

func TestShareObjectLockPermissionMapping(t *testing.T) {
	access := newTestAccess(t)

	// each entry sets a single Permission field and names the caveat flag that
	// must be cleared by it; every other Object Lock flag must stay set.
	for _, tt := range []struct {
		name  string
		set   func(*uplink.Permission)
		allow func(*macaroon.Caveat) bool
	}{
		{"PutObjectRetention", func(p *uplink.Permission) { p.AllowPutObjectRetention = true },
			func(c *macaroon.Caveat) bool { return !c.DisallowPutRetention }},
		{"GetObjectRetention", func(p *uplink.Permission) { p.AllowGetObjectRetention = true },
			func(c *macaroon.Caveat) bool { return !c.DisallowGetRetention }},
		{"PutObjectLegalHold", func(p *uplink.Permission) { p.AllowPutObjectLegalHold = true },
			func(c *macaroon.Caveat) bool { return !c.DisallowPutLegalHold }},
		{"GetObjectLegalHold", func(p *uplink.Permission) { p.AllowGetObjectLegalHold = true },
			func(c *macaroon.Caveat) bool { return !c.DisallowGetLegalHold }},
		{"BypassGovernanceRetention", func(p *uplink.Permission) { p.AllowBypassGovernanceRetention = true },
			func(c *macaroon.Caveat) bool { return !c.DisallowBypassGovernanceRetention }},
		{"PutBucketObjectLockConfiguration", func(p *uplink.Permission) {
			p.AllowPutBucketObjectLockConfiguration = true
		}, func(c *macaroon.Caveat) bool { return !c.DisallowPutBucketObjectLockConfiguration }},
		{"GetBucketObjectLockConfiguration", func(p *uplink.Permission) {
			p.AllowGetBucketObjectLockConfiguration = true
		}, func(c *macaroon.Caveat) bool { return !c.DisallowGetBucketObjectLockConfiguration }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			permission := uplink.Permission{}
			tt.set(&permission)

			shared, err := access.Share(permission)
			require.NoError(t, err)
			caveat := lastCaveat(t, privateAccess.APIKey(shared))

			require.True(t, tt.allow(caveat), "%s did not grant its own capability", tt.name)
			// exactly one flag may be cleared, all the others must stay disallowed.
			cleared := 0
			for _, allow := range []bool{
				!caveat.DisallowPutRetention,
				!caveat.DisallowGetRetention,
				!caveat.DisallowPutLegalHold,
				!caveat.DisallowGetLegalHold,
				!caveat.DisallowBypassGovernanceRetention,
				!caveat.DisallowPutBucketObjectLockConfiguration,
				!caveat.DisallowGetBucketObjectLockConfiguration,
			} {
				if allow {
					cleared++
				}
			}
			require.Equal(t, 1, cleared, "%s granted more than its own capability", tt.name)
		})
	}
}

func TestFullPermissionGrantsAllObjectLockPermissions(t *testing.T) {
	access := newTestAccess(t)

	shared, err := access.Share(uplink.FullPermission())
	require.NoError(t, err)
	caveat := lastCaveat(t, privateAccess.APIKey(shared))

	require.False(t, caveat.DisallowPutRetention)
	require.False(t, caveat.DisallowGetRetention)
	require.False(t, caveat.DisallowPutLegalHold)
	require.False(t, caveat.DisallowGetLegalHold)
	require.False(t, caveat.DisallowBypassGovernanceRetention)
	require.False(t, caveat.DisallowPutBucketObjectLockConfiguration)
	require.False(t, caveat.DisallowGetBucketObjectLockConfiguration)

	// the deprecated coarse permission is deliberately not granted.
	require.True(t, caveat.DisallowLocks)
}

// TestShareDeprecatedAllowLock checks that the deprecated coarse AllowLock is
// still honored, by mapping it onto the granular permissions it covered.
func TestShareDeprecatedAllowLock(t *testing.T) {
	access := newTestAccess(t)

	shared, err := access.Share(uplink.Permission{AllowLock: true}) //nolint:staticcheck // the deprecated field is what is under test
	require.NoError(t, err)
	caveat := lastCaveat(t, privateAccess.APIKey(shared))

	require.False(t, caveat.DisallowPutRetention)
	require.False(t, caveat.DisallowGetRetention)
	require.False(t, caveat.DisallowPutBucketObjectLockConfiguration)
	require.False(t, caveat.DisallowGetBucketObjectLockConfiguration)

	require.True(t, caveat.DisallowPutLegalHold)
	require.True(t, caveat.DisallowGetLegalHold)
	require.True(t, caveat.DisallowBypassGovernanceRetention)

	// the deprecated coarse permission itself is deliberately not granted,
	// no supported satellite acts on it.
	require.True(t, caveat.DisallowLocks)
}

func lastCaveat(t *testing.T, apiKey *macaroon.APIKey) *macaroon.Caveat {
	mac, err := macaroon.ParseMacaroon(apiKey.SerializeRaw())
	require.NoError(t, err)
	caveats := mac.Caveats()
	require.NotEmpty(t, caveats)

	caveat, err := macaroon.ParseCaveat(caveats[len(caveats)-1])
	require.NoError(t, err)
	return caveat
}

func newTestAccess(t *testing.T) *uplink.Access {
	secret, err := macaroon.NewSecret()
	require.NoError(t, err)
	apiKey, err := macaroon.NewAPIKey(secret)
	require.NoError(t, err)

	serialized, err := (&grant.Access{
		SatelliteAddress: "12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S@satellite.example:7777",
		APIKey:           apiKey,
		EncAccess:        grant.NewEncryptionAccessWithDefaultKey(&storj.Key{}),
	}).Serialize()
	require.NoError(t, err)

	access, err := uplink.ParseAccess(serialized)
	require.NoError(t, err)
	return access
}
