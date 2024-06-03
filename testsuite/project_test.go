// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package testsuite_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

const testAccess = "1HYC799Xw6VsvPF1DnbQieSqTNtfousmMUb34uef5ZJRkW3NZSRLYFmb2SvRh7GJD4BF7T3ZmKeGRJF1ZsCrJxWwKz1EsrbemiXRywbHZ6AkYfvgpYbeFbhVeW1vndMYNe54GZYEMjZAihFkg27tQFCrRSYm2powghLTm4RqxucgP9KpgHzi6zmAWNWMx59TKCPRCY7o2r2H"

func TestProject_OpenProjectDialFail(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		projectInfo := planet.Uplinks[0].Projects[0]
		access, err := uplink.RequestAccessWithPassphrase(ctx, projectInfo.Satellite.URL(), projectInfo.APIKey, "mypassphrase")
		require.NoError(t, err)

		err = planet.StopPeer(planet.Satellites[0])
		require.NoError(t, err)

		project, err := uplink.OpenProject(ctx, access)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = project.EnsureBucket(ctx, "bucket")
		require.Error(t, err)
	})
}

func TestProject_OpenProjectMalformedUserAgent(t *testing.T) {
	config := uplink.Config{
		UserAgent: "Invalid (darwin; amd64) invalid/v7.0.6 version/2020-11-17T00:39:14Z",
	}

	ctx := testcontext.New(t)
	defer ctx.Cleanup()
	_, err := config.OpenProject(ctx, &uplink.Access{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "user agent")
}

func BenchmarkProject_OpenProject(b *testing.B) {
	b.ReportAllocs()

	ctx := testcontext.New(b)
	defer ctx.Cleanup()

	clientCertPEM, clientKeyPEM := loadPEMs(b, "testdata/testidentity/identity.cert", "testdata/testidentity/identity.key")

	config := uplink.Config{
		DialTimeout: 10 * time.Second,
		ChainPEM:    clientCertPEM,
		KeyPEM:      clientKeyPEM,
	}

	access, err := uplink.ParseAccess(testAccess)
	require.NoError(b, err)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p, err := config.OpenProject(ctx, access)
		require.NoError(b, err)
		require.NoError(b, p.Close())
	}
}

func loadPEMs(b *testing.B, certPath, keyPath string) (certPEM, keyPEM []byte) {
	if certPath == "" || keyPath == "" {
		b.Skipf("only one of key path and cert path are set (%q, %q)", keyPath, certPath)
	}

	certPEM, err := os.ReadFile(certPath)
	require.NoError(b, err)

	keyPEM, err = os.ReadFile(keyPath)
	require.NoError(b, err)

	return certPEM, keyPEM
}
