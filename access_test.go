// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/uplink"
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
