// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package access2

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/macaroon"
	"storj.io/common/paths"
	"storj.io/common/storj"
)

func TestLimitTo(t *testing.T) {
	// these strings can be of the form <bucket>|<path> or just <path> where the
	// bucket will be implied to be the string "bucket".
	tests := []struct {
		Groups  [][]string
		Valid   []string
		Invalid []string
	}{
		{ // no limit means any path is valid
			Groups: nil,
			Valid:  []string{"a", "b", "c", "a/a", "b/b", "c/c"},
		},
		{ // limited to a
			Groups: [][]string{
				{"a"},
			},
			Valid:   []string{"a", "a/b", "a/b/c"},
			Invalid: []string{"b", "b/a", "c/a"},
		},
		{ // multiple layers
			Groups: [][]string{
				{"a", "f"},
				{"c", "a/b", "f/e"},
				{"a/b/c", "c", "f"},
			},
			Valid:   []string{"a/b/c", "a/b/c/d", "f/e", "f/e/e"},
			Invalid: []string{"a", "a/b", "f", "c", "c/d"},
		},
		{ // check distinct buckets
			Groups: [][]string{
				{"bucket1|", "bucket2|", "bucket3|"},
				{"bucket2|", "bucket3|", "bucket4|"},
			},
			Valid:   []string{"bucket2|anything/here", "bucket3|", "bucket3|whatever"},
			Invalid: []string{"bucket1|", "bucket1|path/ignored", "bucket4|huh", "bucket5|"},
		},
		{ // check buckets with paths
			Groups: [][]string{
				{"b1|p1", "b1|p2", "b2|p3", "b2|p4"},
				{"b1|p1", "b1|p2", "b2|p3"},
			},
			Valid:   []string{"b1|p1", "b1|p1/whatever", "b1|p2", "b2|p3/foo"},
			Invalid: []string{"b3|", "b2|p4", "b1|p3"},
		},
	}

	split := func(prefix string) (bucket, path string) {
		if idx := strings.IndexByte(prefix, '|'); idx >= 0 {
			return prefix[:idx], prefix[idx+1:]
		}
		return "bucket", prefix
	}

	toCaveat := func(group []string) (caveat macaroon.Caveat) {
		for _, prefix := range group {
			bucket, path := split(prefix)
			caveat.AllowedPaths = append(caveat.AllowedPaths, &macaroon.Caveat_Path{
				Bucket:              []byte(bucket),
				EncryptedPathPrefix: []byte(path),
			})
		}
		return caveat
	}

	for _, test := range tests {
		apiKey, err := macaroon.NewAPIKey(nil)
		require.NoError(t, err)

		for _, group := range test.Groups {
			apiKey, err = apiKey.Restrict(toCaveat(group))
			require.NoError(t, err)
		}

		encAccess := NewEncryptionAccessWithDefaultKey(&storj.Key{})
		encAccess.SetDefaultPathCipher(storj.EncNull)
		encAccess.LimitTo(apiKey)

		for _, valid := range test.Valid {
			bucket, path := split(valid)
			_, _, base := encAccess.Store.LookupEncrypted(bucket, paths.NewEncrypted(path))
			require.NotNil(t, base, "searched for %q", valid)
		}

		for _, invalid := range test.Invalid {
			bucket, path := split(invalid)
			_, _, base := encAccess.Store.LookupEncrypted(bucket, paths.NewEncrypted(path))
			require.Nil(t, base, "searched for %q", invalid)
		}
	}
}
