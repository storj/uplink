// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"

	"github.com/btcsuite/btcutil/base58"
	"github.com/gogo/protobuf/proto"

	"storj.io/common/encryption"
	"storj.io/common/macaroon"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

// Access contains everything to access a project
// and specific buckets.
type Access struct {
	satelliteNodeURL string
	apiKey           *macaroon.APIKey
	encAccess        *encryptionAccess
	parseErr         error
}

// SharePrefix defines a prefix that will be shared.
type SharePrefix struct {
	Bucket    string
	KeyPrefix string
}

// Permission defines what actions can be used to share.
type Permission struct {
	AllowRead   bool
	AllowWrite  bool
	AllowList   bool
	AllowDelete bool
}

// ParseAccess parses access string.
//
// For convenience with using other arguments,
// parse does not return an error. But, instead
// delays the calls.
func ParseAccess(access string) *Access {
	data, version, err := base58.CheckDecode(access)
	if err != nil || version != 0 {
		return &Access{parseErr: Error.New("invalid access format")}
	}

	p := new(pb.Scope)
	if err := proto.Unmarshal(data, p); err != nil {
		return &Access{parseErr: Error.New("unable to unmarshal access: %v", err)}
	}

	if len(p.SatelliteAddr) == 0 {
		return &Access{parseErr: Error.New("access missing satellite address")}
	}

	apiKey, err := macaroon.ParseRawAPIKey(p.ApiKey)
	if err != nil {
		return &Access{parseErr: Error.New("access has malformed api key: %v", err)}
	}

	encAccess, err := parseEncryptionAccessFromProto(p.EncryptionAccess)
	if err != nil {
		return &Access{parseErr: Error.New("access has malformed encryption access: %v", err)}
	}

	return &Access{
		satelliteNodeURL: p.SatelliteAddr,
		apiKey:           apiKey,
		encAccess:        encAccess,
	}
}

// IsValid returns error if parsing was unsuccessful.
func (access *Access) IsValid() error {
	return access.parseErr
}

// Serialize serializes access such that it can be used with ParseAccess.
func (access *Access) Serialize() (string, error) {
	switch {
	case len(access.satelliteNodeURL) == 0:
		return "", Error.New("access missing satellite address")
	case access.apiKey == nil:
		return "", Error.New("access missing api key")
	case access.encAccess == nil:
		return "", Error.New("access missing encryption access")
	}

	enc, err := access.encAccess.toProto()
	if err != nil {
		return "", Error.Wrap(err)
	}

	data, err := proto.Marshal(&pb.Scope{
		SatelliteAddr:    access.satelliteNodeURL,
		ApiKey:           access.apiKey.SerializeRaw(),
		EncryptionAccess: enc,
	})
	if err != nil {
		return "", Error.New("unable to marshal access: %v", err)
	}

	return base58.CheckEncode(data, 0), nil
}

// RequestAccessWithPassphrase requests satellite for a new access using a passhprase.
func RequestAccessWithPassphrase(ctx context.Context, satelliteNodeURL, apiKey, passphrase string) (*Access, error) {
	return (Config{}).RequestAccessWithPassphrase(ctx, satelliteNodeURL, apiKey, passphrase)
}

// RequestAccessWithPassphrase requests satellite for a new access using a passphrase.
func (config Config) RequestAccessWithPassphrase(ctx context.Context, satelliteNodeURL, apiKey, passphrase string) (*Access, error) {
	return config.BackwardCompatibleRequestAccessWithPassphraseAndConcurrency(ctx, satelliteNodeURL, apiKey, passphrase, 8)
}

// BackwardCompatibleRequestAccessWithPassphraseAndConcurrency requests satellite for a new access using a passhprase and specific concurrency for the Argon2 key derivation.
func (config Config) BackwardCompatibleRequestAccessWithPassphraseAndConcurrency(ctx context.Context, satelliteNodeURL, apiKey, passphrase string, concurrency uint8) (*Access, error) {
	parsedAPIKey, err := macaroon.ParseAPIKey(apiKey)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	metainfo, _, fullNodeURL, err := config.dial(ctx, satelliteNodeURL, parsedAPIKey)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	info, err := metainfo.GetProjectInfo(ctx)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	key, err := encryption.DeriveRootKey([]byte(passphrase), info.ProjectSalt, "", concurrency)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	encAccess := newEncryptionAccessWithDefaultKey(key)

	return &Access{
		satelliteNodeURL: fullNodeURL,
		apiKey:           parsedAPIKey,
		encAccess:        encAccess,
	}, nil
}

// Share creates new Access with spefic permision. Permission will be applied to namespaces is defined.
func (access *Access) Share(permission Permission, prefixes ...SharePrefix) (*Access, error) {
	caveat := macaroon.Caveat{
		DisallowReads:   !permission.AllowRead,
		DisallowWrites:  !permission.AllowWrite,
		DisallowLists:   !permission.AllowList,
		DisallowDeletes: !permission.AllowDelete,
	}

	for _, prefix := range prefixes {
		unencPath := paths.NewUnencrypted(prefix.KeyPrefix)
		cipher := storj.EncAESGCM // TODO(jeff): pick the right path cipher

		encPath, err := encryption.EncryptPath(prefix.Bucket, unencPath, cipher, access.encAccess.store)
		if err != nil {
			return nil, err
		}
		derivedKey, err := encryption.DerivePathKey(prefix.Bucket, unencPath, access.encAccess.store)
		if err != nil {
			return nil, err
		}

		if err := access.encAccess.store.Add(prefix.Bucket, unencPath, encPath, *derivedKey); err != nil {
			return nil, err
		}
		caveat.AllowedPaths = append(caveat.AllowedPaths, &macaroon.Caveat_Path{
			Bucket:              []byte(prefix.Bucket),
			EncryptedPathPrefix: []byte(encPath.Raw()),
		})
	}

	restrictedAPIKey, err := access.apiKey.Restrict(caveat)
	if err != nil {
		return nil, err
	}

	restrictedAccess := &Access{
		satelliteNodeURL: access.satelliteNodeURL,
		apiKey:           restrictedAPIKey,
		encAccess:        access.encAccess,
	}
	return restrictedAccess, nil
}

// ReadOnlyPermission returns permission that allows reading and listing.
func ReadOnlyPermission() Permission {
	return Permission{
		AllowRead: true,
		AllowList: true,
	}
}

// WriteOnlyPermission returns permission that allows writing and deleting.
func WriteOnlyPermission() Permission {
	return Permission{
		AllowWrite:  true,
		AllowDelete: true,
	}
}

// AllPermissions returns permission that allows all actions.
func AllPermissions() Permission {
	return Permission{
		AllowRead:   true,
		AllowWrite:  true,
		AllowList:   true,
		AllowDelete: true,
	}
}
