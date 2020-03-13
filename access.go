// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"context"
	"time"

	"github.com/btcsuite/btcutil/base58"
	"github.com/gogo/protobuf/proto"
	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/macaroon"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/internal/expose"
)

// Access contains everything to access a project and specific buckets.
type Access struct {
	satelliteAddress string
	apiKey           *macaroon.APIKey
	encAccess        *encryptionAccess
	// TODO: bypassObjectKeyEncryption bool
}

// SharePrefix defines a prefix that will be shared.
type SharePrefix struct {
	Bucket string
	// Prefix is the prefix of the shared object keys.
	Prefix string
}

// Permission defines what actions can be used to share.
type Permission struct {
	// AllowDownaload gives permission to download the object's content. It
	// allows getting object metadata, but it does not allow listing buckets.
	AllowDownload bool
	// AllowUpload gives permission to create buckets and upload new objects.
	// It does not allow overwriting existing objects unless AllowDelete is
	// granted too.
	AllowUpload bool
	// AllowList gives permission to list buckets. It allows getting object
	// metadata, but it does not allow downloading the object's content.
	AllowList bool
	// AllowDelete gives permission to delete buckets and objects. Unless
	// either AllowDownload or AllowList is granted too, no object metadata and
	// no error info will be returned for deleted objects.
	AllowDelete bool
	// NotBefore if set should be always before NotAfter.
	NotBefore time.Time
	// NotAfter if set should be always after NotBefore.
	NotAfter time.Time
}

// ParseAccess parses access string.
func ParseAccess(access string) (*Access, error) {
	data, version, err := base58.CheckDecode(access)
	if err != nil || version != 0 {
		return nil, packageError.New("invalid access format")
	}

	p := new(pb.Scope)
	if err := proto.Unmarshal(data, p); err != nil {
		return nil, packageError.New("unable to unmarshal access: %v", err)
	}

	if len(p.SatelliteAddr) == 0 {
		return nil, packageError.New("access missing satellite address")
	}

	apiKey, err := macaroon.ParseRawAPIKey(p.ApiKey)
	if err != nil {
		return nil, packageError.New("access has malformed api key: %v", err)
	}

	encAccess, err := parseEncryptionAccessFromProto(p.EncryptionAccess)
	if err != nil {
		return nil, packageError.New("access has malformed encryption access: %v", err)
	}

	return &Access{
		satelliteAddress: p.SatelliteAddr,
		apiKey:           apiKey,
		encAccess:        encAccess,
	}, nil
}

// Serialize serializes access such that it can be used with ParseAccess.
func (access *Access) Serialize() (string, error) {
	switch {
	case len(access.satelliteAddress) == 0:
		return "", packageError.New("access missing satellite address")
	case access.apiKey == nil:
		return "", packageError.New("access missing api key")
	case access.encAccess == nil:
		return "", packageError.New("access missing encryption access")
	}

	enc, err := access.encAccess.toProto()
	if err != nil {
		return "", packageError.Wrap(err)
	}

	data, err := proto.Marshal(&pb.Scope{
		SatelliteAddr:    access.satelliteAddress,
		ApiKey:           access.apiKey.SerializeRaw(),
		EncryptionAccess: enc,
	})
	if err != nil {
		return "", packageError.New("unable to marshal access: %v", err)
	}

	return base58.CheckEncode(data, 0), nil
}

// RequestAccessWithPassphrase requests satellite for a new access using a passhprase.
func RequestAccessWithPassphrase(ctx context.Context, satelliteAddress, apiKey, passphrase string) (*Access, error) {
	return (Config{}).RequestAccessWithPassphrase(ctx, satelliteAddress, apiKey, passphrase)
}

// RequestAccessWithPassphrase requests satellite for a new access using a passphrase.
func (config Config) RequestAccessWithPassphrase(ctx context.Context, satelliteAddress, apiKey, passphrase string) (*Access, error) {
	return requestAccessWithPassphraseAndConcurrency(ctx, config, satelliteAddress, apiKey, passphrase, 8)
}

func init() {
	// expose this method for backcomp package.
	expose.RequestAccessWithPassphraseAndConcurrency = requestAccessWithPassphraseAndConcurrency

	// expose this method for private/access package.
	expose.EnablePathEncryptionBypass = enablePathEncryptionBypass
}

// requestAccessWithPassphraseAndConcurrency requests satellite for a new access using a passhprase and specific concurrency for the Argon2 key derivation.
//
// NB: when modifying the signature of this func, also update backcomp and internal/expose packages.
func requestAccessWithPassphraseAndConcurrency(ctx context.Context, config Config, satelliteAddress, apiKey, passphrase string, concurrency uint8) (_ *Access, err error) {
	parsedAPIKey, err := macaroon.ParseAPIKey(apiKey)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	metainfo, _, fullNodeURL, err := config.dial(ctx, satelliteAddress, parsedAPIKey)
	if err != nil {
		return nil, packageError.Wrap(err)
	}
	defer func() { err = errs.Combine(err, metainfo.Close()) }()

	info, err := metainfo.GetProjectInfo(ctx)
	if err != nil {
		return nil, convertKnownErrors(err)
	}

	key, err := encryption.DeriveRootKey([]byte(passphrase), info.ProjectSalt, "", concurrency)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	encAccess := newEncryptionAccessWithDefaultKey(key)
	encAccess.setDefaultPathCipher(storj.EncAESGCM)
	return &Access{
		satelliteAddress: fullNodeURL,
		apiKey:           parsedAPIKey,
		encAccess:        encAccess,
	}, nil
}

// enablePathEncryptionBypass enables path encryption bypass for embedded encryption access.
//
// NB: when modifying the signature of this func, also update private/access and internal/expose packages.
func enablePathEncryptionBypass(access *Access) error {
	access.encAccess.Store().EncryptionBypass = true
	return nil
}

// Share creates new Access with specific permission. Permission will be applied to prefixes when defined.
func (access *Access) Share(permission Permission, prefixes ...SharePrefix) (*Access, error) {
	if permission == (Permission{}) {
		return nil, packageError.New("permission is empty")
	}

	var notBefore, notAfter *time.Time
	if !permission.NotBefore.IsZero() {
		notBefore = &permission.NotBefore
	}
	if !permission.NotAfter.IsZero() {
		notAfter = &permission.NotAfter
	}

	if notBefore != nil && notAfter != nil && notAfter.Before(*notBefore) {
		return nil, packageError.New("invalid time range")
	}

	caveat := macaroon.Caveat{
		DisallowReads:   !permission.AllowDownload,
		DisallowWrites:  !permission.AllowUpload,
		DisallowLists:   !permission.AllowList,
		DisallowDeletes: !permission.AllowDelete,
		NotBefore:       notBefore,
		NotAfter:        notAfter,
	}

	sharedAccess := newEncryptionAccess()
	sharedAccess.setDefaultPathCipher(access.encAccess.Store().GetDefaultPathCipher())
	if len(prefixes) == 0 {
		sharedAccess.setDefaultKey(access.encAccess.Store().GetDefaultKey())
	}

	for _, prefix := range prefixes {
		unencPath := paths.NewUnencrypted(prefix.Prefix)

		encPath, err := encryption.EncryptPathWithStoreCipher(prefix.Bucket, unencPath, access.encAccess.store)
		if err != nil {
			return nil, err
		}
		derivedKey, err := encryption.DerivePathKey(prefix.Bucket, unencPath, access.encAccess.store)
		if err != nil {
			return nil, err
		}

		if err := sharedAccess.store.Add(prefix.Bucket, unencPath, encPath, *derivedKey); err != nil {
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
		satelliteAddress: access.satelliteAddress,
		apiKey:           restrictedAPIKey,
		encAccess:        sharedAccess,
	}
	return restrictedAccess, nil
}

// ReadOnlyPermission returns permission that allows reading and listing.
func ReadOnlyPermission() Permission {
	return Permission{
		AllowDownload: true,
		AllowList:     true,
	}
}

// WriteOnlyPermission returns permission that allows writing and deleting.
func WriteOnlyPermission() Permission {
	return Permission{
		AllowUpload: true,
		AllowDelete: true,
	}
}

// FullPermission returns permission that allows all actions.
func FullPermission() Permission {
	return Permission{
		AllowDownload: true,
		AllowUpload:   true,
		AllowList:     true,
		AllowDelete:   true,
	}
}
