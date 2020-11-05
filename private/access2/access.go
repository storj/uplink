// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package access2

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcutil/base58"

	"storj.io/common/encryption"
	"storj.io/common/macaroon"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

// An Access Grant contains everything to access a project and specific buckets.
// It includes a potentially-restricted API Key, a potentially-restricted set
// of encryption information, and information about the Satellite responsible
// for the project's metadata.
type Access struct {
	SatelliteAddress string
	APIKey           *macaroon.APIKey
	EncAccess        *EncryptionAccess
}

// ParseAccess parses a serialized access grant string.
//
// This should be the main way to instantiate an access grant for opening a project.
// See the note on RequestAccessWithPassphrase.
func ParseAccess(access string) (*Access, error) {
	data, version, err := base58.CheckDecode(access)
	if err != nil || version != 0 {
		return nil, errors.New("invalid access grant format")
	}

	p := new(pb.Scope)
	if err := pb.Unmarshal(data, p); err != nil {
		return nil, fmt.Errorf("unable to unmarshal access grant: %w", err)
	}

	if len(p.SatelliteAddr) == 0 {
		return nil, errors.New("access grant is missing satellite address")
	}

	apiKey, err := macaroon.ParseRawAPIKey(p.ApiKey)
	if err != nil {
		return nil, fmt.Errorf("access grant has malformed api key: %w", err)
	}

	encAccess, err := parseEncryptionAccessFromProto(p.EncryptionAccess)
	if err != nil {
		return nil, fmt.Errorf("access grant has malformed encryption access: %w", err)
	}

	return &Access{
		SatelliteAddress: p.SatelliteAddr,
		APIKey:           apiKey,
		EncAccess:        encAccess,
	}, nil
}

// Serialize serializes an access grant such that it can be used later with
// ParseAccess or other tools.
func (access *Access) Serialize() (string, error) {
	switch {
	case len(access.SatelliteAddress) == 0:
		return "", errors.New("access grant is missing satellite address")
	case access.APIKey == nil:
		return "", errors.New("access grant is missing api key")
	case access.EncAccess == nil:
		return "", errors.New("access grant is missing encryption access")
	}

	enc, err := access.EncAccess.toProto()
	if err != nil {
		return "", err
	}

	data, err := pb.Marshal(&pb.Scope{
		SatelliteAddr:    access.SatelliteAddress,
		ApiKey:           access.APIKey.SerializeRaw(),
		EncryptionAccess: enc,
	})
	if err != nil {
		return "", fmt.Errorf("unable to marshal access grant: %w", err)
	}

	return base58.CheckEncode(data, 0), nil
}

// EncryptionAccess represents an encryption access context. It holds
// information about how various buckets and objects should be
// encrypted and decrypted.
type EncryptionAccess struct {
	Store *encryption.Store
}

// NewEncryptionAccess creates an encryption access context.
func NewEncryptionAccess() *EncryptionAccess {
	store := encryption.NewStore()
	return &EncryptionAccess{Store: store}
}

// NewEncryptionAccessWithDefaultKey creates an encryption access context with
// a default key set.
// Use (*Project).SaltedKeyFromPassphrase to generate a default key.
func NewEncryptionAccessWithDefaultKey(defaultKey *storj.Key) *EncryptionAccess {
	ec := NewEncryptionAccess()
	ec.SetDefaultKey(defaultKey)
	return ec
}

// SetDefaultKey sets the default key for the encryption access context.
// Use (*Project).SaltedKeyFromPassphrase to generate a default key.
func (s *EncryptionAccess) SetDefaultKey(defaultKey *storj.Key) {
	s.Store.SetDefaultKey(defaultKey)
}

// SetDefaultPathCipher sets which cipher suite to use by default.
func (s *EncryptionAccess) SetDefaultPathCipher(defaultPathCipher storj.CipherSuite) {
	s.Store.SetDefaultPathCipher(defaultPathCipher)
}

func (s *EncryptionAccess) toProto() (*pb.EncryptionAccess, error) {
	var storeEntries []*pb.EncryptionAccess_StoreEntry
	err := s.Store.IterateWithCipher(func(bucket string, unenc paths.Unencrypted, enc paths.Encrypted, key storj.Key, pathCipher storj.CipherSuite) error {
		storeEntries = append(storeEntries, &pb.EncryptionAccess_StoreEntry{
			Bucket:          []byte(bucket),
			UnencryptedPath: []byte(unenc.Raw()),
			EncryptedPath:   []byte(enc.Raw()),
			Key:             key[:],
			PathCipher:      pb.CipherSuite(pathCipher),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	var defaultKey []byte
	if key := s.Store.GetDefaultKey(); key != nil {
		defaultKey = key[:]
	}

	return &pb.EncryptionAccess{
		DefaultKey:        defaultKey,
		StoreEntries:      storeEntries,
		DefaultPathCipher: pb.CipherSuite(s.Store.GetDefaultPathCipher()),
	}, nil
}

func parseEncryptionAccessFromProto(p *pb.EncryptionAccess) (*EncryptionAccess, error) {
	access := NewEncryptionAccess()
	if len(p.DefaultKey) > 0 {
		if len(p.DefaultKey) != len(storj.Key{}) {
			return nil, errors.New("invalid default key in encryption access")
		}
		var defaultKey storj.Key
		copy(defaultKey[:], p.DefaultKey)
		access.SetDefaultKey(&defaultKey)
	}

	access.SetDefaultPathCipher(storj.CipherSuite(p.DefaultPathCipher))
	// Unspecified cipher suite means that most probably access was serialized
	// before path cipher was moved to encryption access
	if p.DefaultPathCipher == pb.CipherSuite_ENC_UNSPECIFIED {
		access.SetDefaultPathCipher(storj.EncAESGCM)
	}

	for _, entry := range p.StoreEntries {
		if len(entry.Key) != len(storj.Key{}) {
			return nil, errors.New("invalid key in encryption access entry")
		}
		var key storj.Key
		copy(key[:], entry.Key)

		err := access.Store.AddWithCipher(
			string(entry.Bucket),
			paths.NewUnencrypted(string(entry.UnencryptedPath)),
			paths.NewEncrypted(string(entry.EncryptedPath)),
			key,
			storj.CipherSuite(entry.PathCipher),
		)
		if err != nil {
			return nil, fmt.Errorf("invalid encryption access entry: %w", err)
		}
	}

	return access, nil
}
