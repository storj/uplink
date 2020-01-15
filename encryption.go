// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package uplink

import (
	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

// encryptionAccess represents an encryption access context. It holds
// information about how various buckets and objects should be
// encrypted and decrypted.
type encryptionAccess struct {
	store *encryption.Store
}

// newEncryptionAccess creates an encryption access context
func newEncryptionAccess() *encryptionAccess {
	return &encryptionAccess{
		store: encryption.NewStore(),
	}
}

// newEncryptionAccessWithDefaultKey creates an encryption access context with
// a default key set.
// Use (*Project).SaltedKeyFromPassphrase to generate a default key
func newEncryptionAccessWithDefaultKey(defaultKey *storj.Key) *encryptionAccess {
	ec := newEncryptionAccess()
	ec.setDefaultKey(defaultKey)
	return ec
}

// Store returns the underlying encryption store for the access context.
func (s *encryptionAccess) Store() *encryption.Store {
	return s.store
}

// setDefaultKey sets the default key for the encryption access context.
// Use (*Project).SaltedKeyFromPassphrase to generate a default key
func (s *encryptionAccess) setDefaultKey(defaultKey *storj.Key) {
	s.store.SetDefaultKey(defaultKey)
}

func (s *encryptionAccess) toProto() (*pb.EncryptionAccess, error) {
	var storeEntries []*pb.EncryptionAccess_StoreEntry
	err := s.store.Iterate(func(bucket string, unenc paths.Unencrypted, enc paths.Encrypted, key storj.Key) error {
		storeEntries = append(storeEntries, &pb.EncryptionAccess_StoreEntry{
			Bucket:          []byte(bucket),
			UnencryptedPath: []byte(unenc.Raw()),
			EncryptedPath:   []byte(enc.Raw()),
			Key:             key[:],
		})
		return nil
	})
	if err != nil {
		return nil, Error.Wrap(err)
	}

	var defaultKey []byte
	if key := s.store.GetDefaultKey(); key != nil {
		defaultKey = key[:]
	}

	return &pb.EncryptionAccess{
		DefaultKey:   defaultKey,
		StoreEntries: storeEntries,
	}, nil
}

func parseEncryptionAccessFromProto(p *pb.EncryptionAccess) (*encryptionAccess, error) {
	access := newEncryptionAccess()
	if len(p.DefaultKey) > 0 {
		if len(p.DefaultKey) != len(storj.Key{}) {
			return nil, Error.New("invalid default key in encryption access")
		}
		var defaultKey storj.Key
		copy(defaultKey[:], p.DefaultKey)
		access.setDefaultKey(&defaultKey)
	}

	for _, entry := range p.StoreEntries {
		if len(entry.Key) != len(storj.Key{}) {
			return nil, Error.New("invalid key in encryption access entry")
		}
		var key storj.Key
		copy(key[:], entry.Key)

		err := access.store.Add(
			string(entry.Bucket),
			paths.NewUnencrypted(string(entry.UnencryptedPath)),
			paths.NewEncrypted(string(entry.EncryptedPath)),
			key)
		if err != nil {
			return nil, Error.New("invalid encryption access entry: %v", err)
		}
	}

	return access, nil
}
