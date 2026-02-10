// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package encryption

import (
	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
)

const (
	etagDerivationMsg     = "storj-etag-v1"
	checksumDerivationMsg = "storj-checksum-v1"
)

// EncryptSegmentUserData encrypts a set of segment user data.
func EncryptSegmentUserData(userData metaclient.SegmentUserData, cipherSuite storj.CipherSuite, contentKey *storj.Key) (encUserData metaclient.EncryptedSegmentUserData, _ error) {
	if len(userData.ETag) > 0 {
		etagKey, err := encryption.DeriveKey(contentKey, etagDerivationMsg)
		if err != nil {
			return metaclient.EncryptedSegmentUserData{}, errs.Wrap(err)
		}
		encryptedETag, err := encryption.Encrypt(userData.ETag, cipherSuite, etagKey, &storj.Nonce{})
		if err != nil {
			return metaclient.EncryptedSegmentUserData{}, errs.Wrap(err)
		}
		encUserData.ETag = encryptedETag
	}

	if len(userData.Checksum) > 0 {
		checksumKey, err := encryption.DeriveKey(contentKey, checksumDerivationMsg)
		if err != nil {
			return metaclient.EncryptedSegmentUserData{}, errs.Wrap(err)
		}
		encryptedChecksum, err := encryption.Encrypt(userData.Checksum, cipherSuite, checksumKey, &storj.Nonce{})
		if err != nil {
			return metaclient.EncryptedSegmentUserData{}, errs.Wrap(err)
		}
		encUserData.Checksum = encryptedChecksum
	}

	return encUserData, nil
}

// DecryptSegmentUserData decrypts a set of segment user data.
func DecryptSegmentUserData(encUserData metaclient.EncryptedSegmentUserData, cipherSuite storj.CipherSuite, contentKey *storj.Key) (userData metaclient.SegmentUserData, _ error) {
	if len(encUserData.ETag) > 0 {
		etagKey, err := encryption.DeriveKey(contentKey, etagDerivationMsg)
		if err != nil {
			return metaclient.SegmentUserData{}, errs.Wrap(err)
		}
		eTag, err := encryption.Decrypt(encUserData.ETag, cipherSuite, etagKey, &storj.Nonce{})
		if err != nil {
			return metaclient.SegmentUserData{}, errs.Wrap(err)
		}
		userData.ETag = eTag
	}

	if len(encUserData.Checksum) > 0 {
		checksumKey, err := encryption.DeriveKey(contentKey, checksumDerivationMsg)
		if err != nil {
			return metaclient.SegmentUserData{}, errs.Wrap(err)
		}
		checksum, err := encryption.Decrypt(encUserData.Checksum, cipherSuite, checksumKey, &storj.Nonce{})
		if err != nil {
			return metaclient.SegmentUserData{}, errs.Wrap(err)
		}
		userData.Checksum = checksum
	}

	return userData, nil
}
