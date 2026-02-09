// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package splitter

import (
	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
)

// TODO: move it to separate package?
func encryptUserData(userData metaclient.SegmentUserData, cipherSuite storj.CipherSuite, contentKey *storj.Key) (encUserData metaclient.EncryptedSegmentUserData, _ error) {
	if len(userData.ETag) > 0 {
		etagKey, err := encryption.DeriveKey(contentKey, "storj-etag-v1")
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
		checksumKey, err := encryption.DeriveKey(contentKey, "storj-checksum-v1")
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

func nonceForPosition(position metaclient.SegmentPosition) (storj.Nonce, error) {
	var nonce storj.Nonce
	inc := (int64(position.PartNumber) << 32) | (int64(position.Index) + 1)
	_, err := encryption.Increment(&nonce, inc)
	return nonce, err
}
