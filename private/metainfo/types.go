// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import (
	"time"

	"storj.io/common/storj"
)

// RawObjectItem represents raw object item from get request.
type RawObjectItem struct {
	Version       uint32
	Bucket        string
	EncryptedPath []byte

	StreamID storj.StreamID

	Created  time.Time
	Modified time.Time
	Expires  time.Time

	EncryptedMetadataNonce storj.Nonce
	EncryptedMetadata      []byte

	EncryptionParameters storj.EncryptionParameters
	RedundancyScheme     storj.RedundancyScheme
}

// RawObjectListItem represents raw object item from list objects request.
type RawObjectListItem struct {
	EncryptedPath          []byte
	Version                int32
	Status                 int32
	CreatedAt              time.Time
	StatusAt               time.Time
	ExpiresAt              time.Time
	PlainSize              int64
	EncryptedMetadataNonce storj.Nonce
	EncryptedMetadata      []byte
	StreamID               storj.StreamID
	IsPrefix               bool
}
