// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package privateprops

import "storj.io/uplink/private/metaclient"

// Object contains properties of uplink.Object that should not be exposed to the public API.
type Object struct {
	ETag        []byte
	Checksum    metaclient.ObjectChecksum
	Version     []byte
	IsVersioned bool
	IsLatest    bool
}

// Part contains properties of uplink.Part that should not be exposed to the public API.
type Part struct {
	Checksum []byte
}

// UploadInfo contains properties of uplink.UploadInfo that should not be exposed to the public API.
type UploadInfo struct {
	ETag     []byte
	Checksum metaclient.ObjectChecksum
}
