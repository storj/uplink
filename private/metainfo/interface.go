// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import (
	"time"

	"storj.io/common/storj"
)

// CreateObject has optional parameters that can be set.
type CreateObject struct {
	Metadata    map[string]string
	ContentType string
	Expires     time.Time

	storj.RedundancyScheme
	storj.EncryptionParameters
}

// Object converts the CreateObject to an object with unitialized values.
func (create CreateObject) Object(bucket storj.Bucket, path storj.Path) storj.Object {
	return storj.Object{
		Bucket:      bucket,
		Path:        path,
		Metadata:    create.Metadata,
		ContentType: create.ContentType,
		Expires:     create.Expires,
		Stream: storj.Stream{
			Size:             -1,  // unknown
			Checksum:         nil, // unknown
			SegmentCount:     -1,  // unknown
			FixedSegmentSize: -1,  // unknown

			RedundancyScheme:     create.RedundancyScheme,
			EncryptionParameters: create.EncryptionParameters,
		},
	}
}
