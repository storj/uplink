// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import (
	"context"
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

// ReadOnlyStream is an interface for reading segment information.
type ReadOnlyStream interface {
	Info() storj.Object

	// SegmentsAt returns the segment that contains the byteOffset and following segments.
	// Limit specifies how much to return at most.
	SegmentsAt(ctx context.Context, byteOffset int64, limit int64) (infos []storj.Segment, more bool, err error)
	// Segments returns the segment at index.
	// Limit specifies how much to return at most.
	Segments(ctx context.Context, index int64, limit int64) (infos []storj.Segment, more bool, err error)
}
