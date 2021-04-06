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

	PlainSize int64

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

// SegmentPosition segment position in object.
type SegmentPosition = storj.SegmentPosition

// SegmentDownloadResponseInfo represents segment download information inline/remote.
type SegmentDownloadResponseInfo struct {
	SegmentID           storj.SegmentID
	EncryptedSize       int64
	EncryptedInlineData []byte
	Next                SegmentPosition
	Position            SegmentPosition
	PiecePrivateKey     storj.PiecePrivateKey

	SegmentEncryption SegmentEncryption
}

// SegmentEncryption represents segment encryption key and nonce.
type SegmentEncryption = storj.SegmentEncryption

var (
	// ErrNoPath is an error class for using empty path.
	ErrNoPath = storj.ErrNoPath

	// ErrObjectNotFound is an error class for non-existing object.
	ErrObjectNotFound = storj.ErrObjectNotFound
)

// Object contains information about a specific object.
type Object = storj.Object

// ObjectInfo contains information about a specific object.
type ObjectInfo = storj.ObjectInfo

// Stream is information about an object stream.
type Stream = storj.Stream

// LastSegment contains info about last segment.
type LastSegment = storj.LastSegment

// Segment is full segment information.
type Segment = storj.Segment

// Piece is information where a piece is located.
type Piece = storj.Piece

var (
	// ErrBucket is an error class for general bucket errors.
	ErrBucket = storj.ErrBucket

	// ErrNoBucket is an error class for using empty bucket name.
	ErrNoBucket = storj.ErrNoBucket

	// ErrBucketNotFound is an error class for non-existing bucket.
	ErrBucketNotFound = storj.ErrBucketNotFound
)

// Bucket contains information about a specific bucket.
type Bucket = storj.Bucket

// ListDirection specifies listing direction.
type ListDirection = storj.ListDirection

const (
	// Before lists backwards from cursor, without cursor [NOT SUPPORTED].
	Before = storj.Before
	// Backward lists backwards from cursor, including cursor [NOT SUPPORTED].
	Backward = storj.Backward
	// Forward lists forwards from cursor, including cursor.
	Forward = storj.Forward
	// After lists forwards from cursor, without cursor.
	After = storj.After
)

// ListOptions lists objects.
type ListOptions = storj.ListOptions

// ObjectList is a list of objects.
type ObjectList = storj.ObjectList

// ObjectListItem represents listed object.
type ObjectListItem = storj.ObjectListItem

// BucketListOptions lists objects.
type BucketListOptions = storj.BucketListOptions

// BucketList is a list of buckets.
type BucketList = storj.BucketList
