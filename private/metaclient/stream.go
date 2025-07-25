// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package metaclient

import (
	"time"

	"storj.io/common/pb"
)

// MutableStream is for manipulating stream information.
type MutableStream struct {
	info Object

	dynamic         bool
	dynamicMetadata SerializableMeta
}

// SerializableMeta is an interface for getting pb.SerializableMeta.
type SerializableMeta interface {
	ETag() ([]byte, error)
	Metadata() ([]byte, error)
}

// BucketName returns streams bucket name.
func (stream *MutableStream) BucketName() string { return stream.info.Bucket.Name }

// Path returns streams path.
func (stream *MutableStream) Path() string { return stream.info.Path }

// Info returns object info about the stream.
func (stream *MutableStream) Info() Object { return stream.info }

// Expires returns stream expiration time.
func (stream *MutableStream) Expires() time.Time { return stream.info.Expires }

// Metadata returns metadata associated with the stream.
func (stream *MutableStream) Metadata() ([]byte, error) {
	if stream.dynamic {
		return stream.dynamicMetadata.Metadata()
	}

	if stream.info.ContentType != "" {
		if stream.info.Metadata == nil {
			stream.info.Metadata = make(map[string]string)
			stream.info.Metadata[contentTypeKey] = stream.info.ContentType
		} else if _, found := stream.info.Metadata[contentTypeKey]; !found {
			stream.info.Metadata[contentTypeKey] = stream.info.ContentType
		}
	}
	if stream.info.Metadata == nil {
		return []byte{}, nil
	}
	return pb.Marshal(&pb.SerializableMeta{
		UserDefined: stream.info.Metadata,
	})
}

// ETag returns the etag for the stream.
func (stream *MutableStream) ETag() ([]byte, error) {
	if stream.dynamic {
		return stream.dynamicMetadata.ETag()
	}
	return stream.info.ETag, nil
}

// UploadOptions contains additional options for uploading.
type UploadOptions struct {
	// When Expires is zero, there is no expiration.
	Expires time.Time

	Retention Retention
	LegalHold bool

	IfNoneMatch []string
}

// CommitUploadOptions contains additional options for committing an upload.
type CommitUploadOptions struct {
	CustomMetadata map[string]string
	ETag           []byte

	IfNoneMatch []string
}
