// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package metaclient

import (
	"time"

	"github.com/zeebo/errs"

	"storj.io/common/pb"
)

// MutableStream is for manipulating stream information.
type MutableStream struct {
	info Object

	dynamic         bool
	dynamicMetadata SerializedUserDataProvider
}

// SerializedUserDataProvider is an interface for retrieving a serialized set of object user data.
type SerializedUserDataProvider interface {
	SerializedUserData() (SerializedUserData, error)
}

// SerializedUserData represents a set of object metadata whose values originate from the user.
// Unlike ObjectUserData, the custom metadata is stored serialized as a byte slice.
type SerializedUserData struct {
	Custom   []byte
	ETag     []byte
	Checksum ObjectChecksum
}

// BucketName returns streams bucket name.
func (stream *MutableStream) BucketName() string { return stream.info.Bucket.Name }

// Path returns streams path.
func (stream *MutableStream) Path() string { return stream.info.Path }

// Info returns object info about the stream.
func (stream *MutableStream) Info() Object { return stream.info }

// Expires returns stream expiration time.
func (stream *MutableStream) Expires() time.Time { return stream.info.Expires }

// SerializedUserData returns the serialized user data associated with the stream.
func (stream *MutableStream) SerializedUserData() (SerializedUserData, error) {
	if stream.dynamic {
		return stream.dynamicMetadata.SerializedUserData()
	}

	if stream.info.ContentType != "" {
		if stream.info.UserData.Custom == nil {
			stream.info.UserData.Custom = make(map[string]string)
			stream.info.UserData.Custom[contentTypeKey] = stream.info.ContentType
		} else if _, found := stream.info.UserData.Custom[contentTypeKey]; !found {
			stream.info.UserData.Custom[contentTypeKey] = stream.info.ContentType
		}
	}

	var serializedCustom []byte
	if stream.info.UserData.Custom != nil {
		var err error
		serializedCustom, err = pb.Marshal(&pb.SerializableMeta{
			UserDefined: stream.info.UserData.Custom,
		})
		if err != nil {
			return SerializedUserData{}, errs.Wrap(err)
		}
	}

	return SerializedUserData{
		Custom:   serializedCustom,
		ETag:     stream.info.UserData.ETag,
		Checksum: stream.info.UserData.Checksum,
	}, nil
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
	UserData ObjectUserData

	IfNoneMatch []string
}
