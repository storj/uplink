// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import (
	"context"
	"errors"
	"strings"
	"time"

	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

var contentTypeKey = "content-type"

// Meta info about a segment.
type Meta struct {
	Modified   time.Time
	Expiration time.Time
	Size       int64
	Data       []byte
}

// GetObjectIPs returns the IP addresses of the nodes which hold the object.
func (db *DB) GetObjectIPs(ctx context.Context, bucket storj.Bucket, key string) (_ *GetObjectIPsResponse, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket.Name == "" {
		return nil, storj.ErrNoBucket.New("")
	}

	if key == "" {
		return nil, storj.ErrNoPath.New("")
	}

	encPath, err := encryption.EncryptPathWithStoreCipher(bucket.Name, paths.NewUnencrypted(key), db.encStore)
	if err != nil {
		return nil, err
	}

	return db.metainfo.GetObjectIPs(ctx, GetObjectIPsParams{
		Bucket:        []byte(bucket.Name),
		EncryptedPath: []byte(encPath.Raw()),
	})
}

// CreateObject creates an uploading object and returns an interface for uploading Object information.
func (db *DB) CreateObject(ctx context.Context, bucket, key string, createInfo *CreateObject) (object *MutableObject, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket == "" {
		return nil, storj.ErrNoBucket.New("")
	}

	if key == "" {
		return nil, storj.ErrNoPath.New("")
	}

	info := storj.Object{
		Bucket: storj.Bucket{Name: bucket},
		Path:   key,
	}

	if createInfo != nil {
		info.Metadata = createInfo.Metadata
		info.ContentType = createInfo.ContentType
		info.Expires = createInfo.Expires
		info.RedundancyScheme = createInfo.RedundancyScheme
		info.EncryptionParameters = createInfo.EncryptionParameters
	}

	// TODO: autodetect content type from the path extension
	// if info.ContentType == "" {}

	return &MutableObject{
		info: info,
	}, nil
}

// ModifyObject modifies a committed object.
func (db *DB) ModifyObject(ctx context.Context, bucket, key string) (object *MutableObject, err error) {
	defer mon.Task()(&ctx)(&err)
	return nil, errors.New("not implemented")
}

// DeleteObject deletes an object from database.
func (db *DB) DeleteObject(ctx context.Context, bucket, key string) (_ storj.Object, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket == "" {
		return storj.Object{}, storj.ErrNoBucket.New("")
	}

	if len(key) == 0 {
		return storj.Object{}, storj.ErrNoPath.New("")
	}

	encPath, err := encryption.EncryptPathWithStoreCipher(bucket, paths.NewUnencrypted(key), db.encStore)
	if err != nil {
		return storj.Object{}, err
	}

	object, err := db.metainfo.BeginDeleteObject(ctx, BeginDeleteObjectParams{
		Bucket:        []byte(bucket),
		EncryptedPath: []byte(encPath.Raw()),
	})
	if err != nil {
		return storj.Object{}, err
	}

	return db.objectFromRawObjectItem(ctx, bucket, key, object)
}

// ModifyPendingObject creates an interface for updating a partially uploaded object.
func (db *DB) ModifyPendingObject(ctx context.Context, bucket, key string) (object *MutableObject, err error) {
	defer mon.Task()(&ctx)(&err)
	return nil, errors.New("not implemented")
}

// ListPendingObjects lists pending objects in bucket based on the ListOptions.
func (db *DB) ListPendingObjects(ctx context.Context, bucket string, options storj.ListOptions) (list storj.ObjectList, err error) {
	defer mon.Task()(&ctx)(&err)
	return storj.ObjectList{}, errors.New("not implemented")
}

// ListObjects lists objects in bucket based on the ListOptions.
func (db *DB) ListObjects(ctx context.Context, bucket string, options storj.ListOptions) (list storj.ObjectList, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket == "" {
		return storj.ObjectList{}, storj.ErrNoBucket.New("")
	}

	if options.Prefix != "" && !strings.HasSuffix(options.Prefix, "/") {
		return storj.ObjectList{}, errClass.New("prefix should end with slash")
	}

	var startAfter string
	switch options.Direction {
	// TODO for now we are supporting only storj.After
	// case storj.Forward:
	// 	// forward lists forwards from cursor, including cursor
	// 	startAfter = keyBefore(options.Cursor)
	case storj.After:
		// after lists forwards from cursor, without cursor
		startAfter = options.Cursor
	default:
		return storj.ObjectList{}, errClass.New("invalid direction %d", options.Direction)
	}

	// TODO: we should let libuplink users be able to determine what metadata fields they request as well
	// metaFlags := meta.All
	// if db.pathCipher(bucket) == storj.EncNull || db.pathCipher(bucket) == storj.EncNullBase64URL {
	// 	metaFlags = meta.None
	// }

	// TODO use flags with listing
	// if metaFlags&meta.Size != 0 {
	// Calculating the stream's size require also the user-defined metadata,
	// where stream store keeps info about the number of segments and their size.
	// metaFlags |= meta.UserDefined
	// }

	// Remove the trailing slash from list prefix.
	// Otherwise, if we the list prefix is `/bob/`, the encrypted list
	// prefix results in `enc("")/enc("bob")/enc("")`. This is an incorrect
	// encrypted prefix, what we really want is `enc("")/enc("bob")`.
	prefix := PathForKey(options.Prefix)
	prefixKey, err := encryption.DerivePathKey(bucket, prefix, db.encStore)
	if err != nil {
		return storj.ObjectList{}, errClass.Wrap(err)
	}

	encPrefix, err := encryption.EncryptPathWithStoreCipher(bucket, prefix, db.encStore)
	if err != nil {
		return storj.ObjectList{}, errClass.Wrap(err)
	}

	// We have to encrypt startAfter but only if it doesn't contain a bucket.
	// It contains a bucket if and only if the prefix has no bucket. This is why it is a raw
	// string instead of a typed string: it's either a bucket or an unencrypted path component
	// and that isn't known at compile time.
	needsEncryption := bucket != ""
	var base *encryption.Base
	if needsEncryption {
		_, _, base = db.encStore.LookupEncrypted(bucket, encPrefix)

		startAfter, err = encryption.EncryptPathRaw(startAfter, db.keyCipher(base.PathCipher), prefixKey)
		if err != nil {
			return storj.ObjectList{}, errClass.Wrap(err)
		}
	}

	items, more, err := db.metainfo.ListObjects(ctx, ListObjectsParams{
		Bucket:          []byte(bucket),
		EncryptedPrefix: []byte(encPrefix.Raw()),
		EncryptedCursor: []byte(startAfter),
		Limit:           int32(options.Limit),
		Recursive:       options.Recursive,
	})
	if err != nil {
		return storj.ObjectList{}, errClass.Wrap(err)
	}

	list = storj.ObjectList{
		Bucket: bucket,
		Prefix: options.Prefix,
		More:   more,
		Items:  make([]storj.Object, 0, len(items)),
	}

	for _, item := range items {
		var bucketName string
		var unencryptedKey paths.Unencrypted
		var itemPath string

		if needsEncryption {
			itemPath, err = encryption.DecryptPathRaw(string(item.EncryptedPath), db.keyCipher(base.PathCipher), prefixKey)
			if err != nil {
				// skip items that cannot be decrypted
				if encryption.ErrDecryptFailed.Has(err) {
					continue
				}
				return storj.ObjectList{}, errClass.Wrap(err)
			}

			// TODO(jeff): this shouldn't be necessary if we handled trailing slashes
			// appropriately. there's some issues with list.
			fullPath := prefix.Raw()
			if len(fullPath) > 0 && fullPath[len(fullPath)-1] != '/' {
				fullPath += "/"
			}
			fullPath += itemPath

			bucketName = bucket
			unencryptedKey = paths.NewUnencrypted(fullPath)
		} else {
			itemPath = string(item.EncryptedPath)
			bucketName = string(item.EncryptedPath)
			unencryptedKey = paths.Unencrypted{}
		}

		stream, streamMeta, err := TypedDecryptStreamInfo(ctx, bucketName, unencryptedKey, item.EncryptedMetadata, db.encStore)
		if err != nil {
			// skip items that cannot be decrypted
			if encryption.ErrDecryptFailed.Has(err) {
				continue
			}
			return storj.ObjectList{}, errClass.Wrap(err)
		}

		object, err := db.objectFromRawObjectListItem(bucket, itemPath, item, stream, streamMeta)
		if err != nil {
			return storj.ObjectList{}, errClass.Wrap(err)
		}

		list.Items = append(list.Items, object)
	}

	return list, nil
}

func (db *DB) keyCipher(keyCipher storj.CipherSuite) storj.CipherSuite {
	if db.encStore.EncryptionBypass {
		return storj.EncNullBase64URL
	}
	return keyCipher
}

// GetObject returns information about an object.
func (db *DB) GetObject(ctx context.Context, bucket, key string) (info storj.Object, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket == "" {
		return storj.Object{}, storj.ErrNoBucket.New("")
	}

	if key == "" {
		return storj.Object{}, storj.ErrNoPath.New("")
	}

	encPath, err := encryption.EncryptPathWithStoreCipher(bucket, paths.NewUnencrypted(key), db.encStore)
	if err != nil {
		return storj.Object{}, err
	}

	objectInfo, err := db.metainfo.GetObject(ctx, GetObjectParams{
		Bucket:        []byte(bucket),
		EncryptedPath: []byte(encPath.Raw()),
	})
	if err != nil {
		return storj.Object{}, err
	}

	return db.objectFromRawObjectItem(ctx, bucket, key, objectInfo)
}

func (db *DB) objectFromRawObjectItem(ctx context.Context, bucket, key string, objectInfo RawObjectItem) (storj.Object, error) {
	if objectInfo.Bucket == "" { // zero objectInfo
		return storj.Object{}, nil
	}

	object := storj.Object{
		Version:  0, // TODO:
		Bucket:   storj.Bucket{Name: bucket},
		Path:     key,
		IsPrefix: false,

		Created:  objectInfo.Modified, // TODO: use correct field
		Modified: objectInfo.Modified, // TODO: use correct field
		Expires:  objectInfo.Expires,  // TODO: use correct field

		Stream: storj.Stream{
			ID: objectInfo.StreamID,

			RedundancyScheme:     objectInfo.RedundancyScheme,
			EncryptionParameters: objectInfo.EncryptionParameters,
		},
	}

	streamInfo, streamMeta, err := TypedDecryptStreamInfo(ctx, bucket, paths.NewUnencrypted(key), objectInfo.EncryptedMetadata, db.encStore)
	if err != nil {
		return storj.Object{}, err
	}

	if object.Stream.EncryptionParameters.CipherSuite == storj.EncUnspecified {
		object.Stream.EncryptionParameters = storj.EncryptionParameters{
			CipherSuite: storj.CipherSuite(streamMeta.EncryptionType),
			BlockSize:   streamMeta.EncryptionBlockSize,
		}
	}
	if streamMeta.LastSegmentMeta != nil {
		var nonce storj.Nonce
		copy(nonce[:], streamMeta.LastSegmentMeta.KeyNonce)

		object.Stream.LastSegment = storj.LastSegment{
			EncryptedKeyNonce: nonce,
			EncryptedKey:      streamMeta.LastSegmentMeta.EncryptedKey,
		}
	}

	err = updateObjectWithStream(&object, streamInfo, streamMeta)
	if err != nil {
		return storj.Object{}, err
	}

	return object, nil
}

func (db *DB) objectFromRawObjectListItem(bucket string, path storj.Path, listItem RawObjectListItem, stream *pb.StreamInfo, streamMeta pb.StreamMeta) (storj.Object, error) {
	object := storj.Object{
		Version:  0, // TODO:
		Bucket:   storj.Bucket{Name: bucket},
		Path:     path,
		IsPrefix: listItem.IsPrefix,

		Created:  listItem.CreatedAt, // TODO: use correct field
		Modified: listItem.CreatedAt, // TODO: use correct field
		Expires:  listItem.ExpiresAt,
	}

	err := updateObjectWithStream(&object, stream, streamMeta)
	if err != nil {
		return storj.Object{}, err
	}

	return object, nil
}

func updateObjectWithStream(object *storj.Object, stream *pb.StreamInfo, streamMeta pb.StreamMeta) error {
	if stream == nil {
		return nil
	}

	serializableMeta := pb.SerializableMeta{}
	err := pb.Unmarshal(stream.Metadata, &serializableMeta)
	if err != nil {
		return err
	}

	// ensure that the map is not nil
	if serializableMeta.UserDefined == nil {
		serializableMeta.UserDefined = map[string]string{}
	}

	_, found := serializableMeta.UserDefined[contentTypeKey]
	if !found && serializableMeta.ContentType != "" {
		serializableMeta.UserDefined[contentTypeKey] = serializableMeta.ContentType
	}

	segmentCount := streamMeta.NumberOfSegments
	object.Metadata = serializableMeta.UserDefined

	if object.Stream.Size == 0 {
		object.Stream.Size = ((segmentCount - 1) * stream.SegmentsSize) + stream.LastSegmentSize
	}
	object.Stream.SegmentCount = segmentCount
	object.Stream.FixedSegmentSize = stream.SegmentsSize
	object.Stream.LastSegment.Size = stream.LastSegmentSize

	return nil
}

// MutableObject is for creating an object stream.
type MutableObject struct {
	info storj.Object
}

// Info gets the current information about the object.
func (object *MutableObject) Info() storj.Object { return object.info }

// CreateStream creates a new stream for the object.
func (object *MutableObject) CreateStream(ctx context.Context) (_ *MutableStream, err error) {
	defer mon.Task()(&ctx)(&err)
	return &MutableStream{
		info: object.info,
	}, nil
}

// CreateDynamicStream creates a new dynamic stream for the object.
func (object *MutableObject) CreateDynamicStream(ctx context.Context, metadata SerializableMeta, expires time.Time) (_ *MutableStream, err error) {
	defer mon.Task()(&ctx)(&err)
	return &MutableStream{
		info: object.info,

		dynamic:         true,
		dynamicMetadata: metadata,
		dynamicExpires:  expires,
	}, nil
}

// TypedDecryptStreamInfo decrypts stream info.
func TypedDecryptStreamInfo(ctx context.Context, bucket string, unencryptedKey paths.Unencrypted, streamMetaBytes []byte, encStore *encryption.Store) (
	_ *pb.StreamInfo, streamMeta pb.StreamMeta, err error) {
	defer mon.Task()(&ctx)(&err)

	err = pb.Unmarshal(streamMetaBytes, &streamMeta)
	if err != nil {
		return nil, pb.StreamMeta{}, err
	}

	if encStore.EncryptionBypass {
		return nil, streamMeta, nil
	}

	derivedKey, err := encryption.DeriveContentKey(bucket, unencryptedKey, encStore)
	if err != nil {
		return nil, pb.StreamMeta{}, err
	}

	cipher := storj.CipherSuite(streamMeta.EncryptionType)
	encryptedKey, keyNonce := getEncryptedKeyAndNonce(streamMeta.LastSegmentMeta)
	contentKey, err := encryption.DecryptKey(encryptedKey, cipher, derivedKey, keyNonce)
	if err != nil {
		return nil, pb.StreamMeta{}, err
	}

	// decrypt metadata with the content encryption key and zero nonce
	streamInfo, err := encryption.Decrypt(streamMeta.EncryptedStreamInfo, cipher, contentKey, &storj.Nonce{})
	if err != nil {
		return nil, pb.StreamMeta{}, err
	}

	var stream pb.StreamInfo
	if err := pb.Unmarshal(streamInfo, &stream); err != nil {
		return nil, pb.StreamMeta{}, err
	}

	return &stream, streamMeta, nil
}

func getEncryptedKeyAndNonce(m *pb.SegmentMeta) (storj.EncryptedPrivateKey, *storj.Nonce) {
	if m == nil {
		return nil, nil
	}

	var nonce storj.Nonce
	copy(nonce[:], m.KeyNonce)

	return m.EncryptedKey, &nonce
}

// PathForKey removes the trailing `/` from the raw path, which is required so
// the derived key matches the final list path (which also has the trailing
// encrypted `/` part of the path removed).
func PathForKey(raw string) paths.Unencrypted {
	return paths.NewUnencrypted(strings.TrimSuffix(raw, "/"))
}
