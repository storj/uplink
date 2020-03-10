// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package kvmetainfo

import (
	"context"
	"errors"
	"strings"

	"github.com/gogo/protobuf/proto"

	"storj.io/common/encryption"
	"storj.io/common/memory"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metainfo"
	"storj.io/uplink/private/storage/objects"
	"storj.io/uplink/private/storage/segments"
	"storj.io/uplink/private/storage/streams"
)

// DefaultRS default values for RedundancyScheme
var DefaultRS = storj.RedundancyScheme{
	Algorithm:      storj.ReedSolomon,
	RequiredShares: 20,
	RepairShares:   30,
	OptimalShares:  40,
	TotalShares:    50,
	ShareSize:      1 * memory.KiB.Int32(),
}

// DefaultES default values for EncryptionParameters
// BlockSize should default to the size of a stripe
var DefaultES = storj.EncryptionParameters{
	CipherSuite: storj.EncAESGCM,
	BlockSize:   DefaultRS.StripeSize(),
}

var contentTypeKey = "content-type"

// GetObject returns information about an object
func (db *DB) GetObject(ctx context.Context, bucket storj.Bucket, path storj.Path) (info storj.Object, err error) {
	defer mon.Task()(&ctx)(&err)

	_, info, err = db.getInfo(ctx, bucket, path)

	return info, err
}

// GetObjectStream returns interface for reading the object stream
func (db *DB) GetObjectStream(ctx context.Context, bucket storj.Bucket, object storj.Object) (stream ReadOnlyStream, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket.Name == "" {
		return nil, storj.ErrNoBucket.New("")
	}

	if object.Path == "" {
		return nil, storj.ErrNoPath.New("")
	}

	return &readonlyStream{
		db:   db,
		info: object,
	}, nil
}

// CreateObject creates an uploading object and returns an interface for uploading Object information
func (db *DB) CreateObject(ctx context.Context, bucket storj.Bucket, path storj.Path, createInfo *CreateObject) (object MutableObject, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket.Name == "" {
		return nil, storj.ErrNoBucket.New("")
	}

	if path == "" {
		return nil, storj.ErrNoPath.New("")
	}

	info := storj.Object{
		Bucket: bucket,
		Path:   path,
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

	if info.EncryptionParameters.IsZero() {
		info.EncryptionParameters = storj.EncryptionParameters{
			CipherSuite: DefaultES.CipherSuite,
			BlockSize:   DefaultES.BlockSize,
		}
	}

	if info.RedundancyScheme.IsZero() {
		info.RedundancyScheme = DefaultRS

		// If the provided EncryptionParameters.BlockSize isn't a multiple of the
		// DefaultRS stripeSize, then overwrite the EncryptionParameters with the DefaultES values
		if err := validateBlockSize(DefaultRS, info.EncryptionParameters.BlockSize); err != nil {
			info.EncryptionParameters.BlockSize = DefaultES.BlockSize
		}
	}

	return &mutableObject{
		db:   db,
		info: info,
	}, nil
}

// ModifyObject modifies a committed object
func (db *DB) ModifyObject(ctx context.Context, bucket storj.Bucket, path storj.Path) (object MutableObject, err error) {
	defer mon.Task()(&ctx)(&err)
	return nil, errors.New("not implemented")
}

// DeleteObject deletes an object from database
func (db *DB) DeleteObject(ctx context.Context, bucket storj.Bucket, path storj.Path) (err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket.Name == "" {
		return storj.ErrNoBucket.New("")
	}

	prefixed := prefixedObjStore{
		store:  objects.NewStore(db.streams),
		prefix: bucket.Name,
	}
	return prefixed.Delete(ctx, path)
}

// ModifyPendingObject creates an interface for updating a partially uploaded object
func (db *DB) ModifyPendingObject(ctx context.Context, bucket storj.Bucket, path storj.Path) (object MutableObject, err error) {
	defer mon.Task()(&ctx)(&err)
	return nil, errors.New("not implemented")
}

// ListPendingObjects lists pending objects in bucket based on the ListOptions
func (db *DB) ListPendingObjects(ctx context.Context, bucket storj.Bucket, options storj.ListOptions) (list storj.ObjectList, err error) {
	defer mon.Task()(&ctx)(&err)
	return storj.ObjectList{}, errors.New("not implemented")
}

// ListObjects lists objects in bucket based on the ListOptions
func (db *DB) ListObjects(ctx context.Context, bucket storj.Bucket, options storj.ListOptions) (list storj.ObjectList, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket.Name == "" {
		return storj.ObjectList{}, storj.ErrNoBucket.New("")
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

	prefix := streams.ParsePath(storj.JoinPaths(bucket.Name, options.Prefix))
	prefixKey, err := encryption.DerivePathKey(prefix.Bucket(), streams.PathForKey(prefix.UnencryptedPath().Raw()), db.encStore)
	if err != nil {
		return storj.ObjectList{}, errClass.Wrap(err)
	}

	encPrefix, err := encryption.EncryptPathWithStoreCipher(prefix.Bucket(), prefix.UnencryptedPath(), db.encStore)
	if err != nil {
		return storj.ObjectList{}, errClass.Wrap(err)
	}

	// If the raw unencrypted path ends in a `/` we need to remove the final
	// section of the encrypted path. For example, if we are listing the path
	// `/bob/`, the encrypted path results in `enc("")/enc("bob")/enc("")`. This
	// is an incorrect list prefix, what we really want is `enc("")/enc("bob")`
	if strings.HasSuffix(prefix.UnencryptedPath().Raw(), "/") {
		lastSlashIdx := strings.LastIndex(encPrefix.Raw(), "/")
		encPrefix = paths.NewEncrypted(encPrefix.Raw()[:lastSlashIdx])
	}

	// We have to encrypt startAfter but only if it doesn't contain a bucket.
	// It contains a bucket if and only if the prefix has no bucket. This is why it is a raw
	// string instead of a typed string: it's either a bucket or an unencrypted path component
	// and that isn't known at compile time.
	needsEncryption := prefix.Bucket() != ""
	var base *encryption.Base
	if needsEncryption {
		_, _, base = db.encStore.LookupEncrypted(prefix.Bucket(), encPrefix)

		startAfter, err = encryption.EncryptPathRaw(startAfter, db.pathCipher(base.PathCipher), prefixKey)
		if err != nil {
			return storj.ObjectList{}, errClass.Wrap(err)
		}
	}

	items, more, err := db.metainfo.ListObjects(ctx, metainfo.ListObjectsParams{
		Bucket:          []byte(bucket.Name),
		EncryptedPrefix: []byte(encPrefix.Raw()),
		EncryptedCursor: []byte(startAfter),
		Limit:           int32(options.Limit),
		Recursive:       options.Recursive,
	})
	if err != nil {
		return storj.ObjectList{}, errClass.Wrap(err)
	}

	list = storj.ObjectList{
		Bucket: bucket.Name,
		Prefix: options.Prefix,
		More:   more,
		Items:  make([]storj.Object, len(items)),
	}

	for i, item := range items {
		var path streams.Path
		var itemPath string

		if needsEncryption {
			itemPath, err = encryption.DecryptPathRaw(string(item.EncryptedPath), db.pathCipher(base.PathCipher), prefixKey)
			if err != nil {
				return storj.ObjectList{}, errClass.Wrap(err)
			}

			// TODO(jeff): this shouldn't be necessary if we handled trailing slashes
			// appropriately. there's some issues with list.
			fullPath := prefix.UnencryptedPath().Raw()
			if len(fullPath) > 0 && fullPath[len(fullPath)-1] != '/' {
				fullPath += "/"
			}
			fullPath += itemPath

			path = streams.CreatePath(prefix.Bucket(), paths.NewUnencrypted(fullPath))
		} else {
			itemPath = string(item.EncryptedPath)
			path = streams.CreatePath(string(item.EncryptedPath), paths.Unencrypted{})
		}

		stream, streamMeta, err := streams.TypedDecryptStreamInfo(ctx, item.EncryptedMetadata, path, db.encStore)
		if err != nil {
			return storj.ObjectList{}, errClass.Wrap(err)
		}

		object, err := objectFromMeta(bucket, itemPath, item, stream, &streamMeta)
		if err != nil {
			return storj.ObjectList{}, errClass.Wrap(err)
		}

		list.Items[i] = object
	}

	return list, nil
}

// ListObjectsExtended lists objects in bucket based on the ListOptions
func (db *DB) ListObjectsExtended(ctx context.Context, bucket storj.Bucket, options storj.ListOptions) (list storj.ObjectList, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket.Name == "" {
		return storj.ObjectList{}, storj.ErrNoBucket.New("")
	}

	if options.Prefix != "" && !strings.HasSuffix(options.Prefix, "/") {
		return storj.ObjectList{}, Error.New("prefix should end with slash")
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

	prefix := streams.ParsePath(storj.JoinPaths(bucket.Name, options.Prefix))
	prefixKey, err := encryption.DerivePathKey(prefix.Bucket(), streams.PathForKey(prefix.UnencryptedPath().Raw()), db.encStore)
	if err != nil {
		return storj.ObjectList{}, errClass.Wrap(err)
	}

	encPrefix, err := encryption.EncryptPathWithStoreCipher(prefix.Bucket(), prefix.UnencryptedPath(), db.encStore)
	if err != nil {
		return storj.ObjectList{}, errClass.Wrap(err)
	}

	// If the raw unencrypted path ends in a `/` we need to remove the final
	// section of the encrypted path. For example, if we are listing the path
	// `/bob/`, the encrypted path results in `enc("")/enc("bob")/enc("")`. This
	// is an incorrect list prefix, what we really want is `enc("")/enc("bob")`
	if strings.HasSuffix(prefix.UnencryptedPath().Raw(), "/") {
		lastSlashIdx := strings.LastIndex(encPrefix.Raw(), "/")
		encPrefix = paths.NewEncrypted(encPrefix.Raw()[:lastSlashIdx])
	}

	// We have to encrypt startAfter but only if it doesn't contain a bucket.
	// It contains a bucket if and only if the prefix has no bucket. This is why it is a raw
	// string instead of a typed string: it's either a bucket or an unencrypted path component
	// and that isn't known at compile time.
	needsEncryption := prefix.Bucket() != ""
	var base *encryption.Base
	if needsEncryption {
		_, _, base = db.encStore.LookupEncrypted(prefix.Bucket(), encPrefix)

		startAfter, err = encryption.EncryptPathRaw(startAfter, db.pathCipher(base.PathCipher), prefixKey)
		if err != nil {
			return storj.ObjectList{}, errClass.Wrap(err)
		}
	}

	items, more, err := db.metainfo.ListObjects(ctx, metainfo.ListObjectsParams{
		Bucket:          []byte(bucket.Name),
		EncryptedPrefix: []byte(encPrefix.Raw()),
		EncryptedCursor: []byte(startAfter),
		Limit:           int32(options.Limit),
		Recursive:       options.Recursive,
	})
	if err != nil {
		return storj.ObjectList{}, errClass.Wrap(err)
	}

	list = storj.ObjectList{
		Bucket: bucket.Name,
		Prefix: options.Prefix,
		More:   more,
		Items:  make([]storj.Object, len(items)),
	}

	for i, item := range items {
		var path streams.Path
		var itemPath string

		if needsEncryption {
			itemPath, err = encryption.DecryptPathRaw(string(item.EncryptedPath), db.pathCipher(base.PathCipher), prefixKey)
			if err != nil {
				return storj.ObjectList{}, errClass.Wrap(err)
			}

			// TODO(jeff): this shouldn't be necessary if we handled trailing slashes
			// appropriately. there's some issues with list.
			fullPath := prefix.UnencryptedPath().Raw()
			if len(fullPath) > 0 && fullPath[len(fullPath)-1] != '/' {
				fullPath += "/"
			}
			fullPath += itemPath

			path = streams.CreatePath(prefix.Bucket(), paths.NewUnencrypted(fullPath))
		} else {
			itemPath = string(item.EncryptedPath)
			path = streams.CreatePath(string(item.EncryptedPath), paths.Unencrypted{})
		}

		stream, streamMeta, err := streams.TypedDecryptStreamInfo(ctx, item.EncryptedMetadata, path, db.encStore)
		if err != nil {
			return storj.ObjectList{}, errClass.Wrap(err)
		}

		object, err := objectFromMeta(bucket, itemPath, item, stream, &streamMeta)
		if err != nil {
			return storj.ObjectList{}, errClass.Wrap(err)
		}

		list.Items[i] = object
	}

	return list, nil
}

func (db *DB) pathCipher(pathCipher storj.CipherSuite) storj.CipherSuite {
	if db.encStore.EncryptionBypass {
		return storj.EncNullBase64URL
	}
	return pathCipher
}

type object struct {
	fullpath        streams.Path
	bucket          string
	encPath         paths.Encrypted
	lastSegmentMeta segments.Meta
	streamInfo      *pb.StreamInfo
	streamMeta      pb.StreamMeta
}

func (db *DB) getInfo(ctx context.Context, bucket storj.Bucket, path storj.Path) (obj object, info storj.Object, err error) {
	defer mon.Task()(&ctx)(&err)

	if bucket.Name == "" {
		return object{}, storj.Object{}, storj.ErrNoBucket.New("")
	}

	if path == "" {
		return object{}, storj.Object{}, storj.ErrNoPath.New("")
	}

	fullpath := streams.CreatePath(bucket.Name, paths.NewUnencrypted(path))

	encPath, err := encryption.EncryptPathWithStoreCipher(bucket.Name, paths.NewUnencrypted(path), db.encStore)
	if err != nil {
		return object{}, storj.Object{}, err
	}

	objectInfo, err := db.metainfo.GetObject(ctx, metainfo.GetObjectParams{
		Bucket:        []byte(bucket.Name),
		EncryptedPath: []byte(encPath.Raw()),
	})
	if err != nil {
		return object{}, storj.Object{}, err
	}

	redundancyScheme := objectInfo.Stream.RedundancyScheme

	lastSegmentMeta := segments.Meta{
		Modified:   objectInfo.Created,
		Expiration: objectInfo.Expires,
		Size:       objectInfo.Size,
		Data:       objectInfo.Metadata,
	}

	streamInfo, streamMeta, err := streams.TypedDecryptStreamInfo(ctx, lastSegmentMeta.Data, fullpath, db.encStore)
	if err != nil {
		return object{}, storj.Object{}, err
	}

	info, err = objectStreamFromMeta(bucket, path, objectInfo.StreamID, lastSegmentMeta, streamInfo, streamMeta, redundancyScheme)
	if err != nil {
		return object{}, storj.Object{}, err
	}

	return object{
		fullpath:        fullpath,
		bucket:          bucket.Name,
		encPath:         encPath,
		lastSegmentMeta: lastSegmentMeta,
		streamInfo:      streamInfo,
		streamMeta:      streamMeta,
	}, info, nil
}

func objectFromMeta(bucket storj.Bucket, path storj.Path, listItem storj.ObjectListItem, stream *pb.StreamInfo, streamMeta *pb.StreamMeta) (storj.Object, error) {
	object := storj.Object{
		Version:  0, // TODO:
		Bucket:   bucket,
		Path:     path,
		IsPrefix: listItem.IsPrefix,

		Created:  listItem.CreatedAt, // TODO: use correct field
		Modified: listItem.CreatedAt, // TODO: use correct field
		Expires:  listItem.ExpiresAt,
	}
	if stream != nil {
		serializableMeta := pb.SerializableMeta{}
		err := proto.Unmarshal(stream.Metadata, &serializableMeta)
		if err != nil {
			return storj.Object{}, err
		}

		// ensure that the map is not nil
		if serializableMeta.UserDefined == nil {
			serializableMeta.UserDefined = map[string]string{}
		}

		_, found := serializableMeta.UserDefined[contentTypeKey]
		if !found && serializableMeta.ContentType != "" {
			serializableMeta.UserDefined[contentTypeKey] = serializableMeta.ContentType
		}

		object.Metadata = serializableMeta.UserDefined
		object.Stream.Size = ((numberOfSegments(stream, streamMeta) - 1) * stream.SegmentsSize) + stream.LastSegmentSize
	}

	return object, nil
}

func objectStreamFromMeta(bucket storj.Bucket, path storj.Path, streamID storj.StreamID, lastSegment segments.Meta, stream *pb.StreamInfo, streamMeta pb.StreamMeta, redundancyScheme storj.RedundancyScheme) (storj.Object, error) {
	var nonce storj.Nonce
	var encryptedKey storj.EncryptedPrivateKey
	if streamMeta.LastSegmentMeta != nil {
		copy(nonce[:], streamMeta.LastSegmentMeta.KeyNonce)
		encryptedKey = streamMeta.LastSegmentMeta.EncryptedKey
	}

	rv := storj.Object{
		Version:  0, // TODO:
		Bucket:   bucket,
		Path:     path,
		IsPrefix: false,

		Created:  lastSegment.Modified,   // TODO: use correct field
		Modified: lastSegment.Modified,   // TODO: use correct field
		Expires:  lastSegment.Expiration, // TODO: use correct field

		Stream: storj.Stream{
			ID: streamID,

			RedundancyScheme: redundancyScheme,
			EncryptionParameters: storj.EncryptionParameters{
				CipherSuite: storj.CipherSuite(streamMeta.EncryptionType),
				BlockSize:   streamMeta.EncryptionBlockSize,
			},
			LastSegment: storj.LastSegment{
				EncryptedKeyNonce: nonce,
				EncryptedKey:      encryptedKey,
			},
		},
	}

	if stream != nil {
		serMetaInfo := pb.SerializableMeta{}
		err := proto.Unmarshal(stream.Metadata, &serMetaInfo)
		if err != nil {
			return storj.Object{}, err
		}

		numberOfSegments := streamMeta.NumberOfSegments
		if streamMeta.NumberOfSegments == 0 {
			numberOfSegments = stream.DeprecatedNumberOfSegments
		}

		_, found := serMetaInfo.UserDefined[contentTypeKey]
		if !found && serMetaInfo.ContentType != "" {
			serMetaInfo.UserDefined[contentTypeKey] = serMetaInfo.ContentType
		}

		rv.Metadata = serMetaInfo.UserDefined
		rv.Stream.Size = stream.SegmentsSize*(numberOfSegments-1) + stream.LastSegmentSize
		rv.Stream.SegmentCount = numberOfSegments
		rv.Stream.FixedSegmentSize = stream.SegmentsSize
		rv.Stream.LastSegment.Size = stream.LastSegmentSize
	}

	return rv, nil
}

type mutableObject struct {
	db   *DB
	info storj.Object
}

func (object *mutableObject) Info() storj.Object { return object.info }

func (object *mutableObject) CreateStream(ctx context.Context) (_ MutableStream, err error) {
	defer mon.Task()(&ctx)(&err)
	return &mutableStream{
		db:   object.db,
		info: object.info,
	}, nil
}

func (object *mutableObject) ContinueStream(ctx context.Context) (_ MutableStream, err error) {
	defer mon.Task()(&ctx)(&err)
	return nil, errors.New("not implemented")
}

func (object *mutableObject) DeleteStream(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)
	return errors.New("not implemented")
}

func (object *mutableObject) Commit(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)
	_, info, err := object.db.getInfo(ctx, object.info.Bucket, object.info.Path)
	object.info = info
	return err
}

func numberOfSegments(stream *pb.StreamInfo, streamMeta *pb.StreamMeta) int64 {
	if streamMeta.NumberOfSegments > 0 {
		return streamMeta.NumberOfSegments
	}
	return stream.DeprecatedNumberOfSegments
}
