// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package objects

import (
	"context"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/spacemonkeygo/monkit/v3"
	"go.uber.org/zap"

	"storj.io/common/pb"
	"storj.io/common/ranger"
	"storj.io/common/storj"
	"storj.io/uplink/private/storage/streams"
)

var mon = monkit.Package()

// Meta is the full object metadata
type Meta struct {
	pb.SerializableMeta
	Modified   time.Time
	Expiration time.Time
	Size       int64
	Checksum   string
}

// Metadata interface returns the latest metadata for an object.
type Metadata interface {
	Metadata() ([]byte, error)
}

// Store for objects
type Store interface {
	Get(ctx context.Context, path storj.Path, object storj.Object) (rr ranger.Ranger, err error)
	Put(ctx context.Context, path storj.Path, data io.Reader, metadata Metadata, expiration time.Time) (meta Meta, err error)
	Delete(ctx context.Context, path storj.Path) (err error)
}

type objStore struct {
	store streams.Store
}

// NewStore for objects
func NewStore(store streams.Store) Store {
	return &objStore{store: store}
}

func (o *objStore) Get(ctx context.Context, path storj.Path, object storj.Object) (
	rr ranger.Ranger, err error) {
	defer mon.Task()(&ctx)(&err)

	if len(path) == 0 {
		return nil, storj.ErrNoPath.New("")
	}

	rr, err = o.store.Get(ctx, path, object)
	return rr, err
}

func (o *objStore) Put(ctx context.Context, path storj.Path, data io.Reader, metadata Metadata, expiration time.Time) (meta Meta, err error) {
	defer mon.Task()(&ctx)(&err)

	if len(path) == 0 {
		return Meta{}, storj.ErrNoPath.New("")
	}

	m, err := o.store.Put(ctx, path, data, metadata, expiration)
	return convertMeta(m), err
}

func (o *objStore) Delete(ctx context.Context, path storj.Path) (err error) {
	defer mon.Task()(&ctx)(&err)

	if len(path) == 0 {
		return storj.ErrNoPath.New("")
	}

	return o.store.Delete(ctx, path)
}

// convertMeta converts stream metadata to object metadata
func convertMeta(m streams.Meta) Meta {
	ser := pb.SerializableMeta{}
	err := proto.Unmarshal(m.Data, &ser)
	if err != nil {
		zap.S().Warnf("Failed deserializing metadata: %v", err)
	}
	return Meta{
		Modified:         m.Modified,
		Expiration:       m.Expiration,
		Size:             m.Size,
		SerializableMeta: ser,
	}
}
