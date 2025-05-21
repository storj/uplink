// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package pieceupload

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"slices"

	"github.com/zeebo/errs"

	"storj.io/common/pb"
	"storj.io/common/storj"
)

// These are to improve the readability of the test cases themselves. Integer
// constants are hard to follow.
type revision struct{ value byte }
type piecenum struct{ value int }

func newManager(pieceCount int) *Manager {
	limits := makeLimits(pieceCount)
	exchanger := &fakeExchanger{
		limits: map[byte][]*pb.AddressedOrderLimit{0: limits},
	}
	return NewManager(exchanger, new(fakePieceReader), makeSegmentID(revision{0}), limits)
}

func newManagerWithExchanger(pieceCount int, exchanger LimitsExchanger) *Manager {
	limits := makeLimits(pieceCount)
	return NewManager(exchanger, new(fakePieceReader), makeSegmentID(revision{0}), limits)
}

type fakePieceReader struct{}

func (f fakePieceReader) PieceReader(num int) io.Reader {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], uint64(num))
	return bytes.NewReader(data[:])
}

func pieceReaderNum(r io.Reader) piecenum {
	var data [8]byte
	_, _ = io.ReadFull(r, data[:])
	return piecenum{int(binary.BigEndian.Uint64(data[:]))}
}

type failExchange struct{}

func (failExchange) ExchangeLimits(ctx context.Context, segmentID storj.SegmentID, pieceNumbers []int) (storj.SegmentID, []*pb.AddressedOrderLimit, error) {
	return nil, nil, errors.New("oh no")
}

type fakeExchanger struct {
	limits map[byte][]*pb.AddressedOrderLimit
}

func (f fakeExchanger) ExchangeLimits(ctx context.Context, segmentID storj.SegmentID, pieceNumbers []int) (storj.SegmentID, []*pb.AddressedOrderLimit, error) {
	if len(segmentID) != 1 {
		return nil, nil, errs.New("programmer error: test segment IDs should be a single byte")
	}

	rev := segmentID[0]

	limits := slices.Clone(f.limits[rev])
	if len(limits) == 0 {
		return nil, nil, errs.New("programmer error: no limits for segment ID revision %d", rev)
	}

	rev++

	for _, num := range pieceNumbers {
		if num < 0 || num >= len(limits) {
			return nil, nil, errs.New("programmer error: piece number %d outside the range [0-%d)", num, len(limits))
		}
		limits[num] = makeLimit(piecenum{num}, revision{rev})
	}

	f.limits[rev] = limits

	return makeSegmentID(revision{rev}), limits, nil
}

func makeLimits(n int) []*pb.AddressedOrderLimit {
	var limits []*pb.AddressedOrderLimit
	for num := range n {
		limits = append(limits, makeLimit(piecenum{num}, revision{0}))
	}
	return limits
}

func makeLimit(num piecenum, rev revision) *pb.AddressedOrderLimit {
	return &pb.AddressedOrderLimit{
		Limit: &pb.OrderLimit{
			PieceId:       pieceID(num),
			StorageNodeId: nodeID(num, rev),
		},
	}
}

func makeSegmentID(rev revision) storj.SegmentID {
	return storj.SegmentID{rev.value}
}

func hash(num piecenum) *pb.PieceHash {
	return &pb.PieceHash{PieceId: pieceID(num)}
}

func pieceID(num piecenum) pb.PieceID {
	var id pb.PieceID
	binary.LittleEndian.PutUint64(id[:], uint64(num.value))
	return id
}

func nodeID(num piecenum, rev revision) pb.NodeID {
	var id pb.NodeID
	binary.LittleEndian.PutUint64(id[:8], uint64(num.value))
	binary.LittleEndian.PutUint64(id[8:], uint64(rev.value))
	return id
}
