// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package splitter

import (
	"io"

	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams/buffer"
)

type splitterInline struct {
	position   metaclient.SegmentPosition
	encryption metaclient.SegmentEncryption
	encParams  storj.EncryptionParameters
	contentKey *storj.Key

	encData   []byte
	plainSize int64
}

func (s *splitterInline) Begin() metaclient.BatchItem {
	return &metaclient.MakeInlineSegmentParams{
		StreamID:            nil, // set by the stream batcher
		Position:            s.position,
		Encryption:          s.encryption,
		EncryptedInlineData: s.encData,
		PlainSize:           s.plainSize,
		EncryptedTag:        nil, // set by the segment tracker
	}
}

func (s *splitterInline) Position() metaclient.SegmentPosition { return s.position }
func (s *splitterInline) Inline() bool                         { return true }
func (s *splitterInline) Reader() buffer.Chunker               { return &chunker{s.encData} }
func (s *splitterInline) DoneReading(err error)                {}

func (s *splitterInline) EncryptETag(eTag []byte) ([]byte, error) {
	return encryptETag(eTag, s.encParams.CipherSuite, s.contentKey)
}

func (s *splitterInline) Finalize() *SegmentInfo {
	return &SegmentInfo{
		Encryption:    s.encryption,
		PlainSize:     s.plainSize,
		EncryptedSize: int64(len(s.encData)),
	}
}

type chunker struct {
	buf []byte
}

func (c *chunker) Chunk(n int) (b []byte, err error) {
	if len(c.buf) == 0 {
		return nil, io.EOF
	}
	if n > len(c.buf) {
		n = len(c.buf)
	}
	b, c.buf = c.buf[:n], c.buf[n:]
	return b, nil
}
