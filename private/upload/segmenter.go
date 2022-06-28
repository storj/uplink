package upload

import (
	"context"
	"github.com/zeebo/errs"
)

// SegmentedLayer represents the output of the segmentation.
type SegmentedLayer interface {
	WritingLayer
	StartObject(ctx context.Context, info *StartObject) error
	StartSegment(ctx context.Context, index int) error
	EndSegment(ctx context.Context) error
	EndObject(ctx context.Context) error
}

type Segmenter struct {
	SegmentSize int
	ChunkSize   int
	Output      SegmentedLayer
	ObjectInfo  func() *StartObject

	started     bool
	position    int
	openSegment bool
	index       int
}

var _ WritingLayer = &Segmenter{}

func (s *Segmenter) Write(ctx context.Context, bytes []byte) error {
	err := s.init(ctx)
	if err != nil {
		return err
	}
	if len(bytes) != s.ChunkSize {
		return errs.New("Chunk size is not the expected: %d instead of %d", len(bytes), s.ChunkSize)
	}
	err = s.Output.Write(ctx, bytes)
	if err != nil {
		return err
	}
	s.position += len(bytes)
	if s.position == s.SegmentSize {
		err := s.Output.EndSegment(ctx)
		if err != nil {
			return err
		}
		s.position = 0
	}
	return nil
}

func (s *Segmenter) Commit(ctx context.Context) error {
	err := s.init(ctx)
	if err != nil {
		return err
	}
	err = s.Output.Commit(ctx)
	if err != nil {
		return err
	}
	if s.position > 0 {
		err := s.Output.EndSegment(ctx)
		if err != nil {
			return err
		}
		s.position = 0
	}
	if s.started {
		err := s.Output.EndObject(ctx)
		if err != nil {
			return err
		}
		s.started = false
	}
	return nil
}

func (s *Segmenter) init(ctx context.Context) error {
	if !s.started {
		err := s.Output.StartObject(ctx, s.ObjectInfo())
		if err != nil {
			return err
		}
		s.started = true
	}
	if s.position == 0 {
		err := s.Output.StartSegment(ctx, s.index)
		if err != nil {
			return err
		}
		s.index++
		s.openSegment = true
	}
	return nil
}
