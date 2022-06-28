package upload

import (
	"context"
	"io"
)

type WritingLayer interface {
	Write(context.Context, []byte) error
	Commit(context.Context) error
}

type ChunkedWriter struct {
	size   int
	output WritingLayer
	buffer []byte
	pos    int
	ctx    context.Context
}

var _ io.WriteCloser = &ChunkedWriter{}

func NewChunkedWriter(ctx context.Context, output WritingLayer, size int) (*ChunkedWriter, error) {
	return &ChunkedWriter{
		ctx:    ctx,
		size:   size,
		output: output,
		buffer: make([]byte, size),
	}, nil
}

func (c ChunkedWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.pos > 0 {
		panic("not implemented")
		//remaining := c.size - c.pos
		//if remaining > len(p) {
		//	copy(c.buffer[c.pos:],)
		//}
	}

	if len(p) >= c.size {
		err := c.output.Write(c.ctx, c.buffer)
		if err != nil {
			return 0, err
		}
		return c.Write(p[c.size:])
	}
	panic("not implemented")
}

func (c ChunkedWriter) Close() error {
	return c.Commit()
}

func (c ChunkedWriter) Commit() error {
	return c.output.Commit(c.ctx)
}

var _ io.Writer = &ChunkedWriter{}
