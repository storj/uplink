package upload

import (
	"context"
)

type Padding struct {
	ChunkSize int
	Output    WritingLayer
}

func (p Padding) Write(ctx context.Context, bytes []byte) error {
	return p.Output.Write(ctx, bytes)
}

func (p Padding) Commit(ctx context.Context) error {
	//TODO: do the padding here
	return p.Output.Commit(ctx)
}

var _ WritingLayer = &Padding{}
