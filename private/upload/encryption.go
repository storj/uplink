package upload

import (
	"context"
	"storj.io/common/encryption"
	"storj.io/common/storj"
)

type Encrypter struct {
	transformer        encryption.Transformer
	blockNo            int64
	buffer             []byte
	Output             EncryptedWriterLayer
	EncryptedBlockSize int
}

type EncryptedWriterLayer interface {
	StartObject(ctx context.Context, info *StartObject) error
	StartSegment(ctx context.Context, index int) error
	EndSegment(ctx context.Context) error
	EndObject(ctx context.Context) error
	EncryptedWrite(context.Context, int, []byte) error
	EncryptedCommit(context.Context) error
	EncryptionParams(ctx context.Context, v EncryptionParams) error
}

var _ SegmentedWithKeyLayer = &Encrypter{}

func (e *Encrypter) Write(ctx context.Context, bytes []byte) error {
	out, err := e.transformer.Transform(e.buffer, bytes, 0)
	if err != nil {
		return err
	}
	return e.Output.EncryptedWrite(ctx, len(bytes), out)
}

func (e *Encrypter) Commit(ctx context.Context) error {
	return e.Output.EncryptedCommit(ctx)
}

func (e *Encrypter) StartObject(ctx context.Context, info *StartObject) error {
	return e.Output.StartObject(ctx, info)
}

func (e *Encrypter) StartSegment(ctx context.Context, index int) error {
	return e.Output.StartSegment(ctx, index)
}

func (e *Encrypter) EndSegment(ctx context.Context) error {
	return e.Output.EndSegment(ctx)
}

func (e *Encrypter) EndObject(ctx context.Context) error {
	return e.Output.EndObject(ctx)
}

func (e *Encrypter) EncryptionParams(ctx context.Context, v EncryptionParams) (err error) {
	//TODO: what is the nonce here
	e.transformer, err = encryption.NewEncrypter(v.cipher, v.contentKey, &storj.Nonce{}, e.EncryptedBlockSize)
	if err != nil {
		return err
	}
	return e.Output.EncryptionParams(ctx, v)
}
