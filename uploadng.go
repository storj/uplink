package uplink

import (
	"bytes"
	"context"
	"crypto/rand"
	"github.com/zeebo/errs"
	"storj.io/common/encryption"
	"storj.io/common/memory"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/upload"
)

type UploadNG struct {
	writer *upload.ChunkedWriter
	info   *Object
}

func (u UploadNG) Info() *Object {
	return u.info
}

func (u UploadNG) Write(p []byte) (n int, err error) {
	return u.writer.Write(p)
}

func (u UploadNG) Commit() error {
	return u.writer.Commit()
}

func (u UploadNG) Abort() error {
	return nil
}

func (u UploadNG) SetCustomMetadata(ctx context.Context, custom CustomMetadata) error {
	return nil
}

func NewUploadNG(ctx context.Context, dialer rpc.Dialer, metaClient *metaclient.Client, encParam storj.EncryptionParameters, bucket string, object metaclient.Object) (UploadNG, error) {
	cypher := object.EncryptionParameters.CipherSuite
	if cypher == storj.EncUnspecified {
		cypher = storj.EncAESGCM
	}
	rsScheme := object.Stream.RedundancyScheme
	//TODO we need rsScheme in advance, possible in object. Later all segments should request the same rs/segment size.
	rsScheme.RequiredShares = 6
	rsScheme.TotalShares = 10
	rsScheme.ShareSize = 1024

	contentKey := storj.Key{}
	_, err := rand.Read(contentKey[:])
	if err != nil {
		return UploadNG{}, errs.Wrap(err)
	}

	contentNonce := storj.Nonce{}
	_, err = encryption.Increment(&contentNonce, int64(0)+1)
	if err != nil {
		return UploadNG{}, errs.Wrap(err)
	}

	transformer, err := encryption.NewEncrypter(encParam.CipherSuite, &contentKey, &contentNonce, int(encParam.BlockSize))
	if err != nil {
		return UploadNG{}, errs.Wrap(err)
	}

	plainBlockSize := int(int32(rsScheme.RequiredShares)*rsScheme.ShareSize) - transformer.OutBlockSize() + transformer.InBlockSize()

	pieceLayer := func() upload.PieceLayer {
		return &upload.Hasher{
			Output: &upload.PieceCache{
				Buffer: bytes.NewBuffer(make([]byte, 262144)),
				Output: &upload.PieceWriter{
					Dialer:         dialer,
					AllocationStep: 65536,
					MaximumStep:    262144,
				},
			},
		}
	}
	longtail := upload.SequentialRouter{
		CreateOutput: pieceLayer,
	}

	fromStorj, err := eestream.NewRedundancyStrategyFromStorj(rsScheme)
	if err != nil {
		return UploadNG{}, errs.Wrap(err)
	}

	ec, err := upload.NewECWRiter(&longtail, fromStorj)
	if err != nil {
		return UploadNG{}, err
	}

	meta := &upload.ObjectMeta{
		Output:     ec,
		Metaclient: metaClient,
	}

	encrypter := &upload.Encrypter{
		Output:             meta,
		EncryptedBlockSize: int(int32(rsScheme.RequiredShares) * rsScheme.ShareSize),
	}

	keys := &upload.KeyDerivation{
		Output: encrypter,
		Cipher: encParam.CipherSuite,
	}

	segmenter := &upload.Segmenter{
		SegmentSize: 64 * memory.MiB.Int() / plainBlockSize * plainBlockSize,
		ChunkSize:   plainBlockSize,
		Output:      keys,
		ObjectInfo: func() *upload.StartObject {
			return &upload.StartObject{
				EncryptionParams: encParam,
			}
		},
	}

	padding := &upload.Padding{
		ChunkSize: plainBlockSize,
		Output:    segmenter,
	}

	c, err := upload.NewChunkedWriter(ctx, padding, plainBlockSize)
	return UploadNG{
		writer: c,
		info:   convertObject(&object),
	}, nil
}
