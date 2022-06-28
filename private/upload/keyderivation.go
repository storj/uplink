package upload

import (
	"context"
	"crypto/rand"
	"github.com/zeebo/errs"
	"storj.io/common/encryption"
	"storj.io/common/storj"
)

type KeyDerivation struct {
	Output SegmentedWithKeyLayer
	Cipher storj.CipherSuite
}

var _ SegmentedLayer = &KeyDerivation{}

type SegmentedWithKeyLayer interface {
	SegmentedLayer
	EncryptionParams(ctx context.Context, params EncryptionParams) error
}

type EncryptionParams struct {
	cipher storj.CipherSuite

	// random key to encrypt metadata and segment data
	contentKey *storj.Key

	// contentKey encrypted with the derived key
	encryptedKey *storj.EncryptedPrivateKey

	// random nonce used to encrypted the contentKey
	encryptedKeyNonce *storj.Nonce
}

func (k KeyDerivation) StartSegment(ctx context.Context, index int) error {
	var contentKey storj.Key
	var encryptedKeyNonce storj.Nonce

	// generate random key for encrypting the segment's content
	_, err := rand.Read(contentKey[:])
	if err != nil {
		return errs.Wrap(err)
	}

	// Initialize the content nonce with the current total segment incremented
	// by 1 because at this moment the next segment has not been already
	// uploaded.
	// The increment by 1 is to avoid nonce reuse with the metadata encryption,
	// which is encrypted with the zero nonce.
	contentNonce := storj.Nonce{}
	_, err = encryption.Increment(&contentNonce, int64(index)+1)
	if err != nil {
		return errs.Wrap(err)
	}

	// generate random nonce for encrypting the content key
	_, err = rand.Read(encryptedKeyNonce[:])
	if err != nil {
		return errs.Wrap(err)
	}

	encryptedKey, err := encryption.EncryptKey(&contentKey, k.Cipher, &storj.Key{}, &encryptedKeyNonce)
	if err != nil {
		return errs.Wrap(err)
	}

	err = k.Output.EncryptionParams(ctx, EncryptionParams{
		contentKey:        &contentKey,
		encryptedKeyNonce: &contentNonce,
		encryptedKey:      &encryptedKey,
		cipher:            k.Cipher,
	})
	if err != nil {
		return err
	}
	return k.Output.StartSegment(ctx, index)
}

func (k KeyDerivation) EndSegment(ctx context.Context) error {
	return k.Output.EndSegment(ctx)
}

func (k KeyDerivation) EndObject(ctx context.Context) error {
	return k.Output.EndObject(ctx)
}

func (k KeyDerivation) Write(ctx context.Context, bytes []byte) error {
	return k.Output.Write(ctx, bytes)
}

func (k KeyDerivation) Commit(ctx context.Context) error {
	return k.Output.Commit(ctx)

}

func (k KeyDerivation) StartObject(ctx context.Context, info *StartObject) error {
	return k.Output.StartObject(ctx, info)
}
