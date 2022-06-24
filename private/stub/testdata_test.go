package stub

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/vivint/infectious"
	"github.com/zeebo/errs"
	"io"
	"storj.io/common/encryption"
	"storj.io/common/storj"
	"testing"
)

func TestRun(t *testing.T) {
	res, err := GenerateData(10)
	require.NoError(t, err)
	for k, v := range res {
		fmt.Println(k)
		fmt.Println(len(v))
		fmt.Println(hex.EncodeToString(v))
	}
}
func GenerateData(size int) (map[int][]byte, error) {
	contentKey := storj.Key{}
	nonce := storj.Nonce{}

	encrypter, err := encryption.NewEncrypter(storj.EncAESGCM, &contentKey, &nonce, 256)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	testdata := &BufferWithClose{
		buffer: bytes.NewBuffer(make([]byte, size)),
	}
	paddedReader := encryption.PadReader(testdata, encrypter.InBlockSize())
	transformedReader := encryption.TransformReader(paddedReader, encrypter, 0)
	encrypted := bytes.NewBuffer([]byte{})
	_, err = io.Copy(encrypted, transformedReader)
	if err != nil {
		return nil, err
	}
	fmt.Println(hex.EncodeToString(encrypted.Bytes()))
	fec, err := infectious.NewFEC(4, 6)
	if err != nil {
		return nil, err
	}
	res := map[int][]byte{}
	err = fec.Encode(encrypted.Bytes(), func(share infectious.Share) {
		res[share.Number] = share.Data
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
