// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package stream

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaybeSatStreamID(t *testing.T) {
	bytes, err := hex.DecodeString("0a0c776f726b70726573656e636512500228268f45727e7d86179d61804cb6acf07ae9369e05a179e44098de47264557394dab86888d97312f029ade9c64042c64eec1f63e8620886025d122732350b26275c83efaa9eb43cef60c6c4c15b81d18012a0c08d2f4eca70610a8aa87df01320b088092b8c398feffffff014a20888741879f0b2c359287593df43570c25cadf3a20a17d430a97e509cedf4cd1f521088e24231f6b34ff5a5a0de8e530825996205080210803a")
	if err != nil {
		t.Fatal("failed decoding hex?")
	}

	require.Equal(t, hex.EncodeToString(maybeSatStreamID(bytes)), "88e24231f6b34ff5a5a0de8e53082599")

	bytes, err = hex.DecodeString("0a0c776f726b70726573656e636512500228268f45727e7d86179d61804cb6acf07ae9369e05a179e44098de47264557394dab86888d97312f029ade9c64042c64eec1f63e8620886025d122732350b26275c83efaa9eb43cef60c6c4c15b81d18012a0c08d2f4eca70610a8aa87df01320b088092b8c398feffffff014a20888741879f0b2c359287593df43570c25cadf3a20a17d430a97e509cedcd1f521088e24231f6b34ff5a5a0de8e530825996205080210803a")
	if err != nil {
		t.Fatal("failed decoding hex?")
	}

	require.Equal(t, hex.EncodeToString(maybeSatStreamID(bytes)), "")

	bytes, err = hex.DecodeString("000c776f726b70726573656e636512500228268f45727e7d86179d61804cb6acf07ae9369e05a179e44098de47264557394dab86888d97312f029ade9c64042c64eec1f63e8620886025d122732350b26275c83efaa9eb43cef60c6c4c15b81d18012a0c08d2f4eca70610a8aa87df01320b088092b8c398feffffff014a20888741879f0b2c359287593df43570c25cadf3a20a17d430a97e509cedf4cd1f521088e24231f6b34ff5a5a0de8e530825996205080210803a")
	if err != nil {
		t.Fatal("failed decoding hex?")
	}

	require.Equal(t, hex.EncodeToString(maybeSatStreamID(bytes)), "")
}
