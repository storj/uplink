// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package cohorts

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
)

func TestBitsetIterate(t *testing.T) {
	for range 100 {
		var bs bitset
		set := make(map[byte]bool)

		for range mwc.Intn(50) {
			n := byte(mwc.Intn(256))
			bs.set(n)
			set[n] = true
		}

		for x := range bs.iter {
			assert.That(t, set[x])
			delete(set, x)
		}

		assert.That(t, len(set) == 0)
	}
}
