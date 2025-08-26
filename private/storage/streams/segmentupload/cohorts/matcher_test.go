// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package cohorts

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/mwc"

	"storj.io/common/pb"
)

func TestMatcher_Increment_Literal(t *testing.T) {
	tags := []map[string]string{
		{"dc": "dc-1"},
		{"dc": "dc-1"},
		{"dc": "dc-2"},
	}

	m := NewMatcher(makeLit(2), len(tags))

	// First increment should not satisfy
	require.False(t, m.Increment(tags[0], 0))

	// Second increment should satisfy
	require.True(t, m.Increment(tags[1], 1))
}

func TestMatcher_Increment_And(t *testing.T) {
	tags := []map[string]string{
		{"dc": "dc-1"},
		{"dc": "dc-1"},
		{"dc": "dc-2"},
		{"dc": "dc-2"},
	}

	m := NewMatcher(makeAnd(makeLit(2), makeLit(3)), len(tags))

	// Need at least 3 to satisfy both requirements
	require.False(t, m.Increment(tags[0], 0))
	require.False(t, m.Increment(tags[1], 1))
	require.True(t, m.Increment(tags[2], 2))
}

func TestMatcher_Increment_Withhold(t *testing.T) {
	tags := []map[string]string{
		{"dc": "dc-1"},
		{"dc": "dc-1"},
		{"dc": "dc-1"},
		{"dc": "dc-2"},
		{"dc": "dc-2"},
	}

	// Need 3 total, but withhold 1 dc (the top one)
	m := NewMatcher(makeWithhold("dc", 1, makeLit(2)), len(tags))

	// Add 3 from dc-1 (top dc will be withheld)
	require.False(t, m.Increment(tags[0], 0))
	require.False(t, m.Increment(tags[1], 1))
	require.False(t, m.Increment(tags[2], 2))

	// Add 2 from dc-2, now we have enough after withholding dc-1
	require.False(t, m.Increment(tags[3], 3))
	require.True(t, m.Increment(tags[4], 4))
}

func TestMatcher_Increment_SameNode(t *testing.T) {
	tags := []map[string]string{
		{"dc": "dc-1"},
		{"dc": "dc-1"},
	}

	m := NewMatcher(makeLit(2), len(tags))

	// First increment should not satisfy
	require.False(t, m.Increment(tags[0], 0))

	// Same node again should still not be satisfied
	require.False(t, m.Increment(tags[0], 0))
}

func TestMatcher_Empty_Requirements(t *testing.T) {
	tags := []map[string]string{
		{"dc": "dc-1"},
	}

	{ // Empty requirements should never be satisfied
		m := NewMatcher(&pb.CohortRequirements{}, len(tags))
		require.True(t, m.Increment(tags[0], 0))
	}

	{ // Nil requirements should always be satisfied
		m := NewMatcher(nil, len(tags))
		require.True(t, m.Increment(tags[0], 0))
	}
}

func BenchmarkMatcher_Valid(b *testing.B) {
	const (
		numDC    byte = 3
		numRack  byte = 10
		numNodes byte = 4

		totalNodes = int(numDC * numRack * numNodes)
	)

	var tags []map[string]string
	for dc := range numDC {
		for rack := range numRack {
			for range numNodes {
				tags = append(tags, map[string]string{
					"dc":   fmt.Sprintf("dc-%d", dc),
					"rack": fmt.Sprintf("dc-%d-rack-%d", dc, rack),
				})
			}
		}
	}

	b.ReportAllocs()

loop:
	for i := 0; i < b.N; i++ {
		m := NewMatcher(makeAnd(
			makeWithhold("dc", 1, makeWithhold("rack", 3, makeLit(28))),
			makeLit(49),
		), totalNodes)

		for range 1000 {
			n := byte(mwc.Intn(totalNodes))
			if !m.successes.has(n) && m.Increment(tags[n], n) {
				continue loop
			}
		}

		b.Fatal("failed to satisfy")
	}
}

//
// helpers
//

func makeAnd(reqs ...*pb.CohortRequirements) *pb.CohortRequirements {
	return &pb.CohortRequirements{
		Requirement: &pb.CohortRequirements_And_{
			And: &pb.CohortRequirements_And{
				Requirements: reqs,
			},
		},
	}
}

func makeLit(n int) *pb.CohortRequirements {
	return &pb.CohortRequirements{
		Requirement: &pb.CohortRequirements_Literal_{
			Literal: &pb.CohortRequirements_Literal{
				Value: int32(n),
			},
		},
	}
}

func makeWithhold(key string, n int, child *pb.CohortRequirements) *pb.CohortRequirements {
	return &pb.CohortRequirements{
		Requirement: &pb.CohortRequirements_Withhold_{
			Withhold: &pb.CohortRequirements_Withhold{
				TagKey: key,
				Amount: int32(n),
				Child:  child,
			},
		},
	}
}
