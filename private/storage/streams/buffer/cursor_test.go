// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package buffer

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"
)

func TestCursor(t *testing.T) {
	type result struct {
		n   int64
		ok  bool
		err error
	}
	wrap := func(n int64, ok bool, err error) result { return result{n, ok, err} }
	canceled := errs.New("canceled")

	t.Run("ReadBlocksUntilFinished", func(t *testing.T) {
		done := make(chan result)
		cursor := NewCursor(10)
		go func() { done <- wrap(cursor.WaitRead(1)) }()
		runtime.Gosched() // attempt to cause the goroutine to run
		cursor.DoneWriting(nil)
		require.Equal(t, result{0, false, nil}, <-done)
	})

	t.Run("ReadBlocksUntilFinished_With_Error", func(t *testing.T) {
		done := make(chan result)
		cursor := NewCursor(10)
		go func() { done <- wrap(cursor.WaitRead(1)) }()
		runtime.Gosched() // attempt to cause the goroutine to run
		cursor.DoneWriting(canceled)
		require.Equal(t, result{0, false, canceled}, <-done)
	})

	t.Run("WriteBlocksUntilFinished_With_Error", func(t *testing.T) {
		done := make(chan result)
		cursor := NewCursor(10)
		cursor.WroteTo(10)
		go func() { done <- wrap(cursor.WaitWrite(11)) }()
		runtime.Gosched() // attempt to cause the goroutine to run
		cursor.DoneReading(canceled)
		require.Equal(t, result{0, false, canceled}, <-done)
	})

	t.Run("WriteBlocksUntilFinished", func(t *testing.T) {
		done := make(chan result)
		cursor := NewCursor(10)
		cursor.WroteTo(10)
		go func() { done <- wrap(cursor.WaitWrite(11)) }()
		runtime.Gosched() // attempt to cause the goroutine to run
		cursor.DoneReading(nil)
		require.Equal(t, result{10, false, nil}, <-done)
	})

	t.Run("ReadAllWritten", func(t *testing.T) {
		cursor := NewCursor(10)
		cursor.WroteTo(1)
		cursor.DoneWriting(nil)
		require.Equal(t, result{1, false, nil}, wrap(cursor.WaitRead(1)))
		require.Equal(t, result{1, false, nil}, wrap(cursor.WaitRead(2)))
	})

	t.Run("WriteUnblocksRead", func(t *testing.T) {
		done := make(chan result)
		cursor := NewCursor(10)
		go func() { done <- wrap(cursor.WaitRead(1)) }()
		runtime.Gosched() // attempt to cause the goroutine to run
		cursor.WroteTo(1)
		require.Equal(t, result{1, true, nil}, <-done)
	})

	t.Run("ReadUnblocksWrite", func(t *testing.T) {
		done := make(chan result)
		cursor := NewCursor(10)
		cursor.WroteTo(10)
		go func() { done <- wrap(cursor.WaitWrite(11)) }()
		runtime.Gosched() // attempt to cause the goroutine to run
		cursor.ReadTo(1)
		require.Equal(t, result{11, true, nil}, <-done)
	})

	// ReadTo and WroteTo only take the lock when a waiter has registered
	// itself. Hammer on that handoff from both sides to shake out a lost
	// wakeup, which would otherwise show up as a deadlock.
	t.Run("NoLostWakeups", func(t *testing.T) {
		const rounds = 2000

		for _, readers := range []int{1, 4, 16} {
			t.Run(fmt.Sprintf("Readers%d", readers), func(t *testing.T) {
				cursor := NewCursor(1)

				var wg sync.WaitGroup
				for i := 0; i < readers; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for at := int64(0); at < rounds; {
							m, ok, err := cursor.WaitRead(at + 1)
							require.NoError(t, err)
							if !ok {
								return
							}
							at = m
							cursor.ReadTo(at)
						}
					}()
				}

				for at := int64(0); at < rounds; {
					m, ok, err := cursor.WaitWrite(at + 1)
					require.NoError(t, err)
					require.True(t, ok)
					at = m
					cursor.WroteTo(at)
				}
				cursor.DoneWriting(nil)

				wg.Wait()
			})
		}
	})
}

func BenchmarkNewCursor(b *testing.B) {
	b.ReportAllocs()

	var c *Cursor
	for i := 0; i < b.N; i++ {
		c = NewCursor(1024)
	}
	runtime.KeepAlive(c)
}

// BenchmarkCursorFanOut models a segment upload: one writer feeding the number
// of readers an erasure share count implies. b.N counts writer steps, so the
// per-op cost should stay flat as the reader count grows.
//
// Starved is the regime observed in production, where the writer is the
// bottleneck and the piece readers spend their time waiting on it. Backpressured
// is the inverse, with the readers keeping the writer pinned against writeAhead.
func BenchmarkCursorFanOut(b *testing.B) {
	regimes := []struct {
		name       string
		writeAhead int64
	}{
		{"Starved", 1 << 20},
		{"Backpressured", 4},
	}

	for _, regime := range regimes {
		for _, readers := range []int{1, 16, 80, 110} {
			b.Run(fmt.Sprintf("%s/Readers%d", regime.name, readers), func(b *testing.B) {
				cursor := NewCursor(regime.writeAhead)

				var wg sync.WaitGroup
				for i := 0; i < readers; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for at := int64(0); ; {
							m, ok, err := cursor.WaitRead(at + 1)
							if err != nil || !ok {
								return
							}
							at = m
							cursor.ReadTo(at)
						}
					}()
				}

				b.ResetTimer()
				for at := int64(0); at < int64(b.N); {
					m, _, err := cursor.WaitWrite(at + 1)
					if err != nil {
						b.Fatal(err)
					}
					at = m
					cursor.WroteTo(at)
				}
				b.StopTimer()

				cursor.DoneWriting(nil)
				wg.Wait()
			})
		}
	}
}
