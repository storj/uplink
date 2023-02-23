// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package scheduler

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/sync2"
)

// TestScheduler_Priority checks that earlier handles get priority when handing out resources.
func TestScheduler_Priority(t *testing.T) {
	ctx := context.Background()

	s := New(Options{MaximumConcurrent: 3})

	h1 := s.Join()
	h2 := s.Join()
	h3 := s.Join()

	_, _ = h1.Get(ctx)
	_, _ = h1.Get(ctx)

	var counts [3]int
	for i := 0; i < 1000; i++ {
		func() {
			r, ok := h1.Get(ctx)
			require.True(t, ok)

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			var got Resource

			track := func(h Handle, n int) func() {
				return func() {
					r, ok := h.Get(ctx)
					if ok {
						counts[n]++
						cancel()
						got = r
					}
				}
			}

			// try to acquire the resource with all three handles.
			wait := concurrently(
				track(h1, 0),
				track(h2, 1),
				track(h3, 2),
			)

			// wait for them to all be in the queue to acquire the resource.
			for s.numWaiters() != 3 {
				runtime.Gosched()
			}

			// release the resource to allow one of them to win.
			r.Done()

			// wait for the acquire to happen.
			wait()

			// release the newly acquired resource.
			got.Done()
		}()
	}

	// this should determinisitcally always give to the first resource
	t.Log(counts)
	require.Equal(t, [...]int{1000, 0, 0}, counts)
}

func concurrently(fns ...func()) func() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rng.Shuffle(len(fns), func(i, j int) { fns[i], fns[j] = fns[j], fns[i] })
	return sync2.Go(fns...)
}

// TestScheduler_Limits checks that the configured limits are respected.
func TestScheduler_Limits(t *testing.T) {
	seed := time.Now().UnixNano()
	ctx := context.Background()
	concurrent := int64(0)
	max := int64(0)

	updateMax := func(c int64) {
		for {
			m := atomic.LoadInt64(&max)
			if c <= m {
				return
			}
			if atomic.CompareAndSwapInt64(&max, m, c) {
				return
			}
		}
	}

	const (
		maxConcurrent = 10
		numHandles    = 100
		numResources  = 100
	)

	s := New(Options{MaximumConcurrent: maxConcurrent})

	var counts [maxConcurrent]int64
	var wg sync.WaitGroup
	for i := 0; i < numHandles; i++ {
		i := i

		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + int64(i)))

			h := s.Join()
			defer h.Done()

			held := make([]Resource, 0, maxConcurrent)
			done := func() {
				if len(held) > 0 {
					atomic.AddInt64(&concurrent, -1)
					held[len(held)-1].Done()
					held = held[:len(held)-1]
				}
			}
			defer func() {
				for len(held) > 0 {
					done()
				}
			}()

			for j := 0; j < numResources; j++ {
				if t.Failed() {
					break
				}

				for rng.Intn(3) == 0 {
					done()
				}
				for len(held) > 0 && atomic.LoadInt64(&concurrent) == maxConcurrent {
					done()
				}

				r, ok := h.Get(ctx)
				if !ok {
					t.Error("Unable to get resource")
					break
				}
				held = append(held, r)

				c := atomic.AddInt64(&concurrent, 1)
				updateMax(c)
				if c > maxConcurrent {
					t.Error("maximum concurrent:", c)
					break
				}
				atomic.AddInt64(&counts[c-1], 1)
			}
		}()
	}
	wg.Wait()

	t.Log("observed max:", max)
	t.Log("histogram:", counts)

	require.LessOrEqual(t, max, int64(maxConcurrent))
}

func BenchmarkScheduler_Single(b *testing.B) {
	ctx := context.Background()

	s := New(Options{MaximumConcurrent: 1})

	h := s.Join()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if r, ok := h.Get(ctx); ok {
			r.Done()
		}
	}
}

func BenchmarkScheduler_Parallel(b *testing.B) {
	ctx := context.Background()

	s := New(Options{MaximumConcurrent: runtime.GOMAXPROCS(-1)})

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		h := s.Join()
		defer h.Done()

		for pb.Next() {
			if r, ok := h.Get(ctx); ok {
				r.Done()
			}
		}
	})
}
