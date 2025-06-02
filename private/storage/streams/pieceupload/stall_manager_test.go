// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package pieceupload

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStallManager_SetMaxDuration(t *testing.T) {
	t.Run("multiple calls only sets duration once", func(t *testing.T) {
		manager := NewStallManager()

		// First call should set the duration
		manager.SetMaxDuration(100 * time.Millisecond)

		// Second call should be ignored
		manager.SetMaxDuration(500 * time.Millisecond)

		// Verify the first duration is preserved
		ctx, cleanup := manager.Watch(context.Background())
		defer cleanup()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		// Should be approximately 100ms from now, not 500ms
		require.True(t, time.Until(deadline) <= 100*time.Millisecond)
		require.True(t, time.Until(deadline) > 50*time.Millisecond) // some tolerance
	})

	t.Run("zero duration is ignored", func(t *testing.T) {
		manager := NewStallManager()

		// Zero duration should be ignored
		manager.SetMaxDuration(0)

		// Valid duration should still work
		manager.SetMaxDuration(100 * time.Millisecond)

		ctx, cleanup := manager.Watch(context.Background())
		defer cleanup()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.True(t, time.Until(deadline) <= 100*time.Millisecond)
	})

	t.Run("negative duration is ignored", func(t *testing.T) {
		manager := NewStallManager()

		// Negative duration should be ignored
		manager.SetMaxDuration(-100 * time.Millisecond)

		// Valid duration should still work
		manager.SetMaxDuration(100 * time.Millisecond)

		ctx, cleanup := manager.Watch(context.Background())
		defer cleanup()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.True(t, time.Until(deadline) <= 100*time.Millisecond)
	})

	t.Run("concurrent calls are safe", func(t *testing.T) {
		manager := NewStallManager()

		// Launch multiple goroutines trying to set different durations
		var wg sync.WaitGroup
		durations := []time.Duration{
			100 * time.Millisecond,
			200 * time.Millisecond,
			300 * time.Millisecond,
			400 * time.Millisecond,
			500 * time.Millisecond,
		}

		for _, duration := range durations {
			wg.Add(1)
			go func(d time.Duration) {
				defer wg.Done()
				manager.SetMaxDuration(d)
			}(duration)
		}

		wg.Wait()

		// Verify exactly one duration was set (should be one of the durations above)
		ctx, cleanup := manager.Watch(context.Background())
		defer cleanup()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)

		timeUntil := time.Until(deadline)
		// Should be one of our test durations (with some tolerance)
		var found bool
		for _, d := range durations {
			if timeUntil <= d && timeUntil > d-50*time.Millisecond {
				found = true
				break
			}
		}
		require.True(t, found, "duration should be one of the test values, got %v", timeUntil)
	})
}

func TestStallManager_Watch(t *testing.T) {
	t.Run("with pre-set duration", func(t *testing.T) {
		manager := NewStallManager()
		manager.SetMaxDuration(100 * time.Millisecond)

		ctx, cleanup := manager.Watch(context.Background())
		defer cleanup()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.True(t, time.Until(deadline) <= 100*time.Millisecond)

		// Context should timeout
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
			t.Fatal("context should have timed out")
		}
	})

	t.Run("with dynamic duration set after watch", func(t *testing.T) {
		manager := NewStallManager()

		ctx, cleanup := manager.Watch(context.Background())
		defer cleanup()

		// Initially no deadline
		_, ok := ctx.Deadline()
		require.False(t, ok)

		// Set duration after watch starts
		go func() {
			time.Sleep(50 * time.Millisecond)
			manager.SetMaxDuration(100 * time.Millisecond)
		}()

		// Context should timeout after duration is set
		start := time.Now()
		select {
		case <-ctx.Done():
			elapsed := time.Since(start)
			require.True(t, elapsed >= 100*time.Millisecond && elapsed < 200*time.Millisecond)
		case <-time.After(300 * time.Millisecond):
			t.Fatal("context should have timed out")
		}
	})

	t.Run("cleanup before timeout", func(t *testing.T) {
		manager := NewStallManager()
		manager.SetMaxDuration(200 * time.Millisecond)

		ctx, cleanup := manager.Watch(context.Background())

		// Cleanup early
		go func() {
			time.Sleep(50 * time.Millisecond)
			cleanup()
		}()

		select {
		case <-ctx.Done():
			// Should be cancelled due to cleanup, not timeout
		case <-time.After(300 * time.Millisecond):
			t.Fatal("context should have been cancelled by cleanup")
		}
	})

	t.Run("duration already elapsed when set", func(t *testing.T) {
		manager := NewStallManager()

		ctx, cleanup := manager.Watch(context.Background())
		defer cleanup()

		// Wait some time then set a duration that's already elapsed
		time.Sleep(100 * time.Millisecond)
		manager.SetMaxDuration(50 * time.Millisecond)

		// Context should be cancelled immediately
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("context should have been cancelled immediately")
		}
	})

	t.Run("multiple calls to cleanup are safe", func(t *testing.T) {
		manager := NewStallManager()
		manager.SetMaxDuration(100 * time.Millisecond)

		ctx, cleanup := manager.Watch(context.Background())

		// Call cleanup multiple times
		cleanup()
		cleanup()
		cleanup()

		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
			t.Fatal("context should have been cancelled")
		}
	})

	t.Run("zero duration is ignored", func(t *testing.T) {
		manager := NewStallManager()
		manager.SetMaxDuration(0)

		ctx, cleanup := manager.Watch(context.Background())
		defer cleanup()

		// Should not have a deadline since zero duration is ignored
		_, ok := ctx.Deadline()
		require.False(t, ok)
	})
}
