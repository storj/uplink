// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package pieceupload

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"storj.io/common/time2"
)

// StallManager is a type that helps manage stalls.
type StallManager struct {
	durationSetClose sync.Once
	durationSet      chan struct{}

	duration atomic.Int64
}

// NewStallManager makes a new stall manager.
func NewStallManager() *StallManager {
	return &StallManager{
		durationSet: make(chan struct{}),
	}
}

// SetMaxDuration sets the max duration any child context is allowed to go.
func (manager *StallManager) SetMaxDuration(duration time.Duration) {
	if duration > 0 {
		manager.duration.Store(int64(duration))
	}
	manager.durationSetClose.Do(func() {
		close(manager.durationSet)
	})
}

// Watch makes sure the current context doesn't go longer than SetMaxDuration.
// If no SetMaxDuration is set, it watches anyway, in case SetMaxDuration is
// called later.
// cleanup should be called when the context is no longer needed.
func (manager *StallManager) Watch(parent context.Context) (ctx context.Context, cleanup func()) {
	if duration := time.Duration(manager.duration.Load()); duration > 0 {
		return context.WithTimeout(parent, duration)
	}

	start := time2.Now(parent)
	ctx, cancel := context.WithCancel(parent)
	cancelCh := make(chan struct{})
	var cancelChClose sync.Once

	go func() {
		select {
		case <-cancelCh:
			return
		case <-manager.durationSet:
		}
		duration := time.Duration(manager.duration.Load())
		elapsed := time2.Since(parent, start)
		if elapsed >= duration {
			cancel()
			return
		}
		remaining := duration - elapsed
		timer := time2.NewTimer(parent, remaining)
		defer timer.Stop()

		select {
		case <-cancelCh:
			return
		case <-timer.Chan():
			cancel()
			return
		}
	}()

	return ctx, func() {
		cancelChClose.Do(func() {
			close(cancelCh)
			cancel()
		})
	}
}
