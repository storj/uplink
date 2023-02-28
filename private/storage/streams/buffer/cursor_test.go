// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package buffer

import (
	"runtime"
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
}

func BenchmarkNewCursor(b *testing.B) {
	b.ReportAllocs()

	var c *Cursor
	for i := 0; i < b.N; i++ {
		c = NewCursor(1024)
	}
	runtime.KeepAlive(c)
}
