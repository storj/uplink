// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package splitter

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"storj.io/uplink/private/storage/streams/buffer"
)

// TestFinishDeadlockWithBlockedWriter attempts to reproduce the finding in
// splitter-finish-write-deadlock.md: a producer in Write is blocked in
// Cursor.WaitWrite backpressure while holding encryptedBuffer.mu; a
// concurrent Finish call tries to acquire e.mu via DoneWriting and deadlocks.
//
// We drive baseSplitter directly, because the code path goes through
// encryptedBuffer -> buffer.Buffer -> cursor.WaitWrite.
func TestFinishDeadlockWithBlockedWriter(t *testing.T) {
	const (
		split   = 1 << 20 // 1 MiB
		minimum = 1024    // so writeAhead is small
	)

	bs, err := newBaseSplitter(split, minimum)
	if err != nil {
		t.Fatal(err)
	}

	// Provide a segment via Next so that baseSplitter has a current buffer
	// that the producer can write to. The reader never drains it, so the
	// producer will eventually block in Cursor.WaitWrite once it writes past
	// writeAhead bytes.
	buf := buffer.New(buffer.NewMemoryBackend(split), minimum)
	encBuf := newEncryptedBuffer(buf, nopCloser{buf})

	nextDone := make(chan struct{})
	go func() {
		defer close(nextDone)
		ctx := context.Background()
		_, _, err := bs.Next(ctx, encBuf)
		if err != nil {
			t.Log("Next err:", err)
		}
	}()

	// Start a producer goroutine that writes a large amount. The first
	// `minimum` bytes go into temp, then the rest gets written to encBuf,
	// eventually blocking in WaitWrite once we exceed minimum bytes ahead
	// of the (non-existent) reader.
	writeDone := make(chan error, 1)
	go func() {
		_, err := bs.Write(make([]byte, split))
		writeDone <- err
	}()

	// Wait until Next has been consumed so we know the producer is past
	// the Next handshake and writing to encBuf.
	<-nextDone

	// Wait until the producer is actually parked in Cursor.WaitWrite
	// backpressure. Polling the goroutine dump is more reliable than
	// sleeping a fixed duration.
	waitForGoroutineBlockedIn(t, "buffer.(*Cursor).WaitWrite", 5*time.Second)

	// Now call Finish from a third goroutine. The finding says this will
	// deadlock because Finish needs e.mu which the producer holds.
	finishDone := make(chan struct{})
	go func() {
		bs.Finish(errors.New("aborted"))
		close(finishDone)
	}()

	select {
	case <-finishDone:
		// good, Finish returned
	case <-time.After(5 * time.Second):
		dumpGoroutines(t)
		t.Fatal("Finish deadlocked: did not return within 5s")
	}

	select {
	case werr := <-writeDone:
		t.Log("Write returned err:", werr)
	case <-time.After(5 * time.Second):
		dumpGoroutines(t)
		t.Fatal("Write did not return within 5s after Finish")
	}
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

func dumpGoroutines(t *testing.T) {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	stacks := string(buf[:n])
	for _, block := range strings.Split(stacks, "\n\n") {
		t.Log(block)
	}
}

// waitForGoroutineBlockedIn polls runtime.Stack until some goroutine's stack
// contains needle, or until timeout elapses. On timeout the full goroutine
// dump is logged and the test fails.
func waitForGoroutineBlockedIn(t *testing.T, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if strings.Contains(string(buf[:n]), needle) {
			return
		}
		if time.Now().After(deadline) {
			dumpGoroutines(t)
			t.Fatalf("no goroutine reached %q within %s", needle, timeout)
		}
		time.Sleep(time.Millisecond)
	}
}
