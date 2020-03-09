// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package piecestore

import (
	"sync"

	"storj.io/common/pb"
)

// LockingDownload adds a lock around download making it safe to use concurrently.
// TODO: this shouldn't be needed.
type LockingDownload struct {
	mu       sync.Mutex
	download Downloader
}

// Read downloads content.
func (download *LockingDownload) Read(p []byte) (int, error) {
	download.mu.Lock()
	defer download.mu.Unlock()
	return download.download.Read(p)
}

// Close closes the deownload.
func (download *LockingDownload) Close() error {
	download.mu.Lock()
	defer download.mu.Unlock()
	return download.download.Close()
}

// GetHashAndLimit gets the download's hash and original order limit.
func (download *LockingDownload) GetHashAndLimit() (*pb.PieceHash, *pb.OrderLimit) {
	return download.download.GetHashAndLimit()
}
