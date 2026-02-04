// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package license

import (
	"context"
	"time"
	_ "unsafe" // for go:linkname

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/uplink"
	"storj.io/uplink/private/metaclient"
)

var mon = monkit.Package()

// Entry contains license information.
type Entry struct {
	Type      string
	ExpiresAt time.Time
	Key       []byte
}

// List returns license information for the given bucket.
func List(ctx context.Context, project *uplink.Project, licenseType, bucketName string) (licenses []Entry, err error) {
	defer mon.Task()(&ctx)(&err)

	client, err := dialMetainfoClient(ctx, project)
	if err != nil {
		return nil, convertKnownErrors(err, bucketName, "")
	}
	defer func() { err = errs.Combine(err, client.Close()) }()

	rawLicenses, err := client.AccountLicenses(ctx, metaclient.AccountLicensesRequest{
		Type:       licenseType,
		BucketName: bucketName,
	})
	if err != nil {
		return nil, convertKnownErrors(err, bucketName, "")
	}

	for _, rawLicense := range rawLicenses {
		licenses = append(licenses, Entry{
			Type:      rawLicense.Type,
			ExpiresAt: rawLicense.ExpiresAt,
			Key:       rawLicense.Key,
		})
	}

	return licenses, nil
}

//go:linkname convertKnownErrors storj.io/uplink.convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error

//go:linkname dialMetainfoClient storj.io/uplink.dialMetainfoClient
func dialMetainfoClient(ctx context.Context, project *uplink.Project) (_ *metaclient.Client, err error)
