// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package edge

import (
	"net/url"
	"strings"
)

// JoinShareURL creates a linksharing URL from parts. The existence or accessibility of the target
// is not checked, it might not exist or be inaccessible.
//
// Example result is https://link.us1.storjshare.io/s/l5pucy3dmvzxgs3fpfewix27l5pq/mybucket/myprefix/myobject
//
// The baseURL is the url of the linksharing service, e.g. https://link.us1.storjshare.io. The accessKeyID
// can be obtained by calling RegisterAccess. It must be associated with public visibility.
// The bucket is optional, leave it blank to share the entire project. The object key is also optional,
// if empty shares the entire bucket. It can also be a prefix, in which case it must end with a "/".
func JoinShareURL(baseURL string, accessKeyID string, bucket string, key string) (string, error) {
	if accessKeyID == "" {
		return "", uplinkError.New("accessKeyID is required")
	}

	if bucket == "" && key != "" {
		return "", uplinkError.New("bucket is required if key is specified")
	}

	result, err := url.ParseRequestURI(baseURL)
	if err != nil {
		return "", uplinkError.New("invalid base url: %q", baseURL)
	}

	result.Path = strings.Trim(result.Path, "/")
	result.Path += "/s/"
	result.Path += accessKeyID

	if bucket != "" {
		result.Path += "/" + bucket
	}

	if key != "" {
		result.Path += "/" + key
	}

	return result.String(), nil
}
