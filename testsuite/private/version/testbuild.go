// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

// +build ignore

package main

import (
	"fmt"
	"os"

	"storj.io/uplink/private/version"
)

func main() {
	useragent := os.Args[1]
	useragentStr, err := version.AppendVersionToUserAgent(useragent)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	fmt.Printf("%#v", useragentStr)
}
