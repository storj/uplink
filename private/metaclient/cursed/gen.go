// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

//go:build ignore

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Programs needed for code generation:
//
// storj.io/picobuf/protoc-gen-pico

func main() {
	commonPb := os.Getenv("STORJ_COMMON_PB")
	if commonPb == "" {
		commonPb = "../../../../common/pb"
	}

	cmd := exec.Command("protoc",
		"--lint_out=.",
		"--pico_out=field_access=true,paths=source_relative:.",
		"-I=.",
		"-I="+commonPb,
		"cursed.proto",
	)
	fmt.Println(strings.Join(cmd.Args, " "))
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		fmt.Println(string(out))
	}
	if err != nil {
		panic(err)
	}
}
