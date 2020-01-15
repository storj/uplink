#!/usr/bin/env bash
set -ueo pipefail
set +x

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

go test -parallel 4 -p 6 -vet=off -timeout 20m -race -v ./...

cd $SCRIPTDIR/../testsuite

go test -parallel 4 -p 6 -vet=off -timeout 20m -race -v ./...
