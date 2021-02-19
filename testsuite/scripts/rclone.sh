#!/usr/bin/env bash
set -ueo pipefail

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# setup tmpdir for testfiles and cleanup
TMP=$(mktemp -d -t tmp.XXXXXXXXXX)
cleanup(){
	rm -rf "$TMP"
}
trap cleanup EXIT

cd $TMP; git clone https://github.com/rclone/rclone
RCLONE=$TMP/rclone

pushd $RCLONE
    git fetch --tags
    latest_version=$(git tag -l --sort -version:refname | head -1)
    git checkout $latest_version

    go mod edit -replace storj.io/uplink=$SCRIPTDIR/../../
    go mod tidy

    go build -mod=mod ./fstest/test_all
    go build -mod=mod

    ./rclone config create TestTardigrade tardigrade access_grant $GATEWAY_0_ACCESS

    ./test_all -backends tardigrade
popd