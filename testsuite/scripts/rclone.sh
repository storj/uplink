#!/usr/bin/env bash
set -ueo pipefail
set +x

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $SCRIPTDIR

# setup tmpdir for testfiles and cleanup
TMP=$(mktemp -d -t tmp.XXXXXXXXXX)
cleanup(){
    if [ -f "$TMP"/docker-compose.yaml ]
    then
      docker compose -f "$TMP"/docker-compose.yaml down
    fi
  rm -rf "$TMP"
}
trap cleanup EXIT

STORJ_VERSION=$(go list -modfile "../go.mod" -m -f "{{.Version}}" storj.io/storj)
COMMIT_HASH="${STORJ_VERSION##*-}"

go install storj.io/storj-up@main

echo "Used storj version: $STORJ_VERSION"

cd "$TMP"
storj-up init minimal,db > /dev/null 2>&1
storj-up build -s remote github satellite-api -c "$COMMIT_HASH"
storj-up build -s remote github storagenode -c "$COMMIT_HASH"
storj-up port remove cockroach 26257
storj-up port add cockroach 26257 -e 26666

# start the services
docker compose build
docker compose up -d
storj-up health -d 60 -p 26666
eval $(storj-up credentials -e)

git clone https://github.com/rclone/rclone
RCLONE=$TMP/rclone

pushd $RCLONE
    git fetch --tags
    latest_version=$(git tag -l --sort -version:refname | head -1)
    git checkout $latest_version

    go mod edit -replace storj.io/uplink=$SCRIPTDIR/../../
    go mod tidy -compat=1.17

    go build -mod=mod ./fstest/test_all
    go build -mod=mod

    ./rclone config create TestStorj storj access_grant $UPLINK_ACCESS

    ./test_all -backends storj -output $SCRIPTDIR/../../.build/rclone-integration-tests -config $SCRIPTDIR/config.yaml
popd