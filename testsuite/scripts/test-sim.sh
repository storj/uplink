#!/usr/bin/env bash
set -ueo pipefail
set +x

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $SCRIPTDIR

# setup tmpdir for testfiles and cleanup
TMP=$(mktemp -d -t tmp.XXXXXXXXXX)
cleanup(){
	rm -rf "$TMP"
}
trap cleanup EXIT

STORJ_GO_MOD=${STORJ_GO_MOD:-"../go.mod"}
VERSION=$(go list -modfile $STORJ_GO_MOD -m -f "{{.Version}}" storj.io/storj)


go install storj.io/storj/cmd/certificates@$VERSION
go install storj.io/storj/cmd/identity@$VERSION
go install storj.io/storj/cmd/satellite@$VERSION
go install storj.io/storj/cmd/storagenode@$VERSION
go install storj.io/storj/cmd/versioncontrol@$VERSION
go install storj.io/storj/cmd/storj-sim@$VERSION
go install storj.io/storj/cmd/multinode@$VERSION
go install storj.io/gateway@latest

echo "Used version: $VERSION"

export STORJ_NETWORK_DIR=$TMP

STORJ_NETWORK_HOST4=${STORJ_NETWORK_HOST4:-127.0.0.1}
STORJ_SIM_POSTGRES=${STORJ_SIM_POSTGRES:-""}

# setup the network
# if postgres connection string is set as STORJ_SIM_POSTGRES then use that for testing
if [ -z ${STORJ_SIM_POSTGRES} ]; then
	storj-sim -x --satellites 1 --host $STORJ_NETWORK_HOST4 network setup
else
	storj-sim -x --satellites 1 --host $STORJ_NETWORK_HOST4 network --postgres=$STORJ_SIM_POSTGRES setup
fi

sed -i 's/# metainfo.rate-limiter.enabled: true/metainfo.rate-limiter.enabled: false/g' $(storj-sim network env SATELLITE_0_DIR)/config.yaml

storj-sim -x --satellites 1 --host $STORJ_NETWORK_HOST4 network test bash "$SCRIPTDIR"/rclone.sh
storj-sim -x --satellites 1 --host $STORJ_NETWORK_HOST4 network destroy
