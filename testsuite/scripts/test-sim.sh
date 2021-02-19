#!/usr/bin/env bash
set -ueo pipefail
set +x

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# setup tmpdir for testfiles and cleanup
TMP=$(mktemp -d -t tmp.XXXXXXXXXX)
cleanup(){
	rm -rf "$TMP"
}
trap cleanup EXIT

cd $SCRIPTDIR && go install \
	storj.io/storj/cmd/certificates \
	storj.io/storj/cmd/identity \
	storj.io/storj/cmd/satellite \
	storj.io/storj/cmd/storagenode \
	storj.io/storj/cmd/versioncontrol \
	storj.io/storj/cmd/storj-sim

go install storj.io/gateway@latest

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
