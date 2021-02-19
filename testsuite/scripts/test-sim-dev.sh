#!/usr/bin/env bash
set -ueo pipefail
set +x

# This script is a wrapper for running integration tests locally.
# It's using postgres-dev.sh to configure new postgres instance for
# tests usage.

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $SCRIPTDIR/postgres-dev.sh

$SCRIPTDIR/test-sim.sh