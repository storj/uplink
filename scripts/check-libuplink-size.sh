#!/usr/bin/env bash
set -ueo pipefail
set +x

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

go build -v -o $SCRIPTDIR/../.build/build.out $SCRIPTDIR/../examples/walkthrough/main.go

BUILD_SIZE=$(wc -c < $SCRIPTDIR/../.build/build.out)
CURRENT_SIZE=15359586 # for commit hash: fc4ab8256b07d89ebb08095088aa37864b9766ca

if [ $BUILD_SIZE -gt $CURRENT_SIZE ]; then
    echo "Libuplink size is too big, was $CURRENT_SIZE but now it is $BUILD_SIZE"
    exit 1
fi

echo "Libuplink size did not increase and it is $BUILD_SIZE"
