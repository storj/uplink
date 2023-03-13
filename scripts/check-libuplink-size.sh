#!/usr/bin/env bash
set -ueo pipefail
set +x

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

go build -v -o $SCRIPTDIR/../.build/build.out $SCRIPTDIR/../examples/walkthrough/main.go

BUILD_SIZE=$(wc -c < $SCRIPTDIR/../.build/build.out)
CURRENT_SIZE=13026938

if [ $BUILD_SIZE -gt $CURRENT_SIZE ]; then
    echo "Libuplink size is too big, was $CURRENT_SIZE but now it is $BUILD_SIZE"
    exit 1
fi

echo "Libuplink size did not increase and it is $BUILD_SIZE (limit: $CURRENT_SIZE)"
