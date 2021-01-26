#!/usr/bin/env bash
set -ueo pipefail
set +x

# This script verifies that we don't accidentally import specific packages.

if go list -deps -test . | grep -q "google.golang.org/grpc"; then
    echo "uplink must not have a dependency to grpc";
    exit -1;
fi

if go list -deps -test . | grep -q "storj.io/storj"; then
    echo "uplink must not have a dependency to storj.io/storj";
    exit -1;
fi

if go list -deps -test . | grep -Eq "github.com/(lib/pq|jackc/pg)"; then
    echo "uplink must not have a dependency to postgres";
    exit -1;
fi

if go list -deps -test . | grep -q "redis"; then
    echo "uplink must not have a dependency to redis";
    exit -1;
fi

if go list -deps -test ./... | grep -q "bolt"; then
    echo "common must not have a dependency to bolt";
    exit -1;
fi

if go list -deps . | grep -q "testing"; then
    echo "uplink must not have a dependency to testing";
    exit -1;
fi
