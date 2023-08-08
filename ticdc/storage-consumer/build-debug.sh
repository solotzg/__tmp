#!/bin/bash

set -e

SCRIPT_PATH=$(
    cd $(dirname $0)
    pwd
)

cd ${SCRIPT_PATH}

CGO_ENABLED=1 GO111MODULE=on go build -gcflags="all=-N -l" -o ./bin/
