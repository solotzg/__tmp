#!/bin/bash
set -e

SCRIPT_PATH=$(
    cd $(dirname $0)
    pwd
)

docker compose -f ${SCRIPT_PATH}/tidb/.tmp.tidb-cluster.yml run -it --rm dumpling0 sh -c "$@"
