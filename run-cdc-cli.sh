#!/bin/bash
set -e

SCRIPT_PATH=$(
    cd $(dirname $0)
    pwd
)

docker compose -f ${SCRIPT_PATH}/tidb/.tmp.tidb-cluster.yml exec -T ticdc_server0 bash -c "$@"
