#!/bin/bash
set -e

SCRIPT_PATH=$(
    cd $(dirname $0)
    pwd
)

if [[ -z "${CDC_BIN_PATH}" ]]; then
    docker compose -f ${SCRIPT_PATH}/.tmp.tidb-cluster.yml exec -T ticdc_server0 bash -c "/cdc $@"
else
    ${CDC_BIN_PATH} "$@"
fi
