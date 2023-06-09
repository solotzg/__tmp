#!/bin/bash
set -e

SCRIPT_PATH=$(
  cd $(dirname $0)
  pwd
)

docker compose -f ${SCRIPT_PATH}/tidb/.tmp.tidb-cluster.yml down 2>&1
