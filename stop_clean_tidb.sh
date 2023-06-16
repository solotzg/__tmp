#!/bin/bash
set -e

SCRIPT_PATH=$(
  cd $(dirname $0)
  pwd
)

docker compose -f ${SCRIPT_PATH}/.tmp.tidb-cluster.yml down 2>&1
rm -rf ${SCRIPT_PATH}/.tmp.demo/tidb
