#!/bin/bash
set -e

SCRIPT_PATH=$(
  cd $(dirname $0)
  pwd
)

${SCRIPT_PATH}/stop_tidb.sh
rm -rf ${SCRIPT_PATH}/.tmp.demo/tidb
