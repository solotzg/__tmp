#!/bin/bash

set -e

SCRIPT_PATH=$(
  cd $(dirname $0)
  pwd
)
COMPOSE_FILE_NAME=".tmp.docker-compose_hadoop_hive_spark_flink.yml"

CMD_PREFIX="docker compose -f ${SCRIPT_PATH}/${COMPOSE_FILE_NAME} exec"

if [[ -z "$@" ]]; then
  ${CMD_PREFIX} -it sql-client bash
else
  ${CMD_PREFIX} -T sql-client bash -c "$@"
fi
