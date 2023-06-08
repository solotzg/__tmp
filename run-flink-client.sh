#!/bin/bash

set -e

SCRIPT_PATH=$(
  cd $(dirname $0)
  pwd
)
COMPOSE_FILE_NAME=".tmp.docker-compose_hadoop_hive_spark_flink.yml"

if [[ -z "${HUDI_WS}" ]]; then
  echo "ERROR: env var HUDI_WS is empty"
  exit -1
fi

if [[ -z "${SQL_PATH}" ]]; then
  echo "ERROR: env var SQL_PATH is empty"
  exit -1
fi

# restart cluster
HUDI_WS=${HUDI_WS} docker compose -f ${SCRIPT_PATH}/${COMPOSE_FILE_NAME} exec -it sql-client /bin/bash /pingcap/demo/${SQL_PATH}
