#!/bin/bash

set -e

SCRIPT_PATH=$(
  cd $(dirname $0)
  pwd
)
COMPOSE_FILE_NAME=".tmp.docker-compose_hadoop_hive_spark_flink.yml"

if [[ -z "${SQL_PATH}" ]]; then
  echo "ERROR: env var SQL_PATH is empty"
  exit -1
fi

docker compose -f ${SCRIPT_PATH}/${COMPOSE_FILE_NAME} run -it --rm sql-client /pingcap/demo/flink-sql-client.sh embedded -f /pingcap/demo/${SQL_PATH}
