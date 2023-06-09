#!/bin/bash
set -e

SCRIPT_PATH=$(
  cd $(dirname $0)
  pwd
)
COMPOSE_FILE_NAME=".tmp.docker-compose_hadoop_hive_spark_flink.yml"

docker compose -f ${SCRIPT_PATH}/${COMPOSE_FILE_NAME} down 2>&1

rm -rf ${SCRIPT_PATH}/.tmp.demo/third_party
