#!/bin/bash

set -e

SCRIPT_PATH=$(
  cd $(dirname $0)
  pwd
)
COMPOSE_FILE_NAME=".tmp.docker-compose_hadoop_hive_spark_flink.yml"

docker compose -f ${SCRIPT_PATH}/${COMPOSE_FILE_NAME} down 2>&1

deploy_data_dir=${SCRIPT_PATH}/.tmp.demo
deploy_data_third_party=${deploy_data_dir}/third_party
mkdir -p ${deploy_data_third_party}

cd ${deploy_data_third_party}
mkdir -p hadoop/dfs/name
mkdir -p hadoop/dfs/data
mkdir -p kafka
mkdir -p flink
mkdir -p zookeeper
cd -

chmod 777 -R ${deploy_data_dir}

docker compose --verbose -f ${SCRIPT_PATH}/${COMPOSE_FILE_NAME} up -d 2>&1
