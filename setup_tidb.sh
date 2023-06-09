#!/bin/bash
set -e

SCRIPT_PATH=$(
  cd $(dirname $0)
  pwd
)

${SCRIPT_PATH}/stop_clean_tidb.sh
docker compose -f ${SCRIPT_PATH}/tidb/.tmp.tidb-cluster.yml up -d 2>&1

function wait_env() {
  local timeout='200'
  local failed='true'

  echo "=> wait for env available"

  for ((i = 0; i < "${timeout}"; i++)); do
    if [[ -n $(grep "server is running MySQL protocol" ./.tmp.demo/tidb/log/tidb0/tidb.log) ]]; then
      local failed='false'
      break
    fi

    if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
      echo "   #${i} waiting for env available"
    fi

    sleep 1
  done

  if [ "${failed}" == 'true' ]; then
    echo "   can not set up env" >&2
    exit 1
  else
    echo "   available"
  fi
}

wait_env
