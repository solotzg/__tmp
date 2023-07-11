#!/usr/bin/env bash

SCRIPTPATH="$(
    cd "$(dirname "$0")"
    pwd -P
)"

source ${SCRIPTPATH}/_flink-env.sh
/opt/flink/bin/sql-client.sh $@
