#!/usr/bin/env bash

SCRIPTPATH="$(
    cd "$(dirname "$0")"
    pwd -P
)"

source ${SCRIPTPATH}/_flink-env.sh
/docker-entrypoint.sh taskmanager $@
