#!/usr/bin/env bash

export HADOOP_HOME=/pingcap/env_libs/hadoop-2.8.4
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
ln -s /pingcap/env_libs/*.jar /opt/flink/lib/
/opt/flink/bin/sql-client.sh $@
