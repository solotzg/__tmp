#!/usr/bin/python3

import os
import json
import time
from threading import Thread
import threading

from inner_utils.common import run_cmd_no_msg
from inner_utils import *


SCRIPT_DIR = os.path.realpath(os.path.join(__file__, os.pardir))


def sink_table_info_error():
    logger.info('start thread `{}`'.format(threading.get_ident()))
    sleep_time = 5
    logger.info('start to sleep {}s'.format(sleep_time))
    time.sleep(sleep_time)
    logger.info('finish to sleep {}s'.format(sleep_time))
    cmd = './setup-demo.py --cmd sink_task --sink_task_desc="etl3.3.demo.t3" --sink_task_flink_schema_path ./example3/flink.sql.template'
    _, _, ret = run_cmd(cmd, True)
    assert not ret
    logger.info('end thread `{}`'.format(threading.get_ident()))


def run():
    env_data = json.load(open('{}/.tmp.env.json'.format(SCRIPT_DIR), 'r'))
    tidb_port = env_data['tidb_port']
    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "drop table IF EXISTS demo.t3" '.format(
        tidb_port,
        SCRIPT_DIR)
    run_cmd(cmd)
    cmd = 'mysql -h 0.0.0.0 -P {} -u root < {}/example3/tidb.sql'.format(
        tidb_port,
        SCRIPT_DIR)
    run_cmd(cmd)
    thread_1 = Thread(target=sink_table_info_error,
                      daemon=True, name='flink-hudi-bench')
    thread_1.start()
    loop_cnt = 2000
    logger.info(
        'start to exec with sql `insert into demo.t3 (select max(a)+1,max(a)+1 from demo.t3)`, loop {} times'.format(loop_cnt))
    for _ in range(2000):
        cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "insert into demo.t3 (select max(a)+1,max(a)+1 from demo.t3)"'.format(
            tidb_port)
        _, _, ret = run_cmd_no_msg(cmd)
        assert not ret
    logger.info(
        'finished to exec sql')
    sleep_time = 5
    logger.info('start to sleep {}s'.format(sleep_time))
    time.sleep(sleep_time)
    logger.info('finished sleep {}s'.format(sleep_time))
    thread_1.join()
    data = """
create database demo_hudi;
CREATE TABLE demo_hudi.t3(
  a INT PRIMARY KEY NOT ENFORCED,
  b INT
) WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://namenode:8020/pingcap/demo/etl3-sink-3',
  'table.type' = 'MERGE_ON_READ'
);
SET sql-client.execution.result-mode=TABLEAU;
SET execution.runtime-mode='batch';
select count(*) as demo_hudi_t3_count from demo_hudi.t3;
"""
    sql_file_name = '.tmp.bench.kafka-flink-hudi.sql'
    sql_file = '{}/{}'.format(SCRIPT_DIR, sql_file_name)
    with open(sql_file, 'w') as f:
        f.write(data)
    cmd = "./run-flink-bash.sh /pingcap/demo/flink-sql-client.sh -f /pingcap/demo/{}".format(
        sql_file_name)
    out, err, ret = run_cmd(cmd,)
    if ret:
        print(err)
        print(out)
        exit(-1)
    assert out.find('demo_hudi_t3_count') != -1
    ret_hudi = out.rstrip().split('\n')[-6]
    ret_hudi = [e for e in ret_hudi.split(' ') if e][1]
    logger.info("hudi result: {}".format(ret_hudi))
    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "select count(*) from demo.t3" '.format(
        tidb_port,
        SCRIPT_DIR)
    out, _, ret = run_cmd(cmd, False)
    assert not ret
    mysql_res = out.split('\n')[-2]
    logger.info("mysql result: {}".format(mysql_res))
    assert mysql_res == ret_hudi


def main():
    try:
        run()
        run_cmd(
            '{}/setup-demo.py --cmd rm_etl_job --etl_job_id etl3'.format(SCRIPT_DIR))
    except Exception as e:
        logger.exception(e)
        logger.warning(
            "please run `./setup-demo.py --cmd list_all_jobs` and remove jobs manually by `./setup-demo.py --cmd list_***`")


if __name__ == '__main__':
    main()
