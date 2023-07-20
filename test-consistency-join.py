#!/usr/bin/env python3

import os
import json
import time
from threading import Thread
import threading

from inner_utils import *


SCRIPT_DIR = os.path.realpath(os.path.join(__file__, os.pardir))


def sink_table_info_no_error():
    logger.info('start thread `{}`'.format(threading.get_ident()))
    sleep_time = 5
    logger.info('start to sleep {}s'.format(sleep_time))
    time.sleep(sleep_time)
    logger.info('finish to sleep {}s'.format(sleep_time))
    cmd = './setup-demo.py --cmd sink_task --sink_task_desc="etl5.5.demo.t5" --sink_task_flink_schema_path ./example5/flink.sql.template'
    run_cmd(cmd, show_stdout=True, no_error=True)
    logger.info('end thread `{}`'.format(threading.get_ident()))


def run():
    env_data = json.load(open('{}/.tmp.env.json'.format(SCRIPT_DIR), 'r'))
    tidb_port = env_data['tidb_port']
    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "drop table IF EXISTS demo.t5" '.format(
        tidb_port,
        SCRIPT_DIR)
    run_cmd(cmd, no_error=True)
    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "drop table IF EXISTS demo.t5_build" '.format(
        tidb_port,
        SCRIPT_DIR)
    run_cmd(cmd, no_error=True)
    cmd = 'mysql -h 0.0.0.0 -P {} -u root < {}/example5/tidb.sql'.format(
        tidb_port,
        SCRIPT_DIR)
    run_cmd(cmd, no_error=True)
    thread_1 = Thread(target=sink_table_info_no_error,
                      daemon=True, name='flink-hudi-bench')
    thread_1.start()
    loop_cnt = 2000
    for s in range(loop_cnt):
        build_id = 4 + s
        insert_id = 10 + s
        if s % 100 == 0:
            cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "insert into demo.t5_build values ({},{}) ;"'.format(
                tidb_port, build_id, build_id * 100)
            run_cmd_no_debug_info(cmd, no_error=True, )
        cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "insert into demo.t5 values ({},{});"'.format(
            tidb_port, insert_id, build_id)
        run_cmd_no_debug_info(cmd, no_error=True, )

    logger.info(
        'start to delete a few records')

    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "delete from demo.t5 where a < 3 "'.format(
        tidb_port)
    run_cmd(cmd, no_error=True)

    logger.info(
        'finished to exec sql')
    sleep_time = 5
    logger.info('start to sleep {}s'.format(sleep_time))
    time.sleep(sleep_time)
    logger.info('finished sleep {}s'.format(sleep_time))
    thread_1.join()
    data = """
create database demo_hudi;
CREATE TABLE demo_hudi.t5_join(
  b INT PRIMARY KEY NOT ENFORCED,
  v BIGINT
) WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://namenode:8020/pingcap/demo/etl5-sink-5/join',
  'table.type' = 'MERGE_ON_READ'
);
SET sql-client.execution.result-mode=TABLEAU;
SET execution.runtime-mode='batch';
select b as demo_hudi_t5_group_key, v as demo_hudi_t5_group_res from demo_hudi.t5_join order by demo_hudi_t5_group_key;
"""
    sql_file_name = '.tmp.bench.kafka-flink-hudi-join.sql'
    sql_file = '{}/{}'.format(SCRIPT_DIR, sql_file_name)
    with open(sql_file, 'w') as f:
        f.write(data)
    cmd = "{}/run-flink-bash.sh '/pingcap/demo/flink-sql-client.sh -f /pingcap/demo/{}'".format(
        SCRIPT_DIR,
        sql_file_name)
    hudi_out, _, _ = run_cmd(cmd, no_error=True)

    try:
        hudi_res = [[t for t in line.split(' ') if t]
                    for line in hudi_out.strip().split('\n')]

        hudi_res = [[t for t in line.split(' ') if t]
                    for line in hudi_out.strip().split('\n')]
        for i, data in enumerate(hudi_res):
            if len(data) < 4:
                continue
            if [data[1], data[3]] == ['demo_hudi_t5_group_key', 'demo_hudi_t5_group_res']:
                break
        hudi_res = hudi_res[i+2:i+2+22]
        hudi_res = [[x[1], x[3]] for x in hudi_res]
        logger.info("hudi result: \n{}\n".format(hudi_res))
    except Exception as e:
        logger.error(e)
        logger.warning('hudi std out:\n{}\n'.format(hudi_out))

    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "select build.b as demo_hudi_t5_group_key, sum(c) as demo_hudi_t5_group_res from demo.t5 as prob inner join demo.t5_build as build on prob.b = build.b group by build.b order by build.b" '.format(
        tidb_port,
        SCRIPT_DIR)
    tidb_out, _, _ = run_cmd(cmd, show_stdout=False, no_error=True)
    tidb_res = [[t for t in line.split(' ') if t]
                for line in tidb_out.replace('\t', ' ').strip().split('\n')]
    tidb_res = tidb_res[1:]
    logger.info("mysql result: \n{}\n".format(tidb_res))

    assert hudi_res == tidb_res


def main():
    try:
        run()
        run_cmd(
            '{}/setup-demo.py --cmd rm_etl_job --etl_job_id etl5'.format(
                SCRIPT_DIR),
            no_error=True)
    except Exception as e:
        logger.exception(e)
        logger.warning(
            "please run `./setup-demo.py --cmd list_all_jobs` and remove jobs manually by `./setup-demo.py --cmd list_***`")


if __name__ == '__main__':
    main()
