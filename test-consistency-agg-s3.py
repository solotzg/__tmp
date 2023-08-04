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
    cmd = './setup-demo.py --cmd sink_task --sink_task_desc="etl4.4.demo.t4" --sink_task_flink_schema_path ./example4-s3/flink.sql.template'
    run_cmd(cmd, show_stdout=True, no_error=True)
    logger.info('end thread `{}`'.format(threading.get_ident()))


def run():
    env_data = json.load(open('{}/.tmp.env.json'.format(SCRIPT_DIR), 'r'))
    tidb_port = env_data['tidb_port']
    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "drop table IF EXISTS demo.t4" '.format(
        tidb_port,
        SCRIPT_DIR)
    run_cmd(cmd, no_error=True)
    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "drop table IF EXISTS demo.t4_tmp" '.format(
        tidb_port,
        SCRIPT_DIR)
    run_cmd(cmd, no_error=True)
    cmd = 'mysql -h 0.0.0.0 -P {} -u root < {}/example4/tidb.sql'.format(
        tidb_port,
        SCRIPT_DIR)
    run_cmd(cmd, no_error=True)
    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "create table demo.t4_tmp(a int PRIMARY KEY)"'.format(
        tidb_port,
        SCRIPT_DIR)
    run_cmd(cmd, no_error=True)
    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "insert into demo.t4_tmp values(RAND()*100)"'.format(
        tidb_port,
        SCRIPT_DIR)
    run_cmd(cmd, no_error=True)
    thread_1 = Thread(target=sink_table_info_no_error,
                      daemon=True, name='flink-hudi-bench')
    thread_1.start()
    loop_cnt = 100
    logger.info(
        'start to exec with sql `insert into demo.t4 (select max(a)+1,max(a)+1 from demo.t4)`, loop {} times'.format(loop_cnt))
    for _ in range(loop_cnt):
        cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "insert into demo.t4 (select max(a)+1,max(a)+1 from demo.t4)"'.format(
            tidb_port)
        run_cmd_no_debug_info(cmd, no_error=True, )
    loop_cnt = 2000
    logger.info(
        'start to exec with sql `update demo.t4_tmp set a=a+1; update demo.t4 set b=(select a from demo.t4_tmp) where a > 3;`, loop {} times'.format(loop_cnt))
    for _ in range(loop_cnt):
        cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "update demo.t4_tmp set a=a+1; update demo.t4 set b=(select a from demo.t4_tmp limit 1) where a > 3;"'.format(
            tidb_port)
        run_cmd_no_debug_info(cmd, no_error=True, )

    logger.info(
        'start to delete a few records')

    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "delete from demo.t4 where a < 3 "'.format(
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
CREATE TABLE demo_hudi.t4_agg(
  c INT PRIMARY KEY NOT ENFORCED,
  d BIGINT
) WITH (
  'connector' = 'hudi',
  'path' = 's3a://etl4-sink-4/hudi-agg',
  'table.type' = 'MERGE_ON_READ'
);
SET sql-client.execution.result-mode=TABLEAU;
SET execution.runtime-mode='batch';
select c as demo_hudi_t4_group_key, d as demo_hudi_t4_group_res from demo_hudi.t4_agg;
"""
    sql_file_name = '.tmp.bench.kafka-flink-hudi-agg.sql'
    sql_file = '{}/{}'.format(SCRIPT_DIR, sql_file_name)
    with open(sql_file, 'w') as f:
        f.write(data)
    cmd = "{}/run-flink-bash.sh '/pingcap/demo/flink-sql-client.sh -f /pingcap/demo/{}'".format(
        SCRIPT_DIR,
        sql_file_name)
    hudi_out, _, _ = run_cmd(cmd, no_error=True)

    try:
        hudi_res = [line for line in hudi_out.split('\n') if line]
        for i, line in enumerate(hudi_res):
            if line.find('demo_hudi_t4_group_key') != -1 and line.find('select') == -1:
                hudi_res = hudi_res[i:i+4]
                break
        hudi_res = [[x for x in line.split(' ') if x] for line in hudi_res]
        assert [hudi_res[0][1], hudi_res[0][3]] == [
            'demo_hudi_t4_group_key', 'demo_hudi_t4_group_res']
        hudi_res = hudi_res[2:4]
        hudi_res = [[x[1], x[3]] for x in hudi_res]
        hudi_res = sorted(hudi_res, key=lambda o: o[0])
        logger.info("hudi result: \n{}\n".format(hudi_res))
    except Exception as e:
        logger.error(e)
        logger.warning('hudi std out:\n{}\n'.format(hudi_out))

    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "select b as demo_hudi_t4_group_key, sum(a) as demo_hudi_t4_group_res from demo.t4 group by b" '.format(
        tidb_port,
        SCRIPT_DIR)
    tidb_out, _, _ = run_cmd(cmd, show_stdout=False, no_error=True)
    tidb_res = [[t for t in line.split(' ') if t]
                for line in tidb_out.replace('\t', ' ').strip().split('\n')]
    assert tidb_res[0] == ['demo_hudi_t4_group_key', 'demo_hudi_t4_group_res']
    assert len(tidb_res) == 3
    tidb_res = tidb_res[1:]
    tidb_res = sorted(tidb_res, key=lambda o: o[0])
    logger.info("mysql result: \n{}\n".format(tidb_res))

    assert hudi_res == tidb_res


def main():
    try:
        run()
        run_cmd(
            '{}/setup-demo.py --cmd rm_etl_job --etl_job_id etl4'.format(
                SCRIPT_DIR),
            no_error=True)
    except Exception as e:
        logger.exception(e)
        logger.warning(
            "please run `./setup-demo.py --cmd list_all_jobs` and remove jobs manually by `./setup-demo.py --cmd list_***`")


if __name__ == '__main__':
    main()
