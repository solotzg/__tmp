
#!/usr/bin/python3

import os
import json
import time
from threading import Thread

from inner_utils.common import run_cmd_no_msg
from inner_utils import *


SCRIPT_DIR = os.path.realpath(os.path.join(__file__, os.pardir))


def func():
    time.sleep(5)
    cmd = './setup-demo.py --cmd sink_task --sink_task_desc="etl3.3.demo.t3" --sink_task_flink_schema_path ./example3/flink.sql.template'
    _, _, ret = run_cmd(cmd, True)
    assert not ret


def main():
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
    thread_1 = Thread(target=func, daemon=True, name='flink-hudi-bench')
    thread_1.start()
    for _ in range(2000):
        cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "insert into demo.t3 (select max(a)+1,max(a)+1 from demo.t3)"'.format(
            tidb_port)
        _, _, ret = run_cmd_no_msg(cmd)
        assert not ret
    time.sleep(5)
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
select count(*) from demo_hudi.t3;
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
        print(out[-500:])
        exit(-1)
    ret_hudi = out.split('\n')[-8][-8:-2].strip()
    logger.info("hudi result: {}".format(ret_hudi))
    cmd = 'mysql -h 0.0.0.0 -P {} -u root -e "select count(*) from demo.t3" '.format(
        tidb_port,
        SCRIPT_DIR)
    out, _, ret = run_cmd(cmd, False)
    assert not ret
    mysql_res = out.split('\n')[-2]
    logger.info("mysql result: {}".format(mysql_res))
    assert mysql_res == ret_hudi


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception(e)
    run_cmd('{}/setup-demo.py --cmd rm_etl_job --etl_job_id etl3'.format(SCRIPT_DIR))
