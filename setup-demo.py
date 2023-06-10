#!/usr/bin/python3
import argparse
import datetime
import fcntl
import os
import json as json
from inner_utils import *
import socket
from string import Template


SCRIPT_DIR = os.path.realpath(os.path.join(__file__, os.pardir))
DOWNLOAD_URL = 'http://10.2.12.124:19876'
HUDI_START_PORT_OFFSET = 0
kafka_port_name = 'kafka_port'
flink_jobmanager_port_name = 'flink_jobmanager_port'
HUDI_FLINK_VARS_SET = {"hadoop_web_port", "hdfs_port", "historyserver_port", "hiveserver_port",
                       "spark_web_port", "spark_master_port", kafka_port_name, flink_jobmanager_port_name}
TIDB_START_PORT_OFFSET = len(HUDI_FLINK_VARS_SET) + HUDI_START_PORT_OFFSET
ticdc_port_name = 'ticdc_port'
tidb_port_name = 'tidb_port'
TIDB_VARS_SET = {"pd_port", "tikv_status_port",
                 tidb_port_name, ticdc_port_name}
tidb_compose_name = 'tidb-compose'
hufi_flink_compose = 'hufi-flink-compose'
cdc_name = "cdc"
env_file_path = "{}/.tmp.env.json".format(SCRIPT_DIR)
HUDI_WS = 'HUDI_WS'
tidb_running = 'tidb_running'
hudi_flink_running = 'hudi_flink_running'
TIDB_BRANCH = 'TIDB_BRANCH'
demo_host = 'demo_host'
env_libs_name = 'env_libs'
start_port_name = 'start_port'


def get_host_name():
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip


class Runner:
    def __init__(self):
        pass

    def install_jdk1_8(self):
        self.java_home = None
        java_home_var_name = 'JAVA_HOME'

        def load_java_home(env_data):
            self.java_home = env_data.get(java_home_var_name)
            if self.java_home is None:
                logger.info("env var `{}` not found".format(
                    java_home_var_name))
                env_java_home = os.getenv(java_home_var_name)
                ok = True
                if env_java_home is not None:
                    cmd = '{}/bin/javac -version 2>&1'.format(env_java_home)
                    out, _, retcode = run_cmd(cmd)
                    if retcode:
                        logger.error(
                            "failed to exec `{}`, error:\n{}".format(cmd, out))
                        exit(-1)
                    if not out.startswith('javac'):
                        ok = False
                    else:
                        java_version = out[len('javac')+1:].split('.')
                        if java_version[0] == '1' and java_version[1] == '8':
                            self.java_home = env_java_home
                        else:
                            logger.error('using {}'.format(out.strip()))
                            ok = False
                else:
                    ok = False
                if not ok:
                    logger.error(
                        "please install jdk 1.8 and set export `JAVA_HOME` before running this scripts")
                    exit(-1)
                assert self.java_home
                env_data[java_home_var_name] = self.java_home
                logger.info("save env var `{}`: `{}`".format(
                    java_home_var_name, self.java_home))
                return env_data, True
            else:
                logger.info("load env var `{}`: `{}`".format(
                    java_home_var_name, self.java_home))
                return env_data, False

        try_handle_env_data(load_java_home)

        return self.java_home

    def compile_hudi(self):
        assert self.args.hudi_repo
        hudi_path = self.args.hudi_repo
        assert os.path.exists(hudi_path)

        need_clean = True
        java_home = self.install_jdk1_8()

        def handle_compile_hudi(env_data):
            hudi_compiled = 'hudi_compiled'
            hudi_compiled_time = env_data.get(hudi_compiled)
            if hudi_compiled_time is not None:
                logger.info('hudi was compiled at `{}`'.format(
                    hudi_compiled_time))
                return env_data, False
            _, stderr, retcode = run_cmd("export JAVA_HOME={} && cd {} && mvn {} package -Pintegration-tests -DskipTests -Drat.skip=true".format(
                java_home,
                hudi_path,
                "clean" if need_clean else ""), True)
            if retcode:
                logger.error(stderr)
                exit(-1)
            dt_ms = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            env_data[hudi_compiled] = dt_ms
            env_data[HUDI_WS] = hudi_path
            logger.info('set hudi compiled time: {}'.format(
                hudi_compiled_time))
            return env_data, True

        try_handle_env_data(handle_compile_hudi)

    def _init(self):
        parser = argparse.ArgumentParser(
            description="tidb ticdc flink hudi", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            '--hudi_repo', help='hudi git repo absolute path')
        parser.add_argument(
            '--env_libs', help='path to env binary and libs')
        parser.add_argument(
            '--start_port', help='start port for different components {}'.format(HUDI_FLINK_VARS_SET.union(TIDB_VARS_SET)))
        parser.add_argument(
            '--tidb_branch', help='tidb branch name: master, release-x.y, ...')
        parser.add_argument(
            '--sink_task_desc', help='format: `etl_uid.table_id.db_name.table_name`. sink tidb table to ticdc -> kafka -> flink -> hudi')
        parser.add_argument(
            '--sink_task_flink_schema_path', help='path to sql file include table schema for flink and hudi')
        parser.add_argument(
            '--cmd', help='command enum', choices=(
                'deploy_hudi_flink', 'deploy_tidb', 'deploy_hudi_flink_tidb', 'sink_task',
                'down_hudi_flink', 'stop_tidb', 'down_tidb', 'compile_hudi', 'show_env_vars_info',
                'down',))
        self.args = parser.parse_args()
        if self.args.start_port is not None:
            self.args.start_port = int(self.args.start_port)
        self.funcs_map = {
            'deploy_hudi_flink': self.deploy_hudi_flink,
            'deploy_tidb': self.deploy_tidb,
            'deploy_hudi_flink_tidb': self.deploy_hudi_flink_tidb,
            'sink_task': self.sink_task,
            'down_hudi_flink': self.down_hudi_flink,
            'stop_tidb': self.stop_tidb,
            'down_tidb': self.down_tidb,
            'compile_hudi': self.compile_hudi,
            'show_env_vars_info': self.show_env_vars_info,
            'down': self.down,
        }

    def down(self):
        self.down_hudi_flink()
        self.down_tidb()

    def show_env_vars_info(self):
        env_vars = self.load_env_vars()
        print(env_vars)

    def down_hudi_flink(self):
        env_vars = self.load_env_vars()
        logger.info(
            "hudi flink is running, start to down docker compose")
        cmd = '{}/down_hudi_flink.sh'.format(SCRIPT_DIR)
        _, _, ret = run_cmd(cmd, True, env={HUDI_WS: env_vars[HUDI_WS]})
        if ret:
            logger.error("fail to stop hudi flink")
            exit(-1)

        self.update_env_vars({hudi_flink_running: False})

    def down_tidb(self):
        cmd = '{}/stop_clean_tidb.sh'.format(SCRIPT_DIR)
        _, err, ret = run_cmd(cmd, True)
        if ret:
            logger.error("fail to stop tidb, error:\n{}".format(err))
            exit(-1)

        self.update_env_vars({tidb_running: False})

    def stop_tidb(self):
        env_vars = self.load_env_vars()
        if env_vars.get(tidb_running, False):
            logger.info(
                "tidb is () running, start to stop tidb docker compose".format(env_vars[TIDB_BRANCH]))
            cmd = '{}/stop_tidb.sh'.format(SCRIPT_DIR)
            _, err, ret = run_cmd(cmd, True)
            if ret:
                logger.error("fail to stop tidb, error:\n{}".format(err))
                exit(-1)

            self.update_env_vars({tidb_running: False})

        else:
            logger.info("tidb is NOT running")

    def setup_env_libs(self):
        env_vars = self.load_env_vars()
        self.args.env_libs = self.args.env_libs if self.args.env_libs is not None else env_vars.get(
            env_libs_name)
        assert self.args.env_libs

        if not os.path.exists(self.args.env_libs):
            os.makedirs(self.args.env_libs)

        if self.args.env_libs != env_vars.get(env_libs_name):
            self.update_env_vars({env_libs_name: self.args.env_libs})

        hadoop_name = "hadoop-2.8.4"
        hadoop_path = os.path.join(self.args.env_libs, hadoop_name)
        hadoop_tar_name = '{}.tar.gz'.format(hadoop_name)
        hadoop_url = "{}/{}".format(DOWNLOAD_URL, hadoop_tar_name)

        flink_sql_connector_name = 'flink-sql-connector-kafka_2.12-1.13.6.jar'
        flink_sql_connector_url = "{}/{}".format(
            DOWNLOAD_URL, flink_sql_connector_name)
        hudi_flink_bundle_name = "hudi-flink-bundle_2.12-0.10.0.jar"
        hudi_flink_bundle_url = "{}/{}".format(
            DOWNLOAD_URL, hudi_flink_bundle_name)

        cdc_url = "{}/{}".format(
            DOWNLOAD_URL, cdc_name)

        if not os.path.exists(hadoop_path):
            _, _, status = run_cmd("cd {} && curl -o {} {} && tar zxf {} && cp {}/hdfs-site.xml {}/etc/hadoop/hdfs-site.xml".format(
                self.args.env_libs, hadoop_tar_name, hadoop_url, hadoop_tar_name, SCRIPT_DIR, hadoop_name))
            assert status == 0
        if not os.path.exists(os.path.join(self.args.env_libs, flink_sql_connector_name)):
            _, _, status = run_cmd("cd {} && wget {}".format(
                self.args.env_libs, flink_sql_connector_url))
            assert status == 0
        if not os.path.exists(os.path.join(self.args.env_libs, hudi_flink_bundle_name)):
            _, _, status = run_cmd("cd {} && wget {}".format(
                self.args.env_libs, hudi_flink_bundle_url))
            assert status == 0
        if not os.path.exists(os.path.join(self.args.env_libs, cdc_name)):
            _, _, status = run_cmd("cd {} && wget {} && chmod +x {}".format(
                self.args.env_libs, cdc_url, cdc_name))
            assert status == 0

    def deploy_tidb(self):
        env_vars = self.load_env_vars()
        self.args.start_port = self.args.start_port if self.args.start_port is not None else env_vars.get(
            start_port_name)
        assert self.args.start_port

        tidb_version_prefix = 'release-'
        self.args.tidb_branch = self.args.tidb_branch if self.args.tidb_branch else env_vars.get(
            TIDB_BRANCH)
        assert self.args.tidb_branch
        assert self.args.tidb_branch == 'master' or self.args.tidb_branch.startswith(
            tidb_version_prefix)
        if self.args.tidb_branch.startswith(tidb_version_prefix):
            versions = self.args.tidb_branch[len(tidb_version_prefix):]
            vs = [int(v) for v in versions.split('.')]
            assert len(vs) == 2

        self.setup_env_libs()
        self.gen_tidb_cluster_config_file_from_template(
            self.args.start_port + TIDB_START_PORT_OFFSET, self.args.tidb_branch)

        if env_vars.get(tidb_running, False):
            logger.info(
                "tidb ({}) is running, please stop tidb docker compose if necessary".format(env_vars[TIDB_BRANCH]))
            return
        else:
            logger.info("tidb is NOT running, start docker compose cluster")

        logger.info(
            "start to deploy tidb ({}) cluster".format(env_vars[TIDB_BRANCH]))

        cmd = '{}/setup_tidb.sh'.format(SCRIPT_DIR)
        _, stderr, retcode = run_cmd(cmd, show_stdout=True,)
        if retcode:
            logger.error(
                "failed to deploy tidb cluster, error:\n{}".format(stderr))
            exit(-1)

        self.update_env_vars(
            {tidb_running: True, start_port_name: self.args.start_port})

    def gen_tidb_cluster_config_file_from_template(self, start_port, branch):
        template_file = '{}/{}'.format(SCRIPT_DIR,
                                       'tidb-cluster.yml.template')
        logger.info(
            "start to gen tidb-ticdc cluster docker compose file: start_port={}, branch={}, template_file=`{}`".format(start_port, branch, template_file))
        config_file_path = '{}/tidb/{}'.format(SCRIPT_DIR,
                                               '.tmp.tidb-cluster.yml')
        if os.path.exists(config_file_path):
            logger.warning(
                'flink docker compose file `{}` exists, if need to generate new config, please delete it'.format(config_file_path))
            return

        template_context = load_file(template_file)
        template = Template(template_context)
        var_set = sorted(TIDB_VARS_SET)
        var_map = {}
        for i, v in enumerate(var_set):
            port = start_port+i
            if check_port_occupied(port):
                logger.error(
                    "port {} is occupied, please set new `start_port`".format(port))
                exit(-1)
            var_map[v] = port
        var_map[TIDB_BRANCH] = branch
        var_map['pingcap_demo_path'] = SCRIPT_DIR
        logger.debug("set basic config: {}".format(var_map))
        with open(config_file_path, 'w') as f:
            f.write(template.substitute(var_map))
        logger.info(
            "gen docker compose config file `{}`".format(config_file_path))

        def func(env_data):
            env_data.update(var_map)
            env_data[tidb_compose_name] = config_file_path
            return env_data, True

        try_handle_env_data(func)

    def gen_flink_config_file_from_template(self, start_port, hudi_path):
        template_file = '{}/{}'.format(SCRIPT_DIR,
                                       'docker-compose_hadoop_hive_spark_flink.yml.template')
        logger.info(
            "start to gen flink-hudi cluster docker compose file: start_port={}, hudi_root={}, template_file=`{}`".format(
                start_port, hudi_path, template_file))
        config_file_path = '{}/{}'.format(SCRIPT_DIR,
                                          '.tmp.docker-compose_hadoop_hive_spark_flink.yml')
        if os.path.exists(config_file_path):
            logger.warning(
                'flink docker compose file `{}` exists, if need to generate new config, please delete it'.format(config_file_path))
            return
        template_context = load_file(template_file)
        template = Template(template_context)
        var_set = sorted(HUDI_FLINK_VARS_SET)
        var_map = {}
        for i, v in enumerate(var_set):
            port = start_port+i
            if check_port_occupied(port):
                logger.error(
                    "port {} is occupied, please set new `start_port`".format(port))
                exit(-1)
            var_map[v] = port
        var_map[HUDI_WS] = hudi_path
        var_map['pingcap_demo_path'] = SCRIPT_DIR
        host = get_host_name()
        var_map[demo_host] = host
        var_map[env_libs_name] = self.args.env_libs
        logger.debug("set basic config: {}".format(var_map))
        d = template.substitute(var_map)
        with open(config_file_path, 'w') as f:
            f.write(d)
        logger.info(
            "gen docker compose config file `{}`".format(config_file_path))

        def func(env_data):
            env_data.update(var_map)
            env_data[hufi_flink_compose] = config_file_path
            return env_data, True

        try_handle_env_data(func)

    def deploy_hudi_flink(self):
        env_vars = self.load_env_vars()
        self.args.start_port = self.args.start_port if self.args.start_port is not None else env_vars.get(
            start_port_name)
        assert self.args.start_port

        self.args.hudi_repo = self.args.hudi_repo if self.args.hudi_repo is not None else env_vars.get(
            HUDI_WS)
        assert self.args.hudi_repo
        assert os.path.exists(self.args.hudi_repo)

        self.setup_env_libs()
        self.gen_flink_config_file_from_template(
            self.args.start_port + HUDI_START_PORT_OFFSET, self.args.hudi_repo)
        if env_vars.get(hudi_flink_running, False):
            logger.info(
                "hudi flink is running, please stop hudi flink docker compose if necessary")
            return
        else:
            logger.info(
                "hudi flink is NOT running, start docker compose cluster")
        cmd = '{}/setup_hudi_flink.sh'.format(SCRIPT_DIR)
        _, stderr, retcode = run_cmd(cmd, show_stdout=True, env={
                                     HUDI_WS: env_vars[HUDI_WS]})
        if retcode:
            logger.error(
                "failed to deploy hudi flink cluster, error:\n{}".format(stderr))
            exit(-1)

        self.update_env_vars(
            {hudi_flink_running: True, start_port_name: self.args.start_port})

    def deploy_hudi_flink_tidb(self):
        self.deploy_hudi_flink()
        self.deploy_tidb()

    def load_env_vars(self) -> dict:
        def func(env_data):
            self.env_data = env_data
            return None, False
        try_read_handle_env_data(func)
        return self.env_data

    def update_env_vars(self, new_data):
        def func(env_data):
            env_data.update(new_data)
            return env_data, True
        try_handle_env_data(func)

    def sink_task(self):
        assert self.args.sink_task_desc
        assert self.args.sink_task_flink_schema_path

        _p = self.args.sink_task_desc.split('.')
        if len(_p) != 4:
            logger.error(
                "invalid sink format, need `etl_uid.table_id.db_name.table_name`")
        etl_uid, table_id, db, table_name = self.args.sink_task_desc.split('.')

        assert os.path.exists(self.args.sink_task_flink_schema_path)

        table_id = int(table_id)
        self.setup_env_libs()
        env_vars = self.load_env_vars()
        host = get_host_name()

        out, err, ret = run_cmd(
            "mysql -h 0.0.0.0 -P {} -u root -e 'desc {}.{}' ".format(env_vars[tidb_port_name], db, table_name))
        if ret:
            logger.error("tidb error:\n{}".format(err))
            exit(-1)
        else:
            logger.info('schema of `{}`.`{}` is:\n{}'.format(
                db, table_name, out))

        cdc_server = "http://{}:{}".format(host, env_vars[ticdc_port_name])
        kafka_addr = "{}:{}".format(host, env_vars[kafka_port_name])
        protocol = "canal-json"
        kafka_version = "2.4.0"
        partition_num = 1
        max_message_bytes = 67108864
        replication_factor = 1
        topic = '{}-sink-{}'.format(etl_uid, table_id)
        changefeed_id = topic
        cdc_config = gen_ticdc_config_file(
            etl_uid, table_id, db, table_name)
        cdc_bin_path = os.path.join(self.args.env_libs, cdc_name)
        logger.info('gen topic `{}`, changefeed-id `{}` for sink task `{}`'.format(
            topic, changefeed_id, self.args.sink_task_desc))
        cmd = '{} cli changefeed create --server={} --sink-uri="kafka://{}/{}?protocol={}&kafka-version={}&partition-num={}&max-message-bytes={}&replication-factor={}" --changefeed-id="{}" --config={}'.format(
            cdc_bin_path, cdc_server, kafka_addr, topic, protocol, kafka_version, partition_num, max_message_bytes, replication_factor, changefeed_id, cdc_config)
        _, err, ret = run_cmd(cmd, True)
        if ret:
            logger.error(
                "failed to create table sink task by ticdc client, error:\n{}".format(err))
            exit(-1)

        template = Template(load_file(self.args.sink_task_flink_schema_path))
        var_map = {"kafka_address": "kafkabroker:9092",
                   "kafka_topic": topic, "hdfs_address": "namenode:8020"}
        logger.debug("set basic config: {}".format(var_map))
        sql_file_rel_path = '.tmp.flink.sink-{}-{}-{}.{}.sql'.format(
            etl_uid, table_id, db, table_name)
        flink_sql_path = '{}/{}'.format(SCRIPT_DIR, sql_file_rel_path)
        with open(flink_sql_path, 'w') as f:
            f.write(template.substitute(var_map))
        logger.info("save flink sink sql to `{}`".format(flink_sql_path))
        cmd = '{}/run-flink-client-sql.sh'.format(SCRIPT_DIR)
        out, err, ret = run_cmd(cmd, False, env={
            HUDI_WS: env_vars[HUDI_WS], 'SQL_PATH': sql_file_rel_path})
        if ret:
            logger.error(
                "failed to run flink sql by flink client, error:\n{}".format(err))
            exit(-1)
        logger.info(
            "success to run flink sql by flink client, sql file path: `{}`".format(flink_sql_path))
        logger.info(
            "please open flink jobmanager web site http://{}:{} for details".format(host, env_vars[flink_jobmanager_port_name]))

    def run(self):
        self._init()
        func = self.funcs_map.get(self.args.cmd)
        if func is None:
            exit(-1)
        func()


def check_port_occupied(port):
    s = socket.socket()
    try:
        s.connect(("127.0.0.1", port))
        return True
    except:
        return False
    finally:
        s.close()


def load_file(file_path):
    template_context = []
    with open(file_path, 'r') as f:
        for line in f.readlines():
            line = line.rstrip()
            if not line:
                continue
            template_context.append(line)
    logger.debug('load context from `{}`'.format(file_path))
    return '\n'.join(template_context)


def gen_ticdc_config_file(etl_uid, table_id, db, table):
    logger.info("start to gen ticdc config file: etl_uid={}, table_id={}, db={}, table={}".format(
        etl_uid, table_id, db, table))
    buf = """enable-old-value = true
[filter]
rules = ['{}.{}']""".format(db, table)
    file_name = '.tmp.ticdc-config-{}-{}-{}.{}.toml'.format(
        etl_uid, table_id, db, table)
    file_path = os.path.join(SCRIPT_DIR, file_name)
    with open(file_path, "w") as f:
        f.write(buf)
    logger.info(
        "gen ticdc config file to path `{}`, content:\n{}\n".format(file_path, buf))
    return file_path


def try_handle_env_data(func):
    if not os.path.exists(env_file_path):
        try:
            with open(env_file_path, "x") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
        except FileExistsError:
            pass
    new_data = {}
    with open(env_file_path, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        s = f.read()
        if not s:
            ori_data = {}
        else:
            ori_data = json.loads(s)
        new_data, need_update = func(ori_data)
        if not need_update:
            return
        f.seek(0, 0)
        f.truncate(0)
        json.dump(new_data, f)
    logger.info("save env vars `{}` to `{}`".format(new_data, env_file_path))


def try_read_handle_env_data(func):
    if not os.path.exists(env_file_path):
        try:
            with open(env_file_path, "x") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
        except FileExistsError:
            pass
    with open(env_file_path, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        s = f.read()
        if not s:
            ori_data = {}
        else:
            ori_data = json.loads(s)
        func(ori_data)


def main():
    Runner().run()


if __name__ == '__main__':
    main()
