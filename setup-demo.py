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
HUDI_FLINK_PORT_NAME_SET = {"hadoop_web_port", "hdfs_port", "historyserver_port", "hiveserver_port",
                            "spark_web_port", "spark_master_port", kafka_port_name, flink_jobmanager_port_name}
TIDB_START_PORT_OFFSET = len(HUDI_FLINK_PORT_NAME_SET) + HUDI_START_PORT_OFFSET
ticdc_port_name = 'ticdc_port'
tidb_port_name = 'tidb_port'
TIDB_PORT_NAME_SET = {"pd_port", "tikv_status_port",
                      tidb_port_name, ticdc_port_name}
tidb_compose_name = 'tidb-compose'
hufi_flink_compose = 'hufi-flink-compose'
env_file_path = "{}/.tmp.env.json".format(SCRIPT_DIR)
tmp_env_file_path = "{}/.tmp._env.json".format(SCRIPT_DIR)
HUDI_WS = 'HUDI_WS'
tidb_running_name = 'tidb_running'
hudi_flink_running_name = 'hudi_flink_running'
TIDB_BRANCH = 'TIDB_BRANCH'
demo_host = 'demo_host'
env_libs_name = 'env_libs'
start_port_name = 'start_port'
hadoop_name = "hadoop-2.8.4"
java_home_var_name = 'JAVA_HOME'
tidb_version_prefix = 'release-'
etl_jobs_name = "etl_jobs"
ticdc_changefeed_name = 'ticdc_changefeed'
flink_job_name = 'flink_job'
HDFS_NAME = 'hdfs'
CDC_BIN_PATH = 'CDC_BIN_PATH'
HUDI_FLINK_YML = '.tmp.docker-compose_hadoop_hive_spark_flink.yml'
HUDI_COMPILED_TIME = 'hudi_compiled'
COMPOSE_PROJECT_NAME = 'COMPOSE_PROJECT_NAME'


def get_host_name():
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip


class Runner:
    def __init__(self):
        lock_file = '{}/lock'.format(SCRIPT_DIR)
        assert os.path.exists(lock_file)
        self.unique_lock = open(lock_file, "r")
        fcntl.flock(self.unique_lock, fcntl.LOCK_EX)
        with open(env_file_path, 'a') as _:
            pass
        self._env_vars = self.load_env_data()
        self.funcs_map = {
            'deploy_hudi_flink': self.deploy_hudi_flink,
            'deploy_tidb': self.deploy_tidb,
            'deploy_hudi_flink_tidb': self.deploy_hudi_flink_tidb,
            'sink_task': self.sink_task,
            'down_hudi_flink': self.down_hudi_flink,
            'down_tidb': self.down_tidb,
            'compile_hudi': self.compile_hudi,
            'show_env_vars_info': self.show_env_vars_info,
            'down': self.down,
            'clean': self.clean,
            'list_ticdc_jobs': self.list_ticdc_jobs,
            'rm_ticdc_job': self.rm_ticdc_job,
            'parse_tso': self.parse_tso,
            'list_flink_jobs': self.list_flink_jobs,
            'rm_flink_job': self.rm_flink_job,
            'rm_hdfs_dir': self.rm_hdfs_dir,
            'list_etl_jobs': self.list_etl_jobs,
            'rm_etl_job': self.rm_etl_job,
            'dump_tidb_table': self.dump_tidb_table,
            'list_kafka_topics': self.list_kafka_topics,
            'rm_kafka_topic': self.rm_kafka_topic,
            'destroy': self.destroy,
            'list_cluster_env': self.list_cluster_env,
        }

    def list_cluster_env(self):
        files = [self.env_vars.get(hufi_flink_compose),
                 self.env_vars.get(tidb_compose_name)]
        for p in files:
            if not p or not os.path.exists(p):
                continue
            out, err, _ = run_cmd('docker compose -f {} ps -a'.format(p))
            logger.info("\n{}\n".format(out))
            if err:
                logger.error("\n{}\n".format(err))

    def destroy(self):
        self.clean()
        run_cmd('find {} -name ".tmp.*" | xargs rm -rf'.format(SCRIPT_DIR), True)
        logger.warning("all data is destroyed")

    def save_env_data(self, new_data):
        json.dump(new_data, open(tmp_env_file_path, 'w'))
        os.replace(tmp_env_file_path, env_file_path)
        logger.info("save env vars `{}` to `{}`".format(
            new_data, env_file_path))

    def load_env_data(self):
        data_file_handler = open(env_file_path, "r")
        s = data_file_handler.read()
        if not s:
            data = {}
        else:
            data = json.loads(s)
        return data

    @property
    def java_home(self):
        return self.env_vars.get(java_home_var_name)

    @java_home.setter
    def java_home(self, d):
        self.update_env_vars({java_home_var_name: d})

    def install_jdk1_8(self):
        env_java_home = os.getenv(java_home_var_name)
        if env_java_home is not None:
            logger.info("find env vars `{}`: `{}`".format(
                java_home_var_name, env_java_home))
            ok = True
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
                    pass
                else:
                    logger.error(
                        'got {}, expect javac 1.8.*'.format(out.strip()))
                    ok = False
            if ok:
                self.java_home = env_java_home
            else:
                exit(-1)
        else:
            assert os.path.exists(self.java_home)
            logger.info("using existed `{}`: `{}`".format(
                java_home_var_name, self.java_home))

        return self.java_home

    @property
    def hudi_repo_path(self):
        if self.args.hudi_repo is None:
            self.args.hudi_repo = self.env_vars.get(HUDI_WS)
        else:
            assert os.path.exists(self.args.hudi_repo)
            self.args.hudi_repo = os.path.realpath(self.args.hudi_repo)
        assert self.args.hudi_repo
        self.detect_change_and_update(HUDI_WS, self.args.hudi_repo)
        return self.args.hudi_repo

    @property
    def hudi_compiled_time(self):
        return self.env_vars.get(HUDI_COMPILED_TIME)

    @hudi_compiled_time.setter
    def hudi_compiled_time(self, v):
        self.detect_change_and_update(HUDI_COMPILED_TIME, v)

    def mvn_compile_hudi(self, java_home, hudi_path, need_clean,):
        _, stderr, retcode = run_cmd("export JAVA_HOME={} && cd {} && mvn {} package -Pintegration-tests -DskipTests -Drat.skip=true".format(
            java_home,
            hudi_path,
            "clean" if need_clean else ""), show_stdout=True)
        if retcode:
            logger.error(stderr)
            exit(-1)

    def __mvn_compile_hudi(self, *argv, **args):
        pass

    def compile_hudi(self):
        hudi_path = self.hudi_repo_path

        need_clean = True
        java_home = self.install_jdk1_8()
        if self.hudi_compiled_time is not None:
            logger.info('hudi was compiled at `{}`'.format(
                self.hudi_compiled_time))
            return

        self.mvn_compile_hudi(java_home, hudi_path, need_clean)
        # self.__mvn_compile_hudi()

        self.hudi_compiled_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.detect_change_and_update(HUDI_WS, hudi_path)
        logger.info('set hudi compiled time: {}'.format(
            self.hudi_compiled_time))

    def _init(self):
        parser = argparse.ArgumentParser(
            description="tidb ticdc flink hudi", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            '--hudi_repo', help='hudi git repo absolute path')
        parser.add_argument(
            '--env_libs', help='path to env binary and libs')
        parser.add_argument(
            '--start_port', help='start port for different components {}'.format(HUDI_FLINK_PORT_NAME_SET.union(TIDB_PORT_NAME_SET)))
        parser.add_argument(
            '--tidb_branch', help='tidb branch name: master, release-x.y, ...')
        parser.add_argument(
            '--sink_task_desc', help='format: `etl_uid.table_id.db_name.table_name`. sink tidb table to ticdc -> kafka -> flink -> hudi')
        parser.add_argument(
            '--sink_task_flink_schema_path', help='path to sql file include table schema for flink and hudi')
        parser.add_argument(
            '--cdc_changefeed_id', help="changefeed id of ticdc task. use `--cmd list_ticdc_jobs` to list all tasks"
        )
        parser.add_argument(
            '--tso', help="tso from PD"
        )
        parser.add_argument(
            "--changefeed_start_ts", help="Specifies the starting TSO of the changefeed. From this TSO, the TiCDC cluster starts pulling data. The default value is the current time",
        )
        parser.add_argument(
            "--start_ts", help="tso from PD for tidb dumpling",
        )
        parser.add_argument(
            "--db", help="database name for tidb dumpling",
        )
        parser.add_argument(
            "--table", help="table name for tidb dumpling",
        )
        parser.add_argument(
            "--flink_job_id", help="flink job id",
        )
        parser.add_argument(
            "--etl_job_id", help="tidb etl job id",
        )
        parser.add_argument(
            '--hdfs_url', help="hdfs url like `hdfs://namenode:8020/a/b/c`"
        )
        parser.add_argument(
            '--cdc_bin_path', help="ticdc binary local path"
        )
        parser.add_argument(
            '--flink_bin_path', help="flink binary local path"
        )
        parser.add_argument(
            '--hdfs_bin_path', help="hdfs binary local path"
        )
        parser.add_argument(
            '--ticdc_addr', help="external ticdc server address ip/host:port"
        )
        parser.add_argument(
            '--kafka_addr', help="external kafka server address: ip/host:port"
        )
        parser.add_argument(
            '--hdfs_addr', help="external hdfs server address: ip/host:port"
        )
        parser.add_argument(
            '--dumpling_bin_path', help="dumpling binary local path"
        )
        parser.add_argument(
            '--dumpling_tar_path', help="dir path to save dumped tidb table data"
        )
        parser.add_argument(
            '--flink_sql_client_path', help="path of flink sql client shell script"
        )
        parser.add_argument(
            '--compose_project_name', help="project name for docker compose env"
        )
        parser.add_argument(
            '--kafka_topic', help="kafka topic to be removed"
        )
        cmd_choices = set(self.funcs_map.keys())
        parser.add_argument(
            '--cmd', help='command enum', choices=cmd_choices, required=True)
        self.args = parser.parse_args()

    def _run_kafka_topic(self, args):
        cmd = 'docker compose -f {}/{} exec -T kafka bash -c "/opt/bitnami/kafka/bin/kafka-topics.sh --zookeeper zookeeper:2181 {}" '.format(
            SCRIPT_DIR, HUDI_FLINK_YML, args)
        out, err, retcode = run_cmd(cmd)
        if retcode:
            logger.error(
                "failed to run kafka topics cmd, error:\n{}\n".format(err))
            exit(-1)
        logger.info('\n{}\n'.format(out))

    def rm_kafka_topic(self):
        assert self.args.kafka_topic
        self._run_kafka_topic(
            '--delete --topic {}'.format(self.args.kafka_topic))

    def list_kafka_topics(self):
        self._run_kafka_topic('--list')

    def rm_etl_job(self):
        assert self.args.etl_job_id
        etl_jobs = self.env_vars.get(etl_jobs_name, {})
        job: dict = etl_jobs.get(self.args.etl_job_id)
        if job:
            logger.info("start to remove etl job `{}`:\n{}\n".format(
                self.args.etl_job_id, job))
            for tid, table in job.items():
                logger.info("start to clean data for table `{}`".format(tid))
                x = table.get(ticdc_changefeed_name)
                kafka_topic_to_remove = []
                if x is not None:
                    for a in x:
                        self.args.cdc_changefeed_id = a
                        self.rm_ticdc_job()
                        kafka_topic_to_remove.append(a)
                x = table.get(flink_job_name)
                if x is not None:
                    for a in x:
                        self.args.flink_job_id = a
                        self.rm_flink_job()
                x = table.get(HDFS_NAME)
                if x is not None:
                    for a in x:
                        self.args.hdfs_url = a
                        self.rm_hdfs_dir()
                for a in kafka_topic_to_remove:
                    self.args.kafka_topic = a
                    self.rm_kafka_topic()
            self.rm_etl(self.args.etl_job_id)
        else:
            logger.warning("etl job `{}` NOT found".format(
                self.args.etl_job_id))
            exit(-1)

    def list_etl_jobs(self):
        etl_jobs = self.env_vars.get(etl_jobs_name, {})
        logger.info("\n{}\n".format(etl_jobs))

    def parse_tso(self):
        assert self.args.tso
        cmd = "mysql -h 0.0.0.0 -P {} -u root -e 'SELECT TIDB_PARSE_TSO({})' ".format(
            self.env_vars[tidb_port_name], self.args.tso)
        out, err, ret = run_cmd(cmd)
        if ret:
            logger.error("tidb error:\n{}".format(err))
            exit(-1)
        else:
            logger.info('\n{}'.format(out))

    def list_ticdc_jobs(self):
        host = get_host_name()
        cdc_server = "http://{}:{}".format(host,
                                           self.env_vars[ticdc_port_name])
        ticdc_args = 'cli changefeed list --server={}'.format(
            cdc_server
        )
        cmd_env = {}
        if self.args.cdc_bin_path:
            assert os.path.exists(self.args.cdc_bin_path)
            cmd_env[CDC_BIN_PATH] = self.args.cdc_bin_path
        else:
            ticdc_args = "'{}'".format(ticdc_args)

        cmd = '{}/run-cdc-cli.sh {}'.format(
            SCRIPT_DIR, ticdc_args, )
        out, err, ret = run_cmd(cmd, env=cmd_env)
        if ret:
            logger.error(
                "failed to load ticdc tasks by ticdc client, error:\n{}".format(err))
            exit(-1)
        logger.info('\n{}'.format(out))

    def rm_ticdc_job(self):
        assert self.args.cdc_changefeed_id

        host = get_host_name()
        cdc_server = "http://{}:{}".format(host,
                                           self.env_vars[ticdc_port_name])
        ticdc_args = 'cli changefeed remove --server={} --changefeed-id={}'.format(
            cdc_server, self.args.cdc_changefeed_id
        )
        cmd_env = {}
        if self.args.cdc_bin_path:
            assert os.path.exists(self.args.cdc_bin_path)
            cmd_env[CDC_BIN_PATH] = self.args.cdc_bin_path
        else:
            ticdc_args = "'{}'".format(ticdc_args)
        cmd = '{}/run-cdc-cli.sh {}'.format(
            SCRIPT_DIR, ticdc_args)
        out, err, ret = run_cmd(cmd, env=cmd_env)
        if ret:
            logger.error(
                "failed to load ticdc tasks by ticdc client, error:\n{}".format(err))
            return
        logger.info('\n{}'.format(out))

    def update_env_vars(self, new_data):
        data = self.env_vars
        data.update(new_data)
        self.env_vars = data

    @property
    def start_port(self):
        if self.args.start_port is None:
            self.args.start_port = self.env_vars.get(start_port_name)
        assert self.args.start_port
        self.args.start_port = int(self.args.start_port)
        self.detect_change_and_update(start_port_name, self.args.start_port)
        assert self.args.start_port
        return self.args.start_port

    @start_port.setter
    def start_port(self, p):
        self.args.start_port = int(p)
        self.update_env_vars({start_port_name: self.args.start_port})

    @property
    def env_vars(self) -> dict:
        return self._env_vars

    @env_vars.setter
    def env_vars(self, t):
        self._env_vars = t
        self.save_env_data(self._env_vars)

    def clean(self):
        self.down()
        env_vars = self.env_vars
        files = [env_vars.get(hufi_flink_compose),
                 env_vars.get(tidb_compose_name)]
        for p in files:
            if p:
                logger.info("remove `{}`".format(p))
                if os.path.exists(p):
                    os.remove(p)

    @property
    def tidb_running(self):
        return self.env_vars.get(tidb_running_name, False)

    @tidb_running.setter
    def tidb_running(self, v):
        self.update_env_vars({tidb_running_name: v})

    def down(self):
        try:
            self.down_hudi_flink()
        except:
            pass
        try:
            self.down_tidb()
        except:
            pass

    def show_env_vars_info(self):
        print(self.env_vars)

    def down_hudi_flink(self):
        logger.info("start to down hudi flink docker compose")
        cmd = '{}/down_hudi_flink.sh'.format(SCRIPT_DIR)
        _, _, ret = run_cmd(cmd, True,)
        if ret:
            logger.error("fail to stop hudi flink")
            exit(-1)
        self.hudi_flink_running = False

    def down_tidb(self):
        logger.info("tidb ({}): start to down tidb docker compose".format(
            self.env_vars[TIDB_BRANCH]))
        cmd = '{}/stop_clean_tidb.sh'.format(SCRIPT_DIR)
        _, err, ret = run_cmd(cmd, True)
        if ret:
            logger.error("fail to stop tidb, error:\n{}".format(err))
            exit(-1)

        self.tidb_running = False

    @property
    def env_libs(self):
        if self.args.env_libs is None:
            self.args.env_libs = self.env_vars.get(env_libs_name)
        assert self.args.env_libs
        self.detect_change_and_update(env_libs_name, self.args.env_libs)
        return self.args.env_libs

    def setup_env_libs(self):
        env_libs = self.env_libs

        if not os.path.exists(env_libs):
            os.makedirs(env_libs)

        hadoop_path = os.path.join(env_libs, hadoop_name)
        hadoop_tar_name = '{}.tar.gz'.format(hadoop_name)
        hadoop_url = "{}/{}".format(DOWNLOAD_URL, hadoop_tar_name)

        flink_sql_connector_name = 'flink-sql-connector-kafka_2.12-1.13.6.jar'
        flink_sql_connector_url = "{}/{}".format(
            DOWNLOAD_URL, flink_sql_connector_name)
        hudi_flink_bundle_name = "hudi-flink-bundle_2.12-0.10.0.jar"
        hudi_flink_bundle_url = "{}/{}".format(
            DOWNLOAD_URL, hudi_flink_bundle_name)

        if not os.path.exists(hadoop_path):
            cmd = ['cd {} && mkdir -p .tmp'.format(env_libs),
                   'curl -o {} {}'.format(hadoop_tar_name, hadoop_url),
                   'rm -rf {} && rm -rf .tmp/{}'.format(
                       hadoop_name, hadoop_name),
                   'tar zxf {} -C .tmp'.format(hadoop_tar_name),
                   'mv .tmp/{} {}'.format(hadoop_name, hadoop_name),
                   'cp {}/hdfs-site.xml {}/etc/hadoop/hdfs-site.xml'.format(SCRIPT_DIR, hadoop_name)]
            _, _, status = run_cmd(' && ' .join(cmd))
            assert status == 0
        if not os.path.exists(os.path.join(env_libs, flink_sql_connector_name)):
            _, _, status = run_cmd("cd {} && wget {}".format(
                env_libs, flink_sql_connector_url))
            assert status == 0
        if not os.path.exists(os.path.join(env_libs, hudi_flink_bundle_name)):
            _, _, status = run_cmd("cd {} && wget {}".format(
                env_libs, hudi_flink_bundle_url))
            assert status == 0
            assert status == 0

    def detect_change_and_update(self, key, val):
        if self.env_vars.get(key) != val:
            self.update_env_vars({key: val})

    @property
    def tidb_branch(self):
        if self.args.tidb_branch is None:
            self.args.tidb_branch = self.env_vars.get(TIDB_BRANCH)
        assert self.args.tidb_branch
        assert self.args.tidb_branch == 'master' or self.args.tidb_branch.startswith(
            tidb_version_prefix)
        if self.args.tidb_branch.startswith(tidb_version_prefix):
            versions = self.args.tidb_branch[len(tidb_version_prefix):]
            vs = [int(v) for v in versions.split('.')]
            assert len(vs) == 2
        self.detect_change_and_update(TIDB_BRANCH, self.args.tidb_branch)
        return self.args.tidb_branch

    def deploy_tidb(self):
        tidb_branch = self.tidb_branch
        self.setup_env_libs()
        self.gen_tidb_cluster_config_file_from_template(
            self.start_port + TIDB_START_PORT_OFFSET, tidb_branch)
        if self.tidb_running:
            logger.info(
                "tidb ({}) is running, please stop tidb docker compose if necessary".format(tidb_branch))
            return
        else:
            logger.info("tidb is NOT running, start docker compose cluster")

        logger.info(
            "start to deploy tidb ({}) cluster".format(tidb_branch))

        for port_name in TIDB_PORT_NAME_SET:
            port = self.env_vars.get(port_name)
            if port is None:
                continue
            if is_port_occupied(port):
                logger.error("{}: {} is occupied".format(port_name, port))
                exit(-1)

        cmd = '{}/setup_tidb.sh'.format(SCRIPT_DIR)
        _, stderr, retcode = run_cmd(cmd, show_stdout=True,)
        if retcode:
            logger.error(
                "failed to deploy tidb cluster, error:\n{}".format(stderr))
            exit(-1)

        self.tidb_running = True

    def gen_tidb_cluster_config_file_from_template(self, start_port, branch):
        template_file = '{}/{}'.format(SCRIPT_DIR,
                                       'tidb-cluster.yml.template')
        logger.info(
            "start to gen tidb-ticdc cluster docker compose file: start_port={}, branch={}, template_file=`{}`".format(start_port, branch, template_file))
        config_file_path = '{}/{}'.format(SCRIPT_DIR,
                                          '.tmp.tidb-cluster.yml')
        if os.path.exists(config_file_path):
            logger.warning(
                'flink docker compose file `{}` exists, if need to generate new config, please delete it'.format(config_file_path))
            return

        template_context = load_file(template_file)
        template = Template(template_context)
        var_set = sorted(TIDB_PORT_NAME_SET)
        var_map = {}
        for i, v in enumerate(var_set):
            port = start_port+i
            if is_port_occupied(port):
                logger.error(
                    "port {} is occupied, please set new `start_port`".format(port))
                exit(-1)
            var_map[v] = port
        var_map[TIDB_BRANCH] = branch
        var_map['pingcap_demo_path'] = SCRIPT_DIR
        name = self.compose_project_name + "_tidb"
        all_compose_project_name = self.list_docker_compose_project()
        if name in all_compose_project_name:
            logger.error('docker compose project name `{}` is in use. current compose project names:\n{}\n'.format(
                name, all_compose_project_name))
            exit(-1)
        var_map['TIDB_COMPOSE_PROJECT_NAME'] = name
        logger.debug("set basic config: {}".format(var_map))
        with open(config_file_path, 'w') as f:
            f.write(template.substitute(var_map))
        logger.info(
            "gen docker compose config file `{}`".format(config_file_path))
        var_map[tidb_compose_name] = config_file_path
        self.update_env_vars(var_map)

    def list_docker_compose_project(self):
        cmd = 'docker compose ls -q'
        out, err, ret = run_cmd(cmd)
        if ret:
            logger.error(err)
            exit(-1)
        return set(out.strip().split('\n'))

    def gen_flink_config_file_from_template(self, start_port, hudi_path):
        template_file = '{}/{}'.format(SCRIPT_DIR,
                                       'docker-compose_hadoop_hive_spark_flink.yml.template')
        logger.info(
            "start to gen flink-hudi cluster docker compose file: start_port={}, hudi_root={}, template_file=`{}`".format(
                start_port, hudi_path, template_file))
        config_file_path = '{}/{}'.format(SCRIPT_DIR, HUDI_FLINK_YML)
        if os.path.exists(config_file_path):
            logger.warning(
                'flink docker compose file `{}` exists, if need to generate new config, please delete it'.format(config_file_path))
            return
        template_context = load_file(template_file)
        template = Template(template_context)
        var_set = sorted(HUDI_FLINK_PORT_NAME_SET)
        var_map = {}
        for i, v in enumerate(var_set):
            port = start_port+i
            if is_port_occupied(port):
                logger.error(
                    "port {} is occupied, please set new `start_port`".format(port))
                exit(-1)
            var_map[v] = port
        var_map[HUDI_WS] = hudi_path
        var_map['pingcap_demo_path'] = SCRIPT_DIR
        host = get_host_name()
        var_map[demo_host] = host
        var_map[env_libs_name] = self.args.env_libs
        name = self.compose_project_name + "_lakehouse"
        all_compose_project_name = self.list_docker_compose_project()
        if name in all_compose_project_name:
            logger.error('docker compose project name `{}` is in use. current compose project names:\n{}\n'.format(
                name, all_compose_project_name))
            exit(-1)
        var_map['LAKE_HOUSE_COMPOSE_PROJECT_NAME'] = name

        logger.debug("set basic config: {}".format(var_map))
        d = template.substitute(var_map)
        with open(config_file_path, 'w') as f:
            f.write(d)
        logger.info(
            "gen docker compose config file `{}`".format(config_file_path))
        var_map[hufi_flink_compose] = config_file_path
        self.update_env_vars(var_map)

    @property
    def hudi_flink_running(self):
        return self.env_vars.get(hudi_flink_running_name, False)

    @hudi_flink_running.setter
    def hudi_flink_running(self, v):
        self.update_env_vars({hudi_flink_running_name: v})

    @property
    def compose_project_name(self):
        if self.args.compose_project_name:
            assert self.args.compose_project_name
            self.detect_change_and_update(
                COMPOSE_PROJECT_NAME, self.args.compose_project_name)
        else:
            self.args.compose_project_name = self.env_vars.get(
                COMPOSE_PROJECT_NAME, 'pingcap')
        return self.args.compose_project_name

    def deploy_hudi_flink(self):
        hudi_path = self.hudi_repo_path
        assert os.path.exists(hudi_path)

        if not self.hudi_compiled_time:
            logger.error('please compile hudi before deploying cluster')
            exit(-1)

        self.setup_env_libs()

        self.gen_flink_config_file_from_template(
            self.start_port + HUDI_START_PORT_OFFSET, hudi_path)
        if self.hudi_flink_running:
            logger.info(
                "hudi flink is running, please stop hudi flink docker compose if necessary")
            return
        else:
            logger.info(
                "hudi flink is NOT running, start docker compose cluster")
        cmd = '{}/setup_hudi_flink.sh'.format(SCRIPT_DIR)
        _, stderr, retcode = run_cmd(
            cmd, show_stdout=True,)
        if retcode:
            logger.error(
                "failed to deploy hudi flink cluster, error:\n{}".format(stderr))
            exit(-1)
        self.hudi_flink_running = True

    def deploy_hudi_flink_tidb(self):
        self.deploy_hudi_flink()
        self.deploy_tidb()

    def gen_flink_exec(self, args):
        if self.args.flink_bin_path:
            assert os.path.exists(self.args.flink_bin_path)
            cmd = '{} {}'.format(
                self.args.flink_bin_path, args)
        else:
            cmd = '{}/run-flink-bash.sh /opt/flink/bin/flink {}'.format(
                SCRIPT_DIR, args)
        return cmd

    def rm_flink_job(self):
        assert self.args.flink_job_id
        cmd = self.gen_flink_exec('cancel {}'.format(self.args.flink_job_id))
        out, err, ret = run_cmd(
            cmd, False)
        if ret:
            logger.error(
                "failed to cancel job {} in flink.\nout:\n{}\nerror:\n{}\n".format(self.args.flink_job_id, out, err, ))
            return
        logger.info("\n{}\n".format(out))

    def list_flink_jobs(self):
        cmd = self.gen_flink_exec('list')
        out, err, ret = run_cmd(
            cmd, False)
        if ret:
            logger.error(
                "failed to list all jobs in flink, error:\n{}\n".format(err))
            exit(-1)
        logger.info("\n{}\n".format(out))

    def rm_hdfs_dir(self):
        assert self.args.hdfs_url
        if self.args.hdfs_bin_path:
            assert os.path.exists(self.args.hdfs_bin_path)
            cmd = '{} dfs -rm -r {}'.format(self.args.hdfs_bin_path,
                                            self.args.hdfs_url)
        else:
            cmd = '{}/run-flink-bash.sh {}'.format(
                SCRIPT_DIR, '/pingcap/env_libs/hadoop-2.8.4/bin/hdfs dfs -rm -r {}'.format(self.args.hdfs_url))
        out, err, ret = run_cmd(
            cmd, False, env={})
        if ret:
            logger.error(
                "failed to delete {}, out:\n{}\nerror:\n{}\n".format(self.args.hdfs_url, out, err))
            return
        logger.info("\n{}\n".format(out))

    def sink_task_check_tidb_schema(self, db, table_name):
        out, err, ret = run_cmd(
            "mysql -h 0.0.0.0 -P {} -u root -e 'desc {}.{}' ".format(self.env_vars[tidb_port_name], db, table_name))
        if ret:
            logger.error("tidb error:\n{}".format(err))
            exit(-1)
        else:
            logger.info('schema of `{}`.`{}` is:\n{}'.format(
                db, table_name, out))

    def create_ticdc_sink_job_to_kafka(self, host, etl_uid, table_id, db, table_name):
        ticdc_addr = '{}:{}'.format(
            host, self.env_vars[ticdc_port_name]) if self.args.ticdc_addr is None else self.args.ticdc_addr
        ticdc_server_url = "http://{}".format(ticdc_addr)
        kafka_addr = '{}:{}'.format(
            host, self.env_vars[kafka_port_name]) if self.args.kafka_addr is None else self.args.kafka_addr
        protocol = "canal-json"
        kafka_version = "2.4.0"
        partition_num = 1
        max_message_bytes = 67108864
        replication_factor = 1
        topic = '{}-sink-{}'.format(etl_uid, table_id)
        changefeed_id = topic
        cdc_config_file = gen_ticdc_config_file(
            etl_uid, table_id, db, table_name)
        cdc_config_file_name = '/pingcap/demo/{}'.format(os.path.basename(
            cdc_config_file)) if not self.args.cdc_bin_path else cdc_config_file
        logger.info('gen topic `{}`, changefeed-id `{}` for sink task `{}`'.format(
            topic, changefeed_id, self.args.sink_task_desc))
        ticdc_args = 'cli changefeed create --server={} --sink-uri="kafka://{}/{}?protocol={}&kafka-version={}&partition-num={}&max-message-bytes={}&replication-factor={}" --changefeed-id="{}" --config={}'.format(
            ticdc_server_url, kafka_addr, topic, protocol, kafka_version, partition_num, max_message_bytes, replication_factor, changefeed_id, cdc_config_file_name
        )
        if self.args.changefeed_start_ts:
            ticdc_args = "{} --start-ts={}".format(ticdc_args,
                                                   int(self.args.changefeed_start_ts))
        cmd_env = {}
        if self.args.cdc_bin_path:
            assert os.path.exists(self.args.cdc_bin_path)
            cmd_env[CDC_BIN_PATH] = self.args.cdc_bin_path
        else:
            ticdc_args = "'{}'".format(ticdc_args)

        cmd = '{}/run-cdc-cli.sh {}'.format(
            SCRIPT_DIR, ticdc_args)
        out, err, ret = run_cmd(cmd, True, cmd_env)
        if ret:
            logger.error(
                "failed to create table sink task by ticdc client, error:\n{}".format(err))
            exit(-1)

        stdout = out.split('\n')
        assert stdout[1] == 'ID: {}'.format(changefeed_id)
        prefix = 'Info: '
        assert stdout[2].startswith(prefix)
        ticdc_data = json.loads(stdout[2][len(prefix):])
        logger.info(
            "create table sink job by ticdc client:\n{}\n".format(ticdc_data))

        return topic, changefeed_id, ticdc_data

    def create_flink_job(self, host, etl_uid, table_id, db, table_name, topic, csv_output_path, hdfs_url):
        content = load_file(self.args.sink_task_flink_schema_path)
        kafka_addr = '{}:{}'.format(
            host, self.env_vars[kafka_port_name]) if self.args.kafka_addr is None else self.args.kafka_addr
        template = Template(content)
        var_map = {
            "kafka_address": kafka_addr,
            "kafka_topic": topic,
            "hdfs_address": hdfs_url,
            "csv_file_path": csv_output_path,
        }
        logger.debug("set basic config: {}".format(var_map))
        flink_sql_file = '.tmp.flink.sink-{}-{}-{}.{}.sql'.format(
            etl_uid, table_id, db, table_name)
        flink_sql_real_path = '{}/{}'.format(SCRIPT_DIR, flink_sql_file)
        flink_sql_path_in_docker = '/pingcap/demo/{}'.format(flink_sql_file)
        content = template.substitute(var_map)

        with open(flink_sql_real_path, 'w') as f:
            f.write(content)
        logger.info("save flink sink sql to `{}`".format(flink_sql_real_path))

        if self.args.flink_sql_client_path:
            assert os.path.exists(self.args.flink_sql_client_path)
            cmd = '{} -f {}'.format(
                self.args.flink_sql_client_path, flink_sql_real_path)
        else:
            cmd = '{}/run-flink-bash.sh /pingcap/demo/flink-sql-client.sh -f {}'.format(
                SCRIPT_DIR,
                flink_sql_path_in_docker)

        out, err, ret = run_cmd(cmd, False)
        if ret:
            logger.error(
                "failed to run flink sql by flink client, error:\n{}".format(err))
            exit(-1)
        job_id = None
        job_id_prefix = 'Job ID: '
        for line in out.split('\n'):
            line = line.strip()
            if line.startswith(job_id_prefix):
                job_id = line[len(job_id_prefix):]
            if line.find('[ERROR] ') != -1:
                job_id = None
                break
        if job_id:
            logger.info(
                "success to run flink sql by flink client, sql file path: `{}`, job_id: `{}`".format(flink_sql_real_path, job_id))
            logger.info(
                "please open flink jobmanager web site `http://{}:{}` for details".format(host, self.env_vars[flink_jobmanager_port_name]))
        else:
            logger.error(
                "failed to run flink sql by flink client, sql file path: `{}`, stdout:\n{}\n".format(flink_sql_real_path, out))
            exit(-1)

        return job_id

    def dump_tidb_table(self):
        assert self.args.db
        assert self.args.table
        if self.args.start_ts is None:
            self.args.start_ts = 0

        self._dump_tidb_table(self.args.start_ts,
                              self.args.db, self.args.table)

    def _dump_tidb_table(self, start_ts, db, table_name):
        dumpl_to_path = '/data'
        dumpl_to_path_real = '{}/.tmp.demo/tidb/data/dumpling'.format(
            SCRIPT_DIR)

        if self.args.dumpling_tar_path:
            assert self.args.dumpling_bin_path
            assert os.path.exists(self.args.dumpling_bin_path)
            self.args.dumpling_bin_path = os.path.realpath(
                self.args.dumpling_bin_path)
            dumpl_to_path = self.args.dumpling_tar_path
            dumpl_to_path_real = dumpl_to_path

        args = '-u root -P {} -h {} -o {} --filetype csv --snapshot {} --sql "select * from {}.{}" --output-filename-template "{}.{}" '.format(
            self.env_vars[tidb_port_name], get_host_name(), dumpl_to_path, start_ts, db, table_name, db, table_name)
        cmd_env = {}
        if self.args.dumpling_bin_path:
            assert self.args.dumpling_tar_path
            assert os.path.exists(self.args.dumpling_bin_path)
            cmd_env['DUMPLING_BIN_PATH'] = self.args.dumpling_bin_path
        else:
            args = "'{}'".format(args)

        cmd = "{}/run-dumpling.sh {} ".format(SCRIPT_DIR, args, )
        out, err, ret = run_cmd(cmd, False)
        if ret:
            logger.error("failed to dump table {}.{}, error:\n{}\nstdout:\n{}\n".format(
                db, table_name, err, out))
            exit(-1)

        csv_output_path = '{}/{}.{}.csv'.format(
            dumpl_to_path_real,
            db, table_name)
        logger.info("success to dump table {}.{} to `{}`".format(
            db, table_name, csv_output_path, ))
        cmd = 'sed -i "1d" {}'.format(csv_output_path)
        _, err, ret = run_cmd(cmd)
        if ret:
            logger.error(
                "failed to remove first row of {}, error:\n{}\n".format(csv_output_path, err))
            exit(-1)
        return csv_output_path

    def gen_hdfs_url(self, uri):
        hdfs_addr = 'namenode:8020' if self.args.hdfs_addr is None else self.args.hdfs_addr
        return "hdfs://{}/pingcap/demo/{}".format(hdfs_addr, uri)

    def sink_task(self):
        assert self.args.sink_task_desc
        assert self.args.sink_task_flink_schema_path

        _p = self.args.sink_task_desc.split('.')
        if len(_p) != 4:
            logger.error(
                "invalid sink format, need `etl_uid.table_id.db_name.table_name`")
        etl_uid, table_id, db, table_name = self.args.sink_task_desc.split('.')
        table_id = int(table_id)

        assert os.path.exists(self.args.sink_task_flink_schema_path)

        self.setup_env_libs()

        self.sink_task_check_tidb_schema(db, table_name)

        host = get_host_name()

        kafka_topic, changefeed_id, ticdc_data = self.create_ticdc_sink_job_to_kafka(
            host, etl_uid, table_id, db, table_name)

        start_ts = ticdc_data['start_ts']

        csv_path = self._dump_tidb_table(start_ts, db, table_name)
        if self.args.dumpling_tar_path:
            csv_output_path = csv_path
        else:
            assert csv_path.startswith(SCRIPT_DIR)
            csv_output_path = '/pingcap/demo/{}'.format(
                csv_path[len(SCRIPT_DIR):])

        hdfs_url = self.gen_hdfs_url(changefeed_id)
        job_id = self.create_flink_job(
            host, etl_uid, table_id, db, table_name, kafka_topic, csv_output_path, hdfs_url)

        self.save_etl(
            etl_uid,
            {
                table_id:
                {
                    ticdc_changefeed_name: [changefeed_id,], flink_job_name: [job_id,], HDFS_NAME: [hdfs_url,],
                }
            }
        )

    def save_etl(self, etl_uid, v):
        logger.info("save etl job `{}`:\n{}\n".format(etl_uid, v))
        data = self.env_vars
        etl_jobs = data.setdefault(etl_jobs_name, {})
        etl_jobs[etl_uid] = v
        self.env_vars = data

    def rm_etl(self, etl_uid):
        logger.info("remove etl job `{}`".format(etl_uid))
        data = self.env_vars
        etl_jobs = data.setdefault(etl_jobs_name, {})
        del etl_jobs[etl_uid]
        self.env_vars = data

    def run(self):
        self._init()
        func = self.funcs_map.get(self.args.cmd)
        if func is None:
            exit(-1)
        func()


def is_port_occupied(port):
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


def main():
    if os.getuid() != 0:
        logger.error("please checkout to root user")
        exit(-1)
    Runner().run()


if __name__ == '__main__':
    main()
