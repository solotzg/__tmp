#!/usr/bin/python3

import subprocess
import time

from .logger import logger


def wrap_run_time(func):
    def wrap_func(*args, **kwargs):
        bg = time.time()
        r = func(*args, **kwargs)
        logger.debug('`{}` time cost {:.3f}s'.format(
            func.__name__, time.time() - bg))
        return r

    return wrap_func


@wrap_run_time
def run_cmd(cmd, show_stdout=False, env=None):
    if env:
        logger.debug("\nRUN CMD:\n\t{}\nENV:\n\t{}\n".format(cmd, env))
    else:
        logger.debug("RUN CMD:\n\t{}\n".format(cmd))
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=env)
    stdout = None
    if show_stdout:
        stdout = bytes()
        for line in proc.stdout:
            stdout += (line)
            logger.debug(line.decode('utf-8').rstrip())
    _stdout, stderr = proc.communicate()
    stdout = stdout if show_stdout else _stdout
    return stdout.decode('utf-8'), stderr.decode('utf-8'), proc.returncode


def run_cmd_no_msg(cmd, env=None):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=env)
    stdout, stderr = proc.communicate()
    return stdout.decode('utf-8'), stderr.decode('utf-8'), proc.returncode
