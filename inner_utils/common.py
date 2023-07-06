#!/usr/bin/env python3

import subprocess
import time

from .logger import logger, std_logger


def wrap_run_time(func):
    def wrap_func(*args, **kwargs):
        bg = time.time()
        r = func(*args, **kwargs)
        logger.debug('`{}` time cost {:.3f}s'.format(
            func.__name__, time.time() - bg))
        return r

    return wrap_func


def run_cmd_no_debug_info(cmd, show_stdout=False, env=None, no_error=False):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=env)
    if show_stdout:
        stdout = bytes()
        for line in proc.stdout:
            stdout += (line)
            std_logger.debug(line.decode('utf-8').rstrip())
        _, stderr = proc.communicate()
    else:
        stdout, stderr = proc.communicate()

    stdout, stderr, ret = stdout.decode(
        'utf-8'), stderr.decode('utf-8'), proc.returncode

    if no_error:
        if ret:
            logger.error(
                "Failed to run command `{}`.\n\nError:\n{}\nSTD Out:\n{}\n".format(cmd, stderr, stdout))
            exit(-1)
    return stdout, stderr, ret


@wrap_run_time
def run_cmd(cmd, show_stdout=False, env=None, *args, **argv):
    msg = ("\nRUN CMD:\n\t{}\nENV:\n\t{}\n".format(cmd, env)
           ) if env else ("RUN CMD:\n\t{}\n".format(cmd))
    logger.debug(msg)

    stdout, stderr, ret = run_cmd_no_debug_info(
        cmd, show_stdout=show_stdout, env=env, *args, **argv)

    return stdout, stderr, ret
