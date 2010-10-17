#!/usr/bin/env python

import os
import signal
import re
from subprocess import Popen, PIPE
from time import time, sleep
from sys import exc_info
from traceback import format_tb
from optparse import make_option

def search_path(executable):
    """
    Find rsh on the PATH
    """
    for i in os.environ.get('PATH', os.defpath).split(os.pathsep):
        path = os.path.join(i, executable)
        if os.access(path, os.X_OK):
            return path
    return None

def make_options():
    return [
        make_option('--timeout', dest='timeout', action='store', \
                    type='int', default=30, metavar='SECONDS',    \
                    help='timeout in seconds. Zero means no timeout, default is 5 seconds'),
        make_option('--check-interval', dest='check_interval', action='store',   \
                    type='float', default=0.1, metavar='SECONDS',                \
                    help='time in seconds between two iterations, 0.1 default'),
        make_option('--max-simultanious-jobs', dest='max_simultanious_jobs',     \
                    action='store', type='int', default=200, metavar='NUM',        \
                    help='maximum number of simultaious running jobs, zero means no limit'),
    ]

def parse_options(options):
    return {
        'timeout': options.timeout,
        'check_interval': options.check_interval,
        # subprocess.Popen() -> communicate can't handle more than 512 simultanious processes
        'max_simultanious_jobs': min(options.max_simultanious_jobs, 510),
    }

def _run_rsh_jobs(jobs, start_job_func, end_job_func, timeout=10,         \
                                                      check_interval=0.1, \
                                                      max_simultanious_jobs = 0):
    cur_jobs = []
    jobs_stack = jobs
    if timeout == 0:
        timeout = None

    def run_jobs_from_stack(jobs_cnt):
        new_running_jobs = []
        failed_jobs = []
        while len(new_running_jobs) < jobs_cnt and len(jobs_stack) > 0:
            job = jobs_stack.pop()
            try:
                job.proc = start_job_func(job)
                new_running_jobs.append(job)
            except Exception as ex:
                job.exception = ex
                job.trace = ''.join(format_tb(exc_info()[2]))
                job.proc = None
                failed_jobs.append(job)
        return new_running_jobs, failed_jobs

    def terminate_job(job):
        try:
            os.kill(job.proc.pid, signal.SIGTERM)
        except Exception:
            try:
                os.kill(job.proc.pid, signal.SIGKILL)
            except Exception as ex2:
                job.exception = ex2
                job.trace = ''.join(format_tb(exc_info()[2]))

    if max_simultanious_jobs == 0:
        max_simultanious_jobs = len(jobs)
    cur_jobs, failed_jobs = run_jobs_from_stack(max_simultanious_jobs)
    for job in failed_jobs:
        yield job

    jobs = cur_jobs

    start_time = time()
    while True:
        if timeout != None and time() - start_time >= timeout:
            # exit by timeout
            for job in jobs:
                job.timeouted = True
                terminate_job(job)
                yield job
            break

        cur_jobs = []
        for job in jobs:
            retcode = job.proc.poll()
            if retcode == None:
                # job doesn't done jet
                cur_jobs.append(job)
                continue

            job.retcode = retcode
            end_job_func(job)
            yield job

        new_jobs, failed_jobs = run_jobs_from_stack(max_simultanious_jobs - len(cur_jobs))
        cur_jobs += new_jobs
        for job in failed_jobs:
            yield job
        jobs = cur_jobs
        if len(jobs) == 0:
            # all jobs done
            break
        sleep(check_interval)

def run_shell_jobs(jobs, **args):
    """
    Run shell cmds on remote hosts.
    """
    def start_job_func(job):
        cmd = job.cmd
        if job.wdir != '':
            cmd = 'mkdir -p "%s" && cd "%s" && (%s)' % (job.wdir, job.wdir, cmd)
        # this is an ugly hack to get exit codes from rsh :(
        cmd = '(set -o pipefail; set -u; set -e;\n%s\n); echo $?' % cmd
        return Popen(['rsh', job.host, cmd], stdout=PIPE, stderr=PIPE, close_fds = True)

    def end_job_func(job):
        job.stdout, job.stderr = [ out.strip() for out in job.proc.communicate() ]
        if job.retcode == 0:
            # rsh exit normally, get actual cmd exit code from the last line of output
            last_newline = job.stdout.rfind('\n')
            if last_newline != -1:
                job.retcode = int(job.stdout[last_newline+1:])
                job.stdout = job.stdout[:last_newline]
            elif re.match('^\d+$', job.stdout) != None:
                job.retcode = int(job.stdout)
                job.stdout = ''
        return

    for job in _run_rsh_jobs(jobs, start_job_func, end_job_func, **args):
        yield job

def run_upload_jobs(jobs, **args):
    def start_job_func(job):
        target = '%s:%s' % (job.host, job.wdir)
        return Popen(['rsync', '-qaz'] + job.files + [target], stderr=PIPE, close_fds = True)

    def end_job_func(job):
        _, stderr = job.proc.communicate()
        job.stderr = stderr.strip()

    for job in _run_rsh_jobs(jobs, start_job_func, end_job_func, **args):
        yield job

def run_download_jobs(jobs, **args):
    def start_job_func(job):
        rsync_cmd = [ 'rsync', '-qazR' ]
        rsync_cmd += [ '--rsync-path=cd \'%s\' && rsync' % job.wdir ]
        rsync_cmd += [ '%s:' % job.host ]
        rsync_cmd += [ ':%s' % fname for fname in job.files ]
        rsync_cmd += [ job.target ]
        return Popen(rsync_cmd, stderr=PIPE, close_fds = True)

    def end_job_func(job):
        _, stderr = job.proc.communicate()
        job.stderr = stderr.strip()

    for job in _run_rsh_jobs(jobs, start_job_func, end_job_func, **args):
        yield job

