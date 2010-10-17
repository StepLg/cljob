import sys

from job import job_to_str

class MergeExceptions(object):
    def __init__(self, outfile = sys.stdout, max_jobs_num = 5, job_to_str_func = job_to_str):
        self.outfile = outfile
        self.max_jobs_num = max_jobs_num
        self.errors = {}
        self.job_to_str_func = job_to_str

    def __call__(self, job):
        ex = job.exception
        trace = job.trace

        if ex == None:
            return

        ex_hash = exception_hash(ex, trace)
        if ex_hash not in self.errors:
            self.errors[ex_hash] = {
                'exception': ex,
                'trace': trace,
                'jobs': set()
            }

        self.errors[ex_hash]['jobs'].add(job)

    def finish(self):
        """
        Print errors from Merger errors handler.

        sections -- map from Merger.get_errors()
        max_jobs_num -- maximum hosts to display. If <0 display all hosts
        outfile -- where to write
        """
        for msg, info in self.errors.iteritems():
            hosts = [ self.job_to_str_func(job) for job in info['jobs'] ]

            max_jobs_num = self.max_jobs_num
            if max_jobs_num < 0 or max_jobs_num > len(hosts):
                hosts_msg = ': %s' % ' '.join(hosts)
            elif max_jobs_num == 0:
                hosts_msg = ''
            else:
                hosts_msg = ': %s (and %s more)' % (' '.join(sorted(hosts)[:self.max_jobs_num]), \
                                                    len(hosts) - self.max_jobs_num)

            ex = info['exception']
            msg = '%s.%s: %s' % (ex.__class__.__module__, \
                                 ex.__class__.__name__,   \
                                 str(ex).split('\n')[0])
            print >> self.outfile, "Exception '%s' in %s jobs%s." % (msg, len(hosts), hosts_msg)
            print >> self.outfile, exception_description(ex).strip()
            if info['trace'] != None:
                print >> self.outfile, 'Traceback:'
                print >> self.outfile, ''.join(info['trace'])

            print >> self.outfile

class PrintExceptions(object):
    def __init__(self, job_to_str_func = job_to_str, outfile = sys.stdout):
        self.outfile = outfile
        self.job_to_str_func = job_to_str_func

    def __call__(self, job):
        ex = job.exception
        trace = job.trace

        if ex == None:
            return

        job_info = self.job_to_str_func(job)
        msg = '%s.%s: %s' % (ex.__class__.__module__, \
                             ex.__class__.__name__,   \
                             str(ex).split('\n')[0])
        print >> self.outfile, "Exception '%s' in job %s." % (msg, job_info)
        print >> self.outfile, exception_description(ex).strip()
        if trace != None:
            print >> self.outfile, 'Traceback:'
            print >> self.outfile, ''.join(trace)

        print >> self.outfile

class MergeOutput(object):
    def __init__(self, job_to_str_func = job_to_str, \
                       outfile = sys.stdout,         \
                       max_jobs_num = 5):
        self.outfile = outfile
        self.job_to_str_func = job_to_str_func
        self.max_jobs_num = max_jobs_num
        self.outputs = {}

    def __call__(self, job):
        if job.exception != None:
            return

        if job.retcode != 0:
            return

        out = ''
        job.stdout = job.stdout.strip()
        job.stderr = job.stderr.strip()
        if job.stdout != '':
            out += job.stdout
        if job.stdout != '' and job.stderr != '':
            out += '\n%s\n' % ('='*80)
        if job.stderr != '':
            out += job.stderr

        if out not in self.outputs:
            self.outputs[out] = []

        self.outputs[out].append(job)

    def finish(self):
        for output, jobs in self.outputs.iteritems():
            jobs_info = set([ self.job_to_str_func(job) for job in jobs ])
            if self.max_jobs_num < 0 or len(jobs) <= self.max_jobs_num:
                jobs_info = ': %s' % ' '.join(sorted(jobs_info))
            elif self.max_jobs_num == 0:
                jobs_info = ':'
            else:
                jobs_info = ': %s (and %s jobs more)' % (' '.join(sorted(jobs_info)[:self.max_jobs_num]),
                                                          len(jobs) - self.max_jobs_num)

            print >> self.outfile, 'Output from %s jobs%s\n%s' % (len(jobs), jobs_info, output)
            print >> self.outfile

class PrintOutput(object):
    def __init__(self, job_to_str_func = job_to_str, outfile = sys.stderr):
        self.outfile = outfile
        self.job_to_str_func = job_to_str_func

    def __call__(self, job):
        ex = job.exception
        trace = job.trace

        if ex != None:
            return

        if job.retcode != 0:
            return

        out = ''
        job.stdout = job.stdout.strip()
        job.stderr = job.stderr.strip()
        if job.stdout != '':
            out += job.stdout
        if job.stdout != '' and job.stderr != '':
            out += '\n%s\n' % ('='*80)
        if job.stderr != '':
            out += job.stderr

        host_info = self.job_to_str_func(job)
        print >> self.outfile, 'Output from %s:\n%s\n' % (host_info, out)

class MergeErrors(object):
    def __init__(self, job_to_str_func = job_to_str, \
                       outfile = sys.stderr,         \
                       max_jobs_num = 5):
        self.outfile = outfile
        self.job_to_str_func = job_to_str_func
        self.max_jobs_num = max_jobs_num
        self.outputs = {}

    def __call__(self, job):
        if job.exception != None:
            return

        if job.retcode == 0:
            return

        out = '%s:%s' % (job.retcode, job.stderr)
        if 'stdout' in dir(job) and job.stdout != None and job.stdout != '':
            out = '%s\n%s' % (out, job.stdout)

        if out not in self.outputs:
            self.outputs[out] = {
                'retcode': job.retcode,
                'stderr': job.stderr,
                'stdout': '',
                'jobs': [],
            }
            if 'stdout' in dir(job) and job.stdout != None and job.stdout != '':
                self.outputs['stdout'] = job.stdout

        self.outputs[out]['jobs'].append(job)

    def finish(self):
        for _, info in self.outputs.iteritems():
            jobs = info['jobs']
            jobs_info = set([ self.job_to_str_func(job) for job in jobs ])
            if self.max_jobs_num < 0 or len(jobs) <= self.max_jobs_num:
                jobs_info = ': %s' % ' '.join(sorted(jobs_info))
            elif self.max_jobs_num == 0:
                jobs_info = ':'
            else:
                jobs_info = ': %s (and %s jobs more)' % (' '.join(sorted(jobs_info)[:self.max_jobs_num]),
                                                          len(jobs) - self.max_jobs_num)

            if info['retcode'] == None:
                # job failed by timeout
                print >> self.outfile, 'Failed by timeout %s jobs: %s' % (len(jobs), jobs_info)
            else:
                print >> self.outfile, 'Fail with code %s in %s jobs%s' % (info['retcode'], len(jobs), jobs_info)
                print >> self.outfile, 'Stderr: %s' % info['stderr'].replace('\n', '\n\t')
                if info['stdout'] != '':
                    print >> self.outfile, 'Stdout: %s' % info['stdout'].replace('\n', '\n\t')
                print >> self.outfile

class PrintErrors(object):
    def __init__(self, job_to_str_func = job_to_str, outfile = sys.stderr):
        self.outfile = outfile
        self.job_to_str_func = job_to_str_func

    def __call__(self, job):
        ex = job.exception
        trace = job.trace

        if ex != None:
            return

        if job.retcode == 0:
            return

        out = ''
        stdout = ''
        if 'stdout' in dir(job) and job.stdout != None:
            stdout = job.stdout.strip()
        job.stderr = job.stderr.strip()
        if job.stderr != '':
            out += job.stderr
        if stdout != '' and stderr != '':
            out += '\n%s\n' % ('='*80)
        if stdout != '':
            out += stdout

        host_info = self.job_to_str_func(job)
        print >> self.outfile, 'Fail with code %s in %s job.' % (job.retcode, host_info)
        print >> self.outfile, 'Stderr: %s' % job.stderr.replace('\n', '\n\t')
        if job.stdout != '':
            print >> self.outfile, 'Stdout: %s' % job.stdout.replace('\n', '\n\t')
        print >> self.outfile

def exception_hash(err, traceback = None):
    """
    Format exception as a str hash.
    """
    result = ''
    if isinstance(err, str):
        result = "str: %s" % err
    else:
        if traceback == None:
            traceback = "\nNone\n"
        else:
            traceback = '\n' + traceback
        result = "%s.%s: %s%s" % (err.__class__.__module__, \
                                  err.__class__.__name__,   \
                                  str(err), traceback)
    return result

def exception_description(err):
    """
    Format exception description to display.
    """
    result = ''
    if isinstance(err, str):
        result = err
    elif isinstance(err, Exception):
        result = "Exception class: %s.%s\n" % (err.__class__.__module__, \
                                               err.__class__.__name__)
        if len(err.args) > 0:
            result += "Args:\n"
            arg_num = 0
            for arg in err.args:
                if not isinstance(arg, str):
                    arg = str(arg)

                arg = arg.replace('\n', '\n\t' + ' '*(len(str(arg_num)) + 3))

                result += "\t%s : %s\n" % (arg_num, arg)
                arg_num += 1
    else:
        result = str(err)
    return result

class JobStatuses(object):
    def __init__(self):
        self.stat = {
            'ok': 0,
            'retcode': 0,
            'exception': 0,
        }

    def __call__(self, job):
        target = 'ok'
        if job.exception != None:
            target = 'exception'
        elif job.retcode != 0:
            target = 'retcode'

        self.stat[target] += 1

    def get_statuses(self):
        return self.stat

class ProgressBar(object):
    def __init__(self, pbar):
        self.pbar = pbar
        self.pbar.start()
        self.next_job_id = 1

    def __call__(self, job):
        self.pbar.update(self.next_job_id)
        self.next_job_id += 1

    def finish(self):
        self.pbar.finish()
        print

class DoneJobsToFile(object):
    def __init__(self, fname, job_formatter_func):
        self.job_formatter_func = job_formatter_func
        self.outfile = open(fname, 'w')

    def __call__(self, job):
        if job.exception != None or job.retcode != 0:
            return

        print >> self.outfile, self.job_formatter_func(job)

class FailedJobsAppendFile(object):
    def __init__(self, fname, job_formatter_func):
        self.job_formatter_func = job_formatter_func
        self.outfile = open(fname, 'a')

    def __call__(self, job):
        if job.exception == None and job.retcode == 0:
            return

        print >> self.outfile, self.job_formatter_func(job)

