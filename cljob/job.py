import os.path

class ShellJob(object):
    def __init__(self, host, cmd, wdir = ''):
        self.host = host
        self.cmd = cmd
        self.wdir = wdir

        self.proc = None
        self.retcode = None
        self.stdout = None
        self.stderr = None

        self.exception = None
        self.trace = None

        self.timeouted = False

    def __str__(self):
        return 'ShellCmd %s:%s %s' % (self.host, self.wdir, self.cmd)

class UploadJob(object):
    def __init__(self, host, files, target = ''):
        self.host = host
        self.files = files
        self.wdir = target

        self.proc = None
        self.retcode = None
        self.stderr = None

        self.exception = None
        self.trace = None

        self.timeouted = False

    def __str__(self):
        return 'Upload to %s:%s' % (self.host, self.wdir)

class DownloadJob(object):
    def __init__(self, host, files, target, base_dir=''):
        self.host = host
        self.files = files
        self.target = target
        self.wdir = base_dir

        self.proc = None
        self.retcode = None
        self.stderr = None

        self.exception = None
        self.trace = None

        self.timeouted = False

    def __str__(self):
        return 'Download from %s:%s' % (self.host, self.wdir)

def job_to_str(job):
    return str(job)

def job_host(job):
    return job.host

def job_host_path(job):
    wdir = job.wdir
    if len(wdir) > 0 and wdir[len(wdir)-1] == os.path.sep:
        wdir = wdir[:-1]
    return '%s:%s' % (job.host, wdir)

def job_path(job):
    if job.wdir == '':
        return job.host
    return '%s:%s' % (job.host, job.wdir)

