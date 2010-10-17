"""
Helper functions for host filters and args in tsky utils.
"""

import re
from optparse import make_option
import os.path
from fnmatch import fnmatch
from progressbar import ProgressBar
from getpass import getuser

import handler
import job

def resolve_host_path(host_path, default_path):
    """
    Split 'host[:path]' to host and path, expanding no path to default path
    and relative path to default_path/relative_path

    host_path -- host with optional path in format 'host[:path]'
    default_path -- default path
    """
    chunks = host_path.split(':')
    if len(chunks) == 1:
        if default_path == None:
            raise Exception("No path for host %s." % host_path)
        host, path = chunks[0], default_path
    elif len(chunks) == 2:
        host, path = chunks
        if not os.path.isabs(path):
            if default_path != None:
                path = os.path.join(default_path, path)
    else:
        raise Exception("Wrong host:path format.", host_path)

    return host, path

def parse_host_paths(hosts, default_path):
    """
    Expand list of hosts to dictionary with paths on each host

    hosts -- filter string
    default_path -- default path

    >>> r = parse_host_paths('ws1-400:p1 ws1-400:/p2 ws1-400 sdf150:/p1/p2', '/dp')
    >>> sorted(r.keys())
    ['sdf150', 'ws1-400']
    >>> sorted(r['ws1-400'])
    ['/dp', '/dp/p1', '/p2']
    >>> sorted(r['sdf150'])
    ['/p1/p2']
    """
    if hosts == None:
        return {}
    hosts = [x.strip() for x in re.split('[ ,]+', hosts)]
    hosts = filter(lambda x: len(x) > 0, hosts)
    hosts = set(hosts)
    result = {}
    for host in hosts:
        host, path = resolve_host_path(host, default_path)
        if host not in result:
            result[host] = set()

        result[host].add(path)
    return result

def parse_host_paths_file(file_hnd, default_path):
    """
    Read host paths from file and return dict with paths for each host.

    Blank lines and lines started with '#' are skipping
    file_hnd -- file name or open file resource for reading
    default_path -- default path
    """
    hosts_filter = ""
    if isinstance(file_hnd, str):
        file_hnd = open(file_hnd, 'r')

    for line in file_hnd:
        line = line.strip()
        if len(line) == 0 or line[0] == '#':
            continue
        hosts_filter += ' ' + line.strip()

    return parse_host_paths(hosts_filter, default_path)

def parse_host_paths_file_cmds(file_hnd, default_path, default_cmd = None):
    """
    Read host paths with commands as a second tab-separated field from file.

    Return dict with host as a key and dict of path->cmd as a value.

    Blank lines and lines started with '#' are skipping

    file_hnd -- file name or open file resource for reading
    default_path -- default path for hosts without :path
    default_cmd -- default command for lines without second field
    """
    result = {}
    if isinstance(file_hnd, str):
        file_hnd = open(file_hnd, 'r')

    for line in file_hnd:
        line = line.strip()
        if len(line) == 0 or line[0] == '#':
            continue

        chunks = line.split('\t')
        host, path = resolve_host_path(chunks[0], default_path)
        if len(chunks) == 1:
            if default_cmd == None:
                raise Exception("Cmd doesn't set for host %s." % line)
            cmd = default_cmd
        elif len(chunks) == 2:
            cmd = chunks[1]
        else:
            raise Exception("Wrong fields number on line '%s'." % line)

        if host not in result:
            result[host] = {}

        if path in result[host]:
            raise Exception("Duplicate path '%s' for host '%s'." % (path, host))

        result[host][path] = cmd

    return result

def parse_host_paths_filter(hosts_filter, default_path):
    """
    Read hosts filter and return dict with paths for each host

    hosts_filter -- str like '+SEARCH1 +SDF:/some/path -ws1-400:relative/path'
    default_path -- default path
    """
    chunks = [x.strip() for x in hosts_filter.split(' ')]
    chunks = filter(lambda x: len(x) > 0, chunks)
    chunks = set(chunks)
    incl_hosts = []
    excl_hosts = []
    for chunk in chunks:
        if chunk[0] == '+':
            incl_hosts.append(chunk[1:])
        elif chunk[0] == '-':
            excl_hosts.append(chunk[1:])
        else:
            raise Exception("Can't parse host paths filter '%s' on chunk: '%s'" % (filter, chunk))

    incl_hosts = parse_host_paths(' '.join(incl_hosts), default_path)
    excl_hosts = parse_host_paths(' '.join(excl_hosts), default_path)
    return incl_hosts, excl_hosts

def filter_host_paths(incl_hosts, excl_hosts):
    """
    Remove from incl_hosts all paths from excl_hosts

    This filter supports Unix shell-style wildcards in excl_hosts, so if there is
    /some/path/* in excl_hosts, then all paths in incl_hosts started with /some/path/
    will be excluded.
    incl_hosts -- dictionary with paths for hosts
    excl_hosts -- dictionary with paths for hosts to exclude from incl_hosts

    >>> incl_hosts = {'h1': ['/p1', '/p2/p3', '/p4'], 'h2': ['/p1/p2']}
    >>> excl_hosts = {'h1': ['/p1', '/p4', '/p5'], 'h2': ['/p1/p2'], 'h3': ['/some/path']}
    >>> r = filter_host_paths(incl_hosts, excl_hosts)
    >>> sorted(r.keys())
    ['h1']
    >>> sorted(r['h1'])
    ['/p2/p3']
    """
    for host, paths in excl_hosts.iteritems():
        if host not in incl_hosts:
            continue
        incl_hosts[host] = set(incl_hosts[host]) - set(paths)
        incl_paths = incl_hosts[host].copy()
        for incl_path in incl_paths:
            for excl_path in paths:
                if excl_path[-1:] == os.path.sep:
                    excl_path = excl_path[:-1]
                if fnmatch(incl_path, excl_path):
                    incl_hosts[host].remove(incl_path)
                    break
                if len(excl_path) <= len(incl_path):
                    if incl_path[:len(excl_path)] == excl_path:
                        incl_hosts[host].remove(incl_path)
        if len(incl_hosts[host]) == 0:
            del incl_hosts[host]

    return incl_hosts

def implode_host_paths(paths1, paths2):
    """
    Implode two dictionary with host paths

    paths1 -- dict with paths
    paths2 -- dict with paths
    >>> paths1 = {'h1': ['/p1', '/p2/p3', '/p4'], 'h2': ['/p1/p2']}
    >>> paths2 = {'h1': ['/p1', '/p4', '/p5'], 'h2': ['/p1/p2'], 'h3': ['/some/path']}
    >>> r = implode_host_paths(paths1, paths2)
    >>> sorted(r.keys())
    ['h1', 'h2', 'h3']
    >>> sorted(r['h1'])
    ['/p1', '/p2/p3', '/p4', '/p5']
    >>> sorted(r['h2'])
    ['/p1/p2']
    >>> sorted(r['h3'])
    ['/some/path']
    """
    for host, paths in paths2.iteritems():
        if host not in paths1:
            paths1[host] = paths
        else:
            paths1[host] = set(paths1[host]) | set(paths)

    return paths1

def make_host_options():
    """
    Make t:e:T:E:f:u:U: host options for options parser
    """
    return [
        make_option('-t', '--hosts', dest='hosts', metavar='HOSTS',   \
                    type='string', action='append', default=[],       \
                    help='list of target hosts (comma or space separated): \
                          ws1-400, sdf150 sdf151'),
        make_option('-e', '--exclude-hosts', dest='exclude_hosts', metavar='HOSTS',    \
                    type='string', action='append', default=[],       \
                    help='list of hosts to exclude (comma or space separated): \
                          ws1-400, sdf150 sdf151'),
        make_option('-T', '--hosts-file', dest='file_hosts', metavar='FILE', \
                    type='string', action='append', default=[],              \
                    help='file with target hosts, with one host per line'),
        make_option('-E', '--exclude-hosts-file', dest='file_exclude_hosts',  \
                    help='file with hosts to exclude with one host per line', \
                    metavar='FILE', type='string', action='append', default=[]),
        make_option('-f', '--hosts-filter', dest='hosts_filter',  \
                    help='hosts filter like "+SEARCH1 -ws1-400"', \
                    metavar='FILTER', type='string', action='append', default=[]),
    ]

def make_output_options():
    """
    Make qu:U: --no-merge-err, --max-jobs-err=, --quiet options for output control
    """
    return [
        make_option('--max-jobs-err', dest='max_jobs_err', metavar='NUM',   \
                    type='int', action='store', default=5,                    \
                    help='maximum number of jobs to output in  \
                          merged errors. If <0 then there is no limit'),
        make_option('--no-merge-err', dest='merge_err', action='store_false', \
                    default=True, help="don't merge errors by error text"),
        make_option('-u', '--update-hosts-file', dest='update_hosts_file', \
                    metavar='FILE', type='string', \
                    help='update hosts file with list of non-failed hosts'),
        make_option('-U', '--append-failed-hosts', dest='append_failed_hosts', \
                    metavar='FILE', type='string', \
                    help='append list of failed hosts to FILE'),
        make_option('-q', '--quiet', dest='quiet', action='store_true', \
                    default=False, help="don't output information about errors"),
        make_option('--no-pbar', dest='pbar', action='store_false', default=True, \
                    help='disable progress bar'),
    ]

def check_options(options):
    """
    Check if at least one host option is set.

    For option names see add_hosts_options()
    If none of host options is set return None

    options -- options structure, returned by options parser
    """
    if len(options.hosts) == 0 and              \
       len(options.file_hosts) == 0 and         \
       len(options.exclude_hosts) == 0 and      \
       len(options.file_exclude_hosts) == 0 and \
       len(options.hosts_filter) == 0:
        return False

    return True

def update_host_files(options, ok_hosts, failed_hosts):
    """
    Update ok and failed hosts according to options from make_output_options()

    options -- options struct from make_host_options()
    ok_hosts -- list of hosts which are not failed
    failed_hosts -- failed hosts
    """
    if options.update_hosts_file != None:
        hosts_file = open(options.update_hosts_file, 'w')
        hosts_file.write('\n'.join(sorted(ok_hosts)))
        if len(ok_hosts) > 0:
            hosts_file.write('\n')
        hosts_file.close()

    if options.append_failed_hosts != None:
        hosts_file = open(options.append_failed_hosts, 'a')
        hosts_file.write('\n'.join(sorted(failed_hosts)))
        if len(failed_hosts) > 0:
            hosts_file.write('\n')
        hosts_file.close()

def parse_host_options(options, default_path):
    """
    Return dictionary with paths for host from all filters in options parser.

    For option names see add_hosts_options(). If none of host options is set return None.

    options -- options structure, returned by options parser
    default_path -- default path
    """
    if not check_options(options):
        return None

    incl_hosts = parse_host_paths(' '.join(options.hosts), default_path)
    for file_name in options.file_hosts:
        incl_hosts = implode_host_paths(incl_hosts, parse_host_paths_file(file_name, default_path))

    excl_hosts = parse_host_paths(' '.join(options.exclude_hosts), default_path)
    for file_name in options.file_exclude_hosts:
        excl_hosts = implode_host_paths(excl_hosts, parse_host_paths_file(file_name, default_path))

    for hosts_filter in options.hosts_filter:
        incl, excl = parse_host_paths_filter(hosts_filter, default_path)
        incl_hosts = implode_host_paths(incl_hosts, incl)
        excl_hosts = implode_host_paths(excl_hosts, excl)

    return filter_host_paths(incl_hosts, excl_hosts)

def parse_host_cmd_options(options, default_path, default_cmd):
    """
    Parse optparser options structure to get host paths with cmds.

    For option names see add_hosts_options(). If none of host options is set - return None.
    Return dict with host name as a key and dict path->cmd as a value

    options -- options structure, returned by options parser
    default_path -- default path
    default_cmd -- default command
    """
    if not check_options(options):
        return None

    host_cmds = {}
    def add_paths_cmd(host, paths, cmd):
        """
        Add path with cmd to host_cmds dict.
        """
        if host not in host_cmds:
            host_cmds[host] = {}
        for path in paths:
            if path in host_cmds[host]:
                raise Exception("Duplicate path '%s' for host '%s'." % (path, host))
            host_cmds[host][path] = cmd


    # read exclude host paths from options and files
    excl_hosts = parse_host_paths(' '.join(options.exclude_hosts), default_path)
    for file_name in options.file_exclude_hosts:
        excl_hosts = implode_host_paths(excl_hosts, parse_host_paths_file(file_name, default_path))

    # parse host filters
    for hosts_filter in options.hosts_filter:
        incl, excl = parse_host_paths_filter(hosts_filter, default_path)
        for host, paths in incl.iteritems():
            add_paths_cmd(host, paths, default_cmd)
        excl_hosts = implode_host_paths(excl_hosts, excl)

    # and host paths from options with default cmd
    for host, paths in parse_host_paths(' '.join(options.hosts), default_path).iteritems():
        add_paths_cmd(host, paths, default_cmd)

    # add host paths from file
    for fname in options.file_hosts:
        for host, paths in parse_host_paths_file_cmds(fname,        \
                                                       default_path, \
                                                       default_cmd).iteritems():
            if host not in host_cmds:
                host_cmds[host] = paths
            elif len(set(paths.keys()) & set(host_cmds[host].keys())) == 0:
                for path, cmd in paths.iteritems():
                    host_cmds[host][path] = cmd
            else:
                raise Exception("Duplicate paths for host '%s'." % host)

    for host, paths in excl_hosts.iteritems():
        if host not in host_cmds:
            continue
        incl_cmds = host_cmds[host].copy()
        for incl_cmd in incl_cmds:
            for excl_path in paths:
                if excl_path[-1:] == os.path.sep:
                    excl_path = excl_path[:-1]
                if fnmatch(incl_cmd, excl_path):
                    del host_cmds[host][incl_cmd]
                    break
                if len(excl_path) <= len(incl_cmd):
                    if incl_cmd[:len(excl_path)] == excl_path:
                        del host_cmds[host][incl_cmd]
        if len(host_cmds[host]) == 0:
            del host_cmds[host]

    return host_cmds

def make_output_handlers(options, jobs):
    handlers = []
    if not options.quiet and options.merge_err and options.pbar:
        pbar = ProgressBar(len(jobs))
        handlers.append(handler.ProgressBar(pbar))

    if not options.quiet:
        args = {
            'job_to_str_func': job.job_path,
            'max_jobs_num': options.max_jobs_err,
        }
        if options.merge_err:
            handlers.append(handler.MergeErrors(**args))
            handlers.append(handler.MergeExceptions(**args))
        elif not options.quiet:
            handlers.append(handler.PrintErrors(**args))
            handlers.append(handler.PrintExceptions(**args))

    if options.update_hosts_file != None:
        hnd = handler.DoneJobsToFile(options.update_hosts_file, job.job_path)
        handlers.append(hnd)

    if options.append_failed_hosts:
        hnd = handler.FailedJobsAppendFile(options.append_failed_hosts, job.job_host_path)
        handlers.append(hnd)

    return handlers

def get_default_dir(default_dir_option = None):
    """
    Default user dir on remote hosts.

    Default dir is '/phr/tmp/skynet.$USER/ or value of default_dir variable
    from $HOME/.skynet config.

    If default_dir_option is None -- return default dir.
    If default_dir_option is relative path - return join it to default dir.
    If default_dir_option is an absolute path - return it.

    default_dir_option -- default_dir get from args in cli tool
    """
    if default_dir_option != None and os.path.isabs(default_dir_option):
        return default_dir_option

    default_dir = '/home/%s/' % getuser()
    config_fname = os.path.join(os.environ['HOME'], '.cljobrc')
    if os.path.isfile(config_fname):
        for line in open(config_fname):
            line = line.strip()
            if line == '' or line[0] == '#':
                continue
            name, value = [ x.strip() for x in line.split('=', 1) ]
            if name == 'default_dir':
                default_dir = value
                break
    if default_dir_option != None:
        # then default_dir_option is relative path
        default_dir = os.path.join(default_dir, default_dir_option)

    return default_dir

if __name__ == "__main__":
    import doctest
    doctest.testmod()
