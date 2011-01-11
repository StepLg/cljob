#!/usr/bin/env python

from distutils.core import setup
from glob import glob
from cljob import __author__,  \
                  __email__,   \
                  __license__, \
                  __url__,     \
                  __version__

setup(
    name='cljob',
    version=str(__version__),
    author=__author__,
    author_email=__email__,
    license=__license__,
    url=__url__,
    packages=['cljob'],
    scripts=glob('./trsh*') + ['./tcheck_failures'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Python Software Foundation License",
        "Natural Language :: English",
        "Operating System :: POSIX",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Topic :: System :: Clustering",
        "Topic :: System :: Systems Administration",
        "Topic :: Utilities"
    ],

    description="Cluster Administrators' ssh Wrapper (run shell cmds, upload, download)",
    long_description='''

    cljob is yet another wrapper around ssh/rsh for running jobs on multiple
    targets. At the moment you can run shell commands, upload and download
    files from target machenes. It can support thousands of targets and runs
    a configurable number of jobs concurrently. It separately gathers results,
    output and error messages, displaying summary/status information it comes
    in (asynchronously) and more detailed data after all jobs have completed.

    cljob has a very simple and clear internal structure, so you can easyly add
    your own functionality (for example, up/download files not via rsync).

    Original idea from classh (http://pypi.python.org/pypi/classh).
    '''
)
