#!/usr/bin/env python

from distutils.core import setup
from glob import glob

setup(
    name='cljob',
    version='0.1-dev',
    packages=['cljob'],
    scripts=glob('scripts/trsh*') + ['scripts/tcheck_fails']
)
