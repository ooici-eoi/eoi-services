#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

import os
import sys

# Add /usr/local/include to the path for macs, fixes easy_install for several packages (like gevent and pyyaml)
if sys.platform == 'darwin':
    os.environ['C_INCLUDE_PATH'] = '/usr/local/include'

version = '0.0.1'

setup(  name = 'eoi-services',
        version = version,
        description = 'OOI ION EOI Handler',
        long_description='''
        ''',
        url = '',
        download_url = 'http://ooici.net/releases',
        license = 'Apache 2.0',
        author = 'Christopher Mueller',
        author_email = 'cmueller@asascience.com',
        keywords = ['ooici', 'ion', 'eoi'],
        packages = find_packages(),
        test_suite = 'pyon',
        dependency_links = [
            'http://ooici.net/releases'
        ],
        install_requires = [
        	'pyon',
            'Pydap>=3.0.1',
            'arrayterator>=1.0.1',
            'netCDF4>=0.9.8',
            'cdat_lite>=6.0rc2',
        ],
     )
