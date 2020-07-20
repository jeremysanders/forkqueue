#!/usr/bin/env python3

import os
from distutils.core import setup
import setuptools

urls = {
    'Source': 'https://github.com/jeremysanders/poolqueue',
    'Tracker': 'https://github.com/jeremysanders/poolqueue/issues',
}

classifiers=[
    # How mature is this project? Common values are
    #   3 - Alpha
    #   4 - Beta
    #   5 - Production/Stable
    'Development Status :: 4 - Beta',

    # Indicate who your project is intended for
    'Intended Audience :: Developers',

    # Pick your license as you wish (should match "license" above)
     'License :: OSI Approved :: MIT License',

    # Specify the Python versions you support here. In particular, ensure
    # that you indicate whether you support Python 2, Python 3 or both.
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
]

if not hasattr(os, 'fork'):
    raise RuntimeError('This module only works on operating systems which provide a fork() function')

setup(
    name='poolqueue',
    version='1.0',
    description='Process tasks from a queue using forked processes',
    author='Jeremy Sanders',
    author_email='jeremy@jeremysanders.net',
    url='https://github.com/jeremysanders/poolqueue',
    project_urls=urls,
    license='MIT',
    python_requires='~=3.3',
    classifiers=classifiers,
    py_modules=['poolqueue'],
)
