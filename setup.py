#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2011 by science+computing ag
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


from setuptools import setup
import sys
if sys.version_info < (2, 7):
    print 'ERROR: sPickle requires at least Stackless Python 2.7 to run.'
    sys.exit(1)
try:
    import stackless
    del stackless
except ImportError:
    print 'ERROR: sPickle requires Stackless Python to run.'
    sys.exit(1)
    
from conf import release

setup(
    name='sPickle',
    version=release,
    description='Extended Pickler for Stackless Python',
    author='Anselm Kruis',
    author_email='a.kruis@science-computing.de',
    url='http://pypi.python.org/pypi/sPickle',
    packages=['sPickle', 'sPickle.test'],

    # don't forget to add these files to MANIFEST.in too
    package_data={'sPickle': ['examples/*.py']},

    long_description=
"""
sPickle is an extended version of the pickle module for Stackless Python
------------------------------------------------------------------------

It supports pickling of modules and many resources the standard pickler can't
cope with. 

This version requires Python 2.7 or later. Unfortunately, there is currently no
Python 3 version.

Git repository: git://github.com/akruis/sPickle.git
""",
    classifiers=[
          "License :: OSI Approved :: Apache Software License",
          "Programming Language :: Python",
          "Programming Language :: Python :: 2.7",
          "Environment :: Other Environment",
          "Operating System :: OS Independent",
          "Development Status :: 3 - Alpha", # hasn't been tested outside of flowGuide2
          "Intended Audience :: Developers",
          "Topic :: Software Development :: Libraries :: Python Modules",
      ],
      keywords='pickling sPickle pickle stackless',
      license='Apache Software License',
      install_requires=[
        'RPyC>=3.2.2',
        'ssh>=1.7.14'
      ],
      platforms="any",
      test_suite="sPickle"
    
    )
