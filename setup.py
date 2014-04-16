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
    print 'ERROR: sPickle requires at least Python 2.7 to run.'
    sys.exit(1)

for line in open('conf.py'):
    if line.startswith('release = '):
        exec(line)
        break

setup(
    name='sPickle',
    version=release,  # @UndefinedVariable
    description='Extended Pickler with special support for Stackless Python',
    author='Anselm Kruis',
    author_email='a.kruis@science-computing.de',
    url='http://pypi.python.org/pypi/sPickle',
    packages=['sPickle', 'sPickle.test'],

    # don't forget to add these files to MANIFEST.in too
    package_data={'sPickle': ['examples/*.py']},

    long_description="""
sPickle is an extended version of the pickle module
---------------------------------------------------

It supports pickling of modules and many resources the standard pickler can't
cope with.

This version requires Python 2.7. Unfortunately, there is currently no
Python 3 version.

Git repository: git://github.com/akruis/sPickle.git
""",
    classifiers=[
          "License :: OSI Approved :: Apache Software License",
          "Programming Language :: Python",
          "Programming Language :: Python :: 2.7",
          "Environment :: Other Environment",
          "Operating System :: OS Independent",
          "Development Status :: 4 - Beta",  # hasn't been tested outside of flowGuide2
          "Intended Audience :: Developers",
          "Topic :: Software Development :: Libraries :: Python Modules",
      ],
      keywords='pickling sPickle pickle stackless',
      license='Apache Software License',
      install_requires=[
        # for examples and tests ...
        # 'RPyC>=3.2.2', 'ssh>=1.7.14'
      ],
      platforms="any",
      test_suite="sPickle",
    )
