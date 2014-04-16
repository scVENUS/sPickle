#
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

"""
=========
 sPickle
=========

The module sPickle is an enhanced version of the standard module :mod:`pickle`.
It provides an improved :class:`Pickler` class and a
utility class :class:`SPickleTools`.

The sPickle package tries to push the limits for pickling. The implementation
tries to create correct pickles, but it does not try to be efficient or portable or
nice to read or ... Consider it a proof of concept, a demonstration, that shows
what could be done.

.. warning::
   Although the author is using the sPickle package in production, it is
   more or less untested outside the specific environment it was written for.

.. note::
   The sPickle package is currently requires Python 2.7.

.. autoclass:: Pickler
   :members: analysePicklerStack

.. autoclass:: SPickleTools
   :members:

.. autoclass:: FailSavePickler
   :members: get_replacement

.. autoexception:: StacklessTaskletReturnValueException

"""


from __future__ import absolute_import, division, print_function

from pickle import *
del Pickler
from ._sPickle import (Pickler,  # @Reimport
                       SPickleTools,
                       MODULE_TO_BE_PICKLED_FLAG_NAME,
                       StacklessTaskletReturnValueException,
                       RESOURCE_TYPES,
                       FailSavePickler)
from pickle import __all__

__all__ = __all__[:]
__all__.extend(('SPickleTools', 'StacklessTaskletReturnValueException'))
