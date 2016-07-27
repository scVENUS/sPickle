About sPickle
=============

sPickle is an advanced (and experimental) Pickler for Python. The code
was developed as part of a commercial project and released as free
software under the Apache 2.0 License by science + computing ag. The
software is copyrighted by science + computing ag.

Why did we decide to make sPickle free software? We utilise Python and
other open source products a lot. Therefore we think it is just fair
to release enhancements back to the public.


Requirements
------------

* Python 2.7
* RPyC is used in the unit tests; the test is skipped, if
  import rpyc fails.

(Sorry, Python 3 support is still in its beginnings.
See https://github.com/akruis/sPickle/tree/py3port. I'll happily accept
patches.)


Installation
------------

Get easy_install and do "easy_install sPickle".
Or get the latest source code from github.
git clone git://github.com/akruis/sPickle.git

Using sPickle
-------------

The sPickle module provides a class sPickle.Pickler. This class is very
similar to the conventional pickle.Pickler class and can be used as a
drop in replacement. Instead of:

  from pickle import Pickler

  file = open(filename, "wb")
  pickler = Pickler(file, -1)
  pickler.dump(object_to_be_serialised)
  file.close()

you write:

  from sPickle import Pickler

  file = open(filename, "wb")
  pickler = Pickler(file, -1)
  pickler.dump(object_to_be_serialised)
  file.close()

You see, just 2 characters difference. Of course, there are more
differences under the hood.

* sPickle supports serialisation of more types than the conventional Pickler.
  In fact, most types can be pickled now, if serialisation makes any sense
  for the particular type at all. But be warned: unpickling of some types
  requires the same Python version.

* A sPickle Pickler has a list-typed attribute "serializeableModules". You can
  use this attribute to determine which modules are to be pickled. For
  details, read the comments in the source code and look at the examples.
  
* A sPickle Pickler has some (experimental) features to manipulate the content
  of the created pickle and to handle/replace objects, which can not be pickled. 

Support
=======
There is currently no support available, but you can drop me a mail.
a [dot] kruis [at] atos [dot] net
or use the facilities provided by https://github.com/akruis/sPickle

Plan
====
No further plans currently


### Changes ###

Version 0.1.11
--------------

2016-07-27
- Improved the pickling of global objects from the Python standard library
  with missing or incorrect or misleading  values of "__module__" and/or
  "__name__". This change adds a new class ObjectDispatchBuilder and new
  attributes "object_dispatch" and "object_dispatch_builder" of the pickler.

Version 0.1.10
--------------

2016-02-26
- Removed 'StacklessTaskletReturnValueException' from sPickle.__all__
- Documented constants and exceptions and added them to sPickle.__all__
- Previously a method of the super class was pickled by value, if the
  subclass overloaded the method. Now the pickler performs a correct
  mro-search to find the implementing class of a method.

Version 0.1.9
-------------

2016-02-19
- Fixed pickling of private methods.
- PEP 8 fixes.

Version 0.1.8
-------------

2015-03-26
- Added support for pickling empty cell objects.
- PEP 8 fixes.

Version 0.1.7
-------------

2014-07-12
- Added the optional argument "logger" to class sPickle.Pickler. Can be used to
  customize or suppress logging.

Version 0.1.6
-------------

2014-05-09
- Added limited support for pickling objects returned by io.open().

Version 0.1.5
-------------

2014-04-16
- Cleanups, reformatted source (PEP-8).
- More robust test suite.
- Python 3 compatible syntax.
- I switched the SSH implementation used by the examples back to paramiko.
- No functional changes in package sPickle.

Version 0.1.4
-------------
2014-02-07
New class FailSavePickler. It is a subclass of Pickler, that can replace
otherwise unpickleable objects by surrogates.

Added support for pickling bound and unbound instance methods.

sPickle no longer requires "ssh" and "rpyc". Both modules are
are required to run the examples.

Version 0.1.3
-------------
2013-11-05
Improved pickling of abstract base classes. Every abstract base class
contains two WeakSet attributes _abs_cache and _abc_negative_cache. These
sets cache the result of subclass tests. It is advisable not to pickle the
content of those caches, because it could contain unpickleable objects.
This commit changes sPickle to replace _abs_cache and _abc_negative_cache
by new empty WeakSet instances.

Version 0.1.2
-------------
2013-05-22
Fixed two problems concerning module renaming.
Adapted to Python 2.7.4 and 2.7.5.

Note: Python 2.7.5 broke unpickling of named tuples pickled by Python 2.7.3 or 2.7.4.
This is Python bug http://bugs.python.org/issue18015. You can place the file
http://bugs.python.org/file30338/fix_python_275_issue18015.pth into your
site-packages directory to monkey patch this bug.

Version 0.1.1
-------------
2012-11-06
Support pickling of classes with a custom meta class.
Improved sPickle.Pickler. It no longer modifies sys.modules or modules.

Version 0.1.0
-------------
2012-10-28
Support for plain Python

sPickle no longer requires Stackless Python for most of its work. Of course,
if you want to pickle tasklets you need Stackless, but for functions, classes
and most other objects, the normal CPython 2.7 is good enough.

Version 0.0.9
-------------
2012-10-28
Support pickling of cStringIO objects.
Fixed pickling of recursive collections.OrderedDict objects
Support pickling of <type 'cell'> type objects.
Support pickling of weakref.ReferenceType objects without callback function.
Fixed pickling of modules with a modified __name__ attribute.
Support pickling of super objects.
Fixed classes with unusual __dict__ attribute.

Replaced REDUCE __import__ ... with GLOBAL. Now GLOBAL is used for all imports.
This change makes it possible to get all imported modules
using SPickleTools.getImportList().

Enhancements:
- The new utility method SPickleTools.reducer() simplifies
  the implementation of complex __reduce__ methods.
- I Added an option to rename the module name of pickled objects.
- The methods dumps, dumps_with_external_ids and
  remotemethod of class SPickleTools got new parameters.
- The class Pickler got a new method analysePicklerStack. It can
  be used to extract information about the Pickler state from
  the stack or from a traceback.

Version 0.0.8
-------------
2012-07-17
Important bug fix: previous versions failed to pickle some classes.
Added support for member descriptors, getset descriptors and some
dictproxy objects.

Added a debugging aid for "id(obj) not in self.memo" assertions.
Set the environment variable SPICKLE_TRACE_MEMO_ENTRIES to a list of
space separated memo keys (integer numbers). sPickle will log information
about the object hierarchy, when it adds a traced memo entry.

Version 0.0.7
-------------
Minor fixes. Changed the logging to delay log messages while holding the
import lock. Otherwise you might get a dead-lock.
Upgrade the Python extensions used by the examples and switched from
Paramiko to ssh.

Version 0.0.6
-------------
Improved the documentation and improved the method SPickleTools.remotemethod.

Version 0.0.5
-------------
Finally I found a few hours to add a nice documentation using Sphinx.
Besides the documentation, there are a few cleanups.

Version 0.0.4
-------------
Minor improvements, but unfortunately the documentation is still
incomplete. I release this version, because I'm going to present this
version in my talk at EuroPython 2011.

Version 0.0.3
-------------
Renamed example1 to example2 and added a new example1. Example1 now demonstrates
checkpointing of programs, example2 demonstrates RPyC.
Fixed a few problems.
Use sPickle in flowGuide instead of the original flowGuide pickle code.


Version 0.0.2
-------------
Reorganised the directory layout, added this README.txt and fixed a few
problems. Currently we still use a private source code repository as
science + computing, but I plan to switch to github soon.


Version 0.0.1
-------------
Initial version, released by science + computing ag. This version is more or
less a copy of two modules from the flowGuide2 source code. The code works
for the specific requirements of flowGuide2, but has not been tested outside
of the flowGuide2 environment.
