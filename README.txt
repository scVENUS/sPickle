About sPickle
=============

sPickle is an advanced (and experimental) Pickler for Stackless Python. 
The code was developed as part of a commercial project and released as free
software under the Apache 2.0 License by science + computing ag. The 
software is copyrighted by science + computing ag.

Why did we decide to make sPickle free software? We utilise Python and 
other open source products a lot. Therefore we think it is just fair
to release enhancements back to the public. 


Requirements
------------

* Stackless Python 2.7
* RPyC is used in the unit tests

(Sorry, no Python 3 support and no support for conventional Python. I'll 
happily accept patches.)  
 

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
  for the particular type at all. 
 
* A sPickle Pickler has a list-typed attribute "serializeableModules". You can
  use this attribute to determine which modules are to be pickled. For 
  details, read the comments in the source code and look at the examples.  

Support
=======
There is currently no support available, but you can drop me a mail.
a [dot] kruis [at] science-computing [dot] de  

Plan
====
No further plans currently

Changes
=======

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
