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

"""Example 1: transfer a function and its dependencies

This example pickles a function and its dependencies (here: a class and the 
enclosing module), transfer the pickle to a remote computer, execute it, 
pickle the result on the remote side, transfer the result-pickle back to
this computer, and unpickle the results.

"""   

from __future__ import absolute_import
import sys
import getpass
import socket
import os.path
import collections


import sPickle
import pickletools

import rpyc_over_paramiko

import logging
LOGGER = logging.getLogger(__name__)


Result = collections.namedtuple("Result", "value error exception")

class ComputeTheAnswer(object):
    def __init__(self, param1, param2):
        self.param1 = param1
        self.param2 = param2
        self.result = None
        self.error = None
        self.exception = None
        
    def compute(self):
        try:
            self.result = self.param1 + self.param2
            if int(self.result) != 42:
                self.error = "Result is inconsistent with previous calculations!"
        except Exception, e:
            self.error = "Hey, you asked the wrong question. Try again with different parameters!"
            self.exception = e
            
    def getResult(self):
        return Result(self.result, self.error, self.exception)
        

def doit(connection):
    root = connection.root
    rsys = root.getmodule("sys")
    rsys.stdout = sys.stdout
    
    # We need the sPickle module on the remote side too. 
    # To simplify the development, we add the current sPickle directory 
    # This works fine, if the remote host is "localhost" and does not 
    # hurt otherwise.
    rsys.path.append(os.path.dirname(os.path.dirname(sPickle.__file__)))
    connection.execute("import sys; print 'Remote sys.path: %r' % sys.path")

    # the directory sPickle/examples is not in sys.path, therefore
    # it is not possible to import modules from this directory. 
    # Therefore ask the pickler to serialise those modules.
    pt = sPickle.SPickleTools(serializeableModules=["sPickle/examples"])
    
    # This function encapsulates the operations to be performed on the 
    # remote side.
    def function(param1, param2):
        algorithm = ComputeTheAnswer(param1, param2)
        algorithm.compute()
        return algorithm.getResult()

    # Pickle function, transfer it to the remote side, unpickle 
    # it on the remote side and return a proxy to the remote function.
    # Transfer the return value as Pickle
    remote_function = pt.remotemethod(connection, function)
    
    # Lets perform a few computations
    r = remote_function(22, 20)
    print "Result: ", r
    r = remote_function("4","1")
    print "Result: ", r
    r = remote_function("42",None)
    print "Result: ", r


def main(argv):
    """Run this example
    
    Usage: main [hostname [commands to run the rpycserver on hostname]]

    """
    host = argv.pop(0) if argv else socket.getfqdn()
    if not argv:
        argv = rpyc_over_paramiko.START_RPYC_CLASSIC_SERVER_ARGV
    print "Host is: %r" % host
    print "Command line is: %r" % argv
    username = getpass.getuser()
    password = None  # I'm using an ssh agent
    
    connection = rpyc_over_paramiko.newRPyCConnectionOverParamiko(argv, host, username, password)
    try:
        doit(connection)
    finally:
        connection.close()
    

if __name__ == '__main__':
    logging.basicConfig(level=logging.WARN)
    main(sys.argv[1:])
