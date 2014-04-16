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

"""Example 2: transfer a function and its dependencies

This example pickles a function and its dependencies (here: a class and the
enclosing module), transfer the pickle to a remote computer, execute it,
pickle the result on the remote side, transfer the result-pickle back to
this computer, and unpickle the results.

"""

from __future__ import absolute_import, division, print_function
import sys
import getpass
import socket
import collections
import os
import logging
LOGGER = logging.getLogger(__name__)

import sPickle
import rpyc_over_ssh

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

    # the directory sPickle/examples is not in sys.path, therefore
    # it is not possible to import modules from this directory.
    # Therefore ask the pickler to serialise those modules.
    pt = sPickle.SPickleTools(serializeableModules=["sPickle/examples"])

    # This function encapsulates the operations to be performed on the
    # remote side.

    remoteLogger = logging.getLogger("remoteLogger")
    additionalResources = (
                           # for production, you want to uncomment the next line, because
                           # you get a much better performance, if the logger is an RPyC
                           # proxy instead of the log-file

                           #remoteLogger,
                          )

    @pt.remotemethod(connection, create_only_once=True, additionalResourceObjects=additionalResources)
    def remote_function(param1, param2):
        remoteLogger.info("Running on Host %r, PID %d", socket.gethostname(), os.getpid())
        remoteLogger.info("Starting function with parameters: %r, %r", param1, param2)
        algorithm = ComputeTheAnswer(param1, param2)
        remoteLogger.info("Computing ...")
        algorithm.compute()
        remoteLogger.info("Computing is done.")
        return algorithm.getResult()

    # Lets perform a few computations
    r = remote_function(22, 20)
    print("Result: ", r)
    r = remote_function("4", "1")
    print("Result: ", r)
    r = remote_function("42", None)
    print("Result: ", r)


def main(argv):
    """Run this example

    Usage: main [hostname [commands to run the rpycserver on hostname]]
    """
    host = argv.pop(0) if argv else socket.getfqdn()
    argv.extend(rpyc_over_ssh.START_RPYC_CLASSIC_SERVER_ARGV[len(argv):])
    if '@' in host:
        username, host = host.split('@', 1)
    else:
        username = getpass.getuser()
    print("Host is: %r  username is: %r" % (host, username))
    print("Command line is: %r" % argv)
    password = None  # I'm using an ssh agent

    connection = rpyc_over_ssh.newRPyCConnectionOverSsh(argv, host, username, password)
    connection.root.getmodule("sys").stdout = sys.stdout  # redirect remote stdout to our local stdout
    try:
        doit(None)         # local execution
        doit(connection)   # remote execution
    finally:
        connection.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        # filename="example2.log"
                        )
    logging.getLogger().addHandler(logging.FileHandler("example2.log", mode='w'))
    main(sys.argv[1:])
