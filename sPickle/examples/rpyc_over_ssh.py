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
This module contains a few functions and classes, that help
you to build RPyC connections using the paramiko ssh client.

Unfortunately this code is not completely documented. See the
documentation of paramiko and RPyC for details.
"""

from __future__ import absolute_import, division, print_function

import sys
import os.path
import threading
import getpass
import socket
import collections

import rpyc
SERVER_FILE = os.environ.get("RPYC_SERVER_FILE", os.path.join(os.path.dirname(rpyc.__file__), "scripts", "rpyc_classic.py"))
import paramiko
import logging
LOGGER = logging.getLogger(__name__)

__all__ = ("newRPyCConnectionOverSsh", "argv2command", "START_RPYC_CLASSIC_SERVER_ARGV")

START_RPYC_CLASSIC_SERVER_ARGV = [os.path.abspath(sys.executable), SERVER_FILE, '-m', 'stdio', '-q']


class SshRpycStream(rpyc.SocketStream):
    def __init__(self, sock):
        if not isinstance(sock, RpycParamicoChannel):
            # That's an evil hack. We modify the class of sock
            sock.__class__ = RpycParamicoChannel
            sock.__dict__['_stdoutPipe'] = None
        super(SshRpycStream, self).__init__(sock)


class RpycParamicoChannel(paramiko.Channel):
    def __init__(self, *args, **kw):
        self._stdoutPipe = None
        super(RpycParamicoChannel, self).__init__(*args, **kw)

    def filenoStdout(self):
        """
        Returns an OS-level file descriptor which can be used for polling, but
        but I{not} for reading or writing.  This is primaily to allow python's
        C{select} module to work.

        The first time C{fileno} is called on a channel, a pipe is created to
        simulate real OS-level file descriptor (FD) behavior.  Because of this,
        two OS-level FDs are created, which will use up FDs faster than normal.
        (You won't notice this effect unless you have hundreds of channels
        open at the same time.)

        @return: an OS-level file descriptor
        @rtype: int

        @warning: This method causes channel reads to be slightly less
           efficient.
        """
        self.lock.acquire()
        try:
            if self._stdoutPipe is not None:
                return self._stdoutPipe.fileno()
            # create the pipe and feed in any existing data
            self._stdoutPipe = paramiko.pipe.make_pipe()
            self.in_buffer.set_event(self._stdoutPipe)
            return self._stdoutPipe.fileno()
        finally:
            self.lock.release()

    def fileno(self):
        return self.filenoStdout()


def newRPyCConnectionOverSsh(command, host, username, password):
    """get a RPyC connection to a given host via ssh

    Open a ssh connection to the given host and
    run command. Command must start a RPyC server, that
    is connected to its stdio. Usually command will look
    similar to::

        stackless_python2.7 .../rpyc/scripts/rpyc_classic.py -m stdio -q

    (You Run the command on the given host via host and return a RpycStream object

    The RpycStream can be used to communicate with the command.
    """
    if not isinstance(command, basestring) and isinstance(command, collections.Sequence):
        # convert the argv string vector to a single string
        command = argv2command(command)

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=username, password=password)  # there are more authentication options
    transport = client.get_transport()
    transport.setName("paramiko.Transport for rpyc stream")
    channel = transport.open_session()
    channel.exec_command(command)

    # append stderr to sys.stderr or self.stderr
    # the thread must hold a copy of the paramiko client object in order to keep the connection alive
    closeEvent = threading.Event()
    closeEvent.clear()
    stderrThread = threading.Thread(target=__copy2stderr,
                      args=(None, channel, client, closeEvent),
                      name="Stderr copy for %s" % (channel.get_name(),))
    stderrThread.daemon = False  # keep the interpreter alive until this thread is done
    stderrThread.start()
    prs = SshRpycStream(channel)
    # keep the paramiko-client alive. This is very important, otherwise the connection will shutdown
    prs._sshClient = client
    c = rpyc.connect_stream(prs, rpyc.SlaveService)
    return c


def __copy2stderr(out, chanel, sshclient, closeEvent):
    """Copy the stderr sub chanel of the paramiko connection to out or sys.stderr"""
    length = 10000
    try:
        while not closeEvent.is_set():
            buf = chanel.recv_stderr(length)
            if not buf:
                break
            if out is None:
                sys.stderr.write(buf)
            else:
                if isinstance(out, int):
                    os.write(out, buf)
                else:
                    out.write(buf)
    except Exception:
        LOGGER.debug("Ignoring exception while closing", exc_info=True)


def argv2command(argv):
    """Convert the argv vector into a ssh command string.

    Unfortunately the ssh protocol uses a single string instead
    of an array of strings like posix exec.
    """
    v = []
    SPECIAL1 = (' ', '\t', '\\', '&', '|', ';', '<', '>',
               '(', ')', '!', '$', "'", '"', '`', '(', ')', '{', '}',)
    for arg in argv:
        encoded = []
        for c in arg:
            if c in SPECIAL1:
                encoded.append('\\')
            encoded.append(c)
        encoded = b"".join(encoded)
        v.append(encoded)
    command = b" ".join(v)
    return command


def _hello_world(argv):
    """A RPyC hello world example"""
    host = argv.pop(0) if argv else socket.getfqdn()
    if not argv:
        argv = START_RPYC_CLASSIC_SERVER_ARGV

    print("Host is: %r" % host)
    print("Command line is: %r" % argv)

    username = getpass.getuser()
    password = None  # I'm using a ssh agent

    connection = newRPyCConnectionOverSsh(argv, host, username, password)
    try:
        root = connection.root         # get the remote root object
        rsys = root.getmodule("sys")   # get the remote sys module
        rsys.stdout = sys.stdout       # redirect the remote stdout to our stdout
        connection.execute(b"from __future__ import print_function; print('Hallo, World!')")  # print on the remote side
    finally:
        connection.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    _hello_world(sys.argv[1:])
