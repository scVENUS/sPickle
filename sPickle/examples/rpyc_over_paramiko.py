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

from __future__ import absolute_import
import sys
import imp
import os.path

import sPickle
import rpyc
import paramiko
import pickletools

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO




def main(args):
    print "Hello, World!"
    
    l = []
    pickler = sPickle.Pickler(l)
    pickler.serializeableModules.append("/rpyc/")

    t = imp.find_module("rpyc_classic", [ os.path.join(p, "scripts") for p in rpyc.__path__])
    try:
        m = imp.load_module("rpyc.__rpyc_classic_server", *t)
    finally:
        t[0].close()

    args = ["--mode", 'stdio', "--quiet"]

    imp.acquire_lock()
    try:
        argv_saved = sys.argv
        try:
            sys.argv = [sys.argv[0]] + list(args)
            options = m.get_options()
        finally:
            sys.argv = argv_saved
    finally:
        imp.release_lock()
    handler = getattr(m, options.handler)
    
    # pickler.dump((handler, options))
    pickler.dump(rpyc)
    p = pickletools.optimize("".join(l))
    pickletools.dis(p, None, None, 4)

if __name__ == '__main__':
    main(sys.argv[1:])
