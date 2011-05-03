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

"""Example 1: checkpoint a running program.

This example demonstrate how to checkpoint and restart 
a program. 
"""   

from __future__ import absolute_import
import sys
import os.path
import time


import checkpointing

import logging
LOGGER = logging.getLogger(__name__)

def long_running_function_with_checkpointing(checkpointSupport, *args, **keywords):
    print "At program start"
    print "  arguments: ", args
    print "  keywords: ", keywords

    print "Computing ...",
    iterator = iter(xrange(1,21))
    for i in iterator:
        time.sleep(0.1)
        print " ", i,
        if i == 10:
            break
    print ""
    
    #
    print "Checkpointing ..."
    isCmdResult, result = checkpointSupport.forkAndCheckpoint()
    if isCmdResult:
        # result is the pickle
        checkpointFile = keywords["checkpointFile"]
        f = open(checkpointFile, "wb")
        f.write(result)
        f.close()
        print "Saved state as %r" % (checkpointFile,)
        return 0  # posix exit code
    
    # after restart
    newArgs, newKeywords = result
    
    print "Restarted program resumes execution"
    print "  original arguments: ", args
    print "  original keywords: ", keywords
    print "  new arguments: ", newArgs
    print "  new keywords: ", newKeywords

    print "Computing ...",
    for i in iterator:
        time.sleep(0.1)
        print " ", i,
    print ""
    
    print "Done."
    
    return 0 # posix exit code




def main(argv):
    """Run this example
    
    Usage: main [hostname [commands to run the rpycserver on hostname]]

    """
    checkpointFile = "example1.pickle"
    mode = argv.pop(0)
    if mode == "start":
                
        from sPickle import SPickleTools
        # always serialize __main__, because the main used during a resume 
        # operation is most likely a different module loaded from a different file
        pt = SPickleTools(serializeableModules=['__main__'])
        return checkpointing.runCheckpointable(pt.dumps, 
                                               long_running_function_with_checkpointing, 
                                               checkpointFile = checkpointFile, 
                                               *argv)

    elif mode == "resume":
        
        # Resume the execution of the checkpoint
        # Note: this mode 2 does not define any functional logic.

        return checkpointing.resumeCheckpoint(open(checkpointFile, "rb").read(), *argv)

    else:
        print >> sys.stderr, 'Usage: %s: [ "start" | "resume" ] ...' % (os.path.basename(__file__),) 
        return 1
        
if __name__ == '__main__':
    logging.basicConfig(level=logging.WARN)
    main(sys.argv[1:])
