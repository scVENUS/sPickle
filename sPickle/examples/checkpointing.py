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
Rudimentary checkpointing support.

This module provides two functions `runCheckpointable`
and `resumeCheckpoint`, that can be used to checkpoint
and resume a (single threaded) python program.
"""   

from __future__ import absolute_import
import sys
import types
import sPickle
import stackless
import inspect

class _CheckpointSupport(object):
    """Handler class"""
    CMD_CHECKPOINT = "checkpoint"
    def __init__(self):
        self._restoreTraceFunc = None
        
    def _switchTraceFunc(self, trace):
        oldTraceFunc = sys.gettrace()
        func = sys.settrace
        # in case of the PyDev debugger, we have to mangle the function a little bit.
        # This is kind of black magic, but unfortunately, I don't know any better way 
        # to do it
        if isinstance(func, types.MethodType):
            func = getattr(sys.settrace, "__func__")
        func = getattr(func, "func_globals", {}).get("SetTrace", func)
        self._restoreTraceFunc = lambda : func(oldTraceFunc)
        if trace:
            func(trace)
            trace = None

    def _taskletRun(self, trace, callable, args, keywords):
        self._switchTraceFunc(trace)
        try:
            raise sPickle.StacklessTaskletReturnValueException(callable(self, *args, **keywords))
        finally:
            self._restoreTraceFunc()
            self._restoreTraceFunc = None
            
    def _loop(self, tasklet, pickler):
        try:
            while True:
                tasklet.run()
                cmd = tasklet.tempval
                if cmd == _CheckpointSupport.CMD_CHECKPOINT:
                    result = pickler((self, tasklet))
                # elif cmd == "another command":
                #     ...
                #     result = ...
                else:
                    raise ValueError("Unknown command %r" % (cmd,))
                tasklet.tempval = (True, sys.gettrace(), result)
        except sPickle.StacklessTaskletReturnValueException, e:
            return e.value
        
        
    def forkAndCheckpoint(self, cmd=CMD_CHECKPOINT):
        """Checkpoint the current thread.
        
        This method creates a checkpoint of the current thread. 
        The method returns two times with different return values.
        If you invoke this method, it returns the tuple `True`, 
        `checkpoint`. `checkpoint` is the pickled state of 
        the current thread as a byte string. 
        
        If you resume the program using the 
        `resumeCheckpoint` function, it returns the tuple
        `False`, `args_keywords`. args_keywords is a tuple containing the 
        *args and **keywords parameters given to the resumeCheckpoint function.
        """
        self._restoreTraceFunc()
        self._restoreTraceFunc = None
        sys.exc_clear()

        isCmdResult, trace, result = stackless.schedule(cmd)
        self._switchTraceFunc(trace)

        return (isCmdResult, result)

        
def runCheckpointable(pickler, callable, *args, **keywords):
    """Run callable as a checkpointable tasklet.
    
    `pickler` must be a function or method, that takes an object 
    and returns a pickle of the object.
    `callable` can be any callable, with at least one positional 
    parameter. This first parameter receives a _CheckpointSupport object,
    that provides a method `forkAndCheckpoint`. Additional parameters of 
    `callable` can be set using `args` and `keywords`.
    """ 
    
    checkpointSupport = _CheckpointSupport()
    tasklet = stackless.tasklet(checkpointSupport._taskletRun)
    tasklet.setup(sys.gettrace(), callable, args, keywords)
    tasklet.tempval = None
    return checkpointSupport._loop(tasklet, pickler)

def resumeCheckpoint(checkpoint, *args, **keywords):
    """Resume the execution of a checkpoint.
    
    The `checkpoint` parameter must be the string you 
    got from the method `forkAndCheckpoint`. 
    """
    pt = sPickle.SPickleTools()
    # pt.dis(checkpoint, sys.stdout)
    restored = pt.loads(checkpoint, useCPickle=False)
    checkpointSupport, tasklet = restored
    tasklet.tempval = (False, sys.gettrace(), (args, keywords))
    return checkpointSupport._loop(tasklet, pt.dumps)

if __name__ == "__main__":
    import os
    import logging
    logging.basicConfig(level=logging.WARN)
    import argparse
    parser = argparse.ArgumentParser(description="Resume a check-pointed program.")
    g=parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--demo", action="store_true", help='do not resume a program, but run the demo')
    g.add_argument("--stdin", action="store_true", help='read the checkpoint from stdin')
    g.add_argument("checkpoint_file", nargs='?', help='name of the checkpoint file')
    parser.add_argument('--dis', action="store_true", help='dump the disassembled checkpoint to stdout')
    parser.add_argument('args', help='arguments given to the resumed program', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    
    if args.demo:
        def sample(checkpointSupport):
            internalState = 1
            print "Internal state is ", internalState
            while internalState < 3:
                flag, result = checkpointSupport.forkAndCheckpoint()
                if flag:
                    print "Created checkpoint"
                    return result
                print "Resuming ..."
                internalState += 1
                print "Internal state is ", internalState
            print "Done"
            return 0
        
        # the directory sPickle/examples is not in sys.path, therefore
        # it is not possible to import modules from this directory. 
        # Therefore ask the pickler to serialise those modules.
        pt = sPickle.SPickleTools(serializeableModules=["sPickle/examples"])
        
        checkpoint = runCheckpointable(pt.dumps, sample, *args.args)
        while isinstance(checkpoint, str):
            checkpoint = resumeCheckpoint(checkpoint)
        sys.exit(checkpoint)
    
    # regular code
    if args.stdin:
        try:
            import msvcrt
        except ImportError:
            pass
        else:
            msvcrt.setmode(sys.stdin.fileno(), os.O_BINARY)
        checkpoint = sys.stdin.read()
    else:
        f = open(args.checkpoint_file, "rb")
        checkpoint = f.read()
        f.close()
    if args.dis:
        sPickle.SPickleTools().dis(checkpoint)
        sys.exit(0)
    sys.exit(resumeCheckpoint(checkpoint, *args.args))
