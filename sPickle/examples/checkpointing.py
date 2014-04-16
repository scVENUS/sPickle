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

from __future__ import absolute_import, division, print_function

import sys
import sPickle
import stackless


class _CheckpointSupport(object):
    """Handler class"""
    CMD_CHECKPOINT = "checkpoint"

    def __init__(self):
        self._restoreTraceFunc = None

    def _taskletRun(self, callable_, args, keywords):
        raise sPickle.StacklessTaskletReturnValueException(callable_(self, *args, **keywords))

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
                tasklet.tempval = (True, result)
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
        sys.exc_clear()

        isCmdResult, result = stackless.schedule(cmd)

        return (isCmdResult, result)


def runCheckpointable(pickler, callable_, *args, **keywords):
    """Run callable_ as a checkpointable tasklet.

    `pickler` must be a function or method, that takes an object
    and returns a pickle of the object.
    `callable_` can be any callable, with at least one positional
    parameter. This first parameter receives a _CheckpointSupport object,
    that provides a method `forkAndCheckpoint`. Additional parameters of
    `callable_` can be set using `args` and `keywords`.
    """
    checkpointSupport = _CheckpointSupport()
    tasklet = stackless.tasklet(checkpointSupport._taskletRun)
    tasklet.setup(callable_, args, keywords)
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
    tasklet.tempval = (False, (args, keywords))
    return checkpointSupport._loop(tasklet, pt.dumps)

if __name__ == "__main__":
    import os
    import logging
    logging.basicConfig(level=logging.WARN)
    import argparse
    parser = argparse.ArgumentParser(description="Resume a check-pointed program.")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--demo", action="store_true", help='do not resume a program, but run the demo')
    g.add_argument("--stdin", action="store_true", help='read the checkpoint from stdin')
    g.add_argument("checkpoint_file", nargs='?', help='name of the checkpoint file')
    parser.add_argument('--dis', action="store_true", help='dump the disassembled checkpoint to stdout')
    parser.add_argument('args', help='arguments given to the resumed program', nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if args.demo:
        def sample(checkpointSupport):
            internalState = 1
            print("Internal state is ", internalState)
            while internalState < 3:
                flag, result = checkpointSupport.forkAndCheckpoint()
                if flag:
                    print("Created checkpoint")
                    return result
                print("Resuming ...")
                internalState += 1
                print("Internal state is ", internalState)
            print("Done")
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
