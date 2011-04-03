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

import pickle
import cPickle
import pickletools
import thread
import os.path
import collections

saved_dispatch = pickle.Pickler.dispatch.copy()
saved_dispatch_table = pickle.dispatch_table.copy()

import types
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from bz2 import compress, decompress
import sys
import struct
import socket
import tempfile
import codecs

SOCKET_PAIR_TYPE = None
if hasattr(socket, "socketpair"):
    _sp = socket.socketpair()
    SOCKET_PAIR_TYPE = type(_sp[0])
    _sp[0].close()
    _sp[1].close()
    del _sp
else:
    _s = socket.socket()
    SOCKET_PAIR_TYPE = type(_s._sock)
    _s.close()
    del _s
    

__LOGGER = None
def LOGGER():
    global __LOGGER
    if __LOGGER is None:
        import logging
        __LOGGER = logging.getLogger(__name__)
    return __LOGGER


# types to be replaced by proxies, if possible  , 
RESOURCE_TYPES = (file, socket.SocketType, SOCKET_PAIR_TYPE, socket._closedsocket,
                  tempfile._TemporaryFileWrapper, tempfile.SpooledTemporaryFile,
                  codecs.StreamReader, codecs.StreamWriter)

try:
    from rpyc.core.netref import BaseNetref as _BaseNetref
except ImportError:
    def isRpycProxy(obj):
        # rpyc is not available
        return False
else:
    def isRpycProxy(obj):
        return isinstance(obj, _BaseNetref)


#
# the next 4 functions are used to unpickle modules
#

def create_module(cls, name, doc=None):
    """create and imports a module.
    
    If sys.modules already contains a module
    with the same name, this module gets 
    reloaded
    """
    mod = sys.modules.get(name)
    if type(mod) is cls:
        mod.__dict__.clear()
        mod.__dict__['__doc__'] = doc
    else:
        mod = cls(name, doc)
        sys.modules[name] = mod
    return mod
        
def save_modules_entry(name):
    """returns a module from sys.modules
    
    If sys.modules has no emtry for name, an
    empty tuple is returned as a flag object. (We
    can't use None, because None is a possible value
    in sys.modules.)
    """
    from imp import acquire_lock
    acquire_lock()
    if not sys.modules.has_key(name):
        return ()  # just a dummy marker
    mod = sys.modules.get(name)
    del sys.modules[name]
    return mod

def restore_modules_entry(doDel, old, new):
    """Restore the content of sys.modules."""
    try:
        if doDel and sys.modules.has_key(new.__name__) and old == ():
            del sys.modules[new.__name__]
        if old != ():
            sys.modules[new.__name__] = old
    finally:
        from imp import release_lock
        release_lock()
    return new


def create_thread_lock(locked):
    """recreate a lock object"""
    l = thread.allocate_lock()
    if locked:
        if not l.acquire(0):
            raise pickle.UnpicklingError("Failed to aquire a nely created lock")
    return l

def create_null_file(mode, closed):
    """recreate a file object"""
    f = open(os.devnull, mode)
    if closed:
        f.close()
    return f

def create_closed_socket():
    """recreate a file object"""
    s = socket.socket()
    s.close()
    return s

def create_closed_socketpair_socket():
    if hasattr(socket, "socketpair"):
        sp = socket.socketpair()
        sp[0].close()
        sp[1].close()
        return sp[0]
    so = socket.socket()
    s = so._sock
    so.close()
    return s
    
class DictAlreadyExistError(pickle.PicklingError):
    """The dictionary of an object has been pickled prior to the object itself.
    
    This exception is used for backtracking. Its 'obj'-attribute
    is the object, whose dictionary had already been pickled 
    """
    def __init__(self, msg, obj, *args, **kw):
        super(DictAlreadyExistError, self).__init__(msg, *args, **kw)
        self.obj = obj
        
class List2Writable(object):
    """A simple list to file adapter.
    
    Only write is supported.
    """
    def __init__(self, listish):
        self.write = listish.append
                
class Pickler(pickle.Pickler):
    """The flowGuide pickler.
    
    This pickler supports pickling of modules and program state
    """
    
    def mustSerialize(self, obj):
        """test, if a module must be serialised"""
        # Legacy check for flowGuide2 workflow modules 
        if getattr(obj, "__wf_module__", None):
            return True
        if obj in self.serializeableModules:
            return True
        for item in self.serializeableModules:
            if obj is item:
                return True
            if isinstance(item, basestring):
                if obj.__name__ and obj.__name__.startswith(item):
                    return True
                f = getattr(obj, "__file__", None)
                if f and os.path.normcase(os.path.normpath(f)).find(
                        os.path.normcase(os.path.normpath(item))) != -1:
                    return True
                
        
    
    
    
    
    def __init__(self, file, protocol=pickle.HIGHEST_PROTOCOL):
        """This takes a file-like or a list-like object for writing a pickle data stream.

        The optional protocol argument tells the pickler to use the
        given protocol; the only supported protocol is 2.  The default
        protocol is 2. (Protocol 0 is the
        only protocol that can be written to a file opened in text
        mode and read back successfully.  When using a protocol higher
        than 0, make sure the file is opened in binary mode, both when
        pickling and unpickling.)

        Specifying a negative protocol version selects the highest
        protocol version supported.  The higher the protocol used, the
        more recent the version of Python needed to read the pickle
        produced.

        The file parameter must have a write() method that accepts a single
        string argument.  It can thus be an open file object, a StringIO
        object, or any other custom object that meets this interface.
        As an alternative you can use a list or any other instance of 
        collections.MutableSequence.

        """
        if protocol < 0:
            protocol = pickle.HIGHEST_PROTOCOL
        if protocol != pickle.HIGHEST_PROTOCOL:
            raise pickle.PickleError("The sPickle Pickler supports protocol %d only. Requested protocol was %d" %(pickle.HIGHEST_PROTOCOL, protocol))

        self.__fileIsList = isinstance(file, collections.MutableSequence)
        if self.__fileIsList:
            listish = file
        else:
            listish = []
            self.__write = file.write
            
        pickle.Pickler.__init__(self, List2Writable(listish), protocol)
        self.writeList = listish
        
        # copy and patch the dispatch table
        self.dispatch = self.__class__.dispatch.copy()
        for k in pickle.Pickler.dispatch.iterkeys():
            f = getattr(Pickler, pickle.Pickler.dispatch[k].__name__, None)
            if f.__func__ is not pickle.Pickler.dispatch[k]:
                self.dispatch[k] = f.__func__
        self.dispatch[thread.LockType] = self.saveLock.__func__
        self.dispatch[types.FileType] = self.saveFile.__func__
        self.dispatch[socket.SocketType] = self.saveSocket.__func__
        self.dispatch[SOCKET_PAIR_TYPE] = self.saveSocketPairSocket.__func__
        
        #self.dispatch[types.ModuleType] = Pickler.saveModule
        
        # initiallize the module_dict_ids module dict lookup table
        # this stackless specific call creates the dict self.module_dict_ids
        self._pickle_moduledict(self, {})
        self.object_dict_ids = {}
        
        self.serializeableModules = [] 

    def dump(self, obj):
        """Write a pickled representation of obj to the open file."""
        try:
            if self.proto >= 2:
                self.write(pickle.PROTO + chr(self.proto))
            self.dict_checkpoint(obj, self.save)
            self.write(pickle.STOP)
        finally:
            if not self.__fileIsList:
                self.__write("".join(self.writeList))

    def dict_checkpoint(self, obj, method, *args, **kw):
        """Checkpint for dictionary backtracking"""
        memo = self.memo
        writePos = len(self.writeList) 
        memoPos = len(self.memo)
        
        done = False
        saveList = []
        currentSave = None
        while not done:
            try:
                for currentSave in saveList:
                    self.save(currentSave)
                    self.write(pickle.POP)
                currentSave = None
                method(obj, *args, **kw)
                done = True
            except DictAlreadyExistError, e:
                dictHolder = e.obj
                assert currentSave is not dictHolder
                if memo[id(dictHolder.__dict__)][0] < memoPos:
                    raise
                
                saveList.insert(0, e.obj)
                del self.writeList[writePos:]
                for k in memo.keys():
                    v= memo[k]
                    if isinstance(v, types.TupleType):
                        if v[0] >= memoPos:
                            del memo[k]
                            

    class TraceFunctionSurrogate(object):
        def __reduce__(self):
            return (sys.gettrace, ())

    def save(self, obj):
        # Check for persistent id (defined by a subclass)
        pid = self.persistent_id(obj)
        if pid:
            self.save_pers(pid)
            return

        # Check the memo
        x = self.memo.get(id(obj))
        if x:
            self.write(self.get(x[0]))
            return

        objDict = getattr(obj, "__dict__", None)
        if isinstance(objDict, types.DictType):
            dictId = id(objDict)
            x = self.memo.get(dictId)
            if x is not None:
                raise DictAlreadyExistError("__dict__ already pickled (memo %s) for %r" % (x[0], obj), obj)


        # special cases 
        if isinstance(obj, types.ModuleType):
            if self.saveModule(obj):
                return
            
        if isinstance(obj, types.FrameType):
            trace_func = obj.f_trace
            if trace_func is not None:
                obj.f_trace = self.TraceFunctionSurrogate()
            try:
                return self.super_save(obj)
            finally:
                if trace_func is not None:
                    obj.f_trace = trace_func
                
        self.super_save(obj)
 
    def super_save(self, obj):
        return self.__class__.__bases__[0].save(self, obj)
    
    def memoize(self, obj):
        """Store an object in the memo."""
        objDict = getattr(obj, "__dict__", None)
        if isinstance(objDict, types.DictType):
            dictId = id(objDict)
            # x = self.memo.get(dictId)
            
            self.object_dict_ids[dictId] = obj
        return self.__class__.__bases__[0].memoize(self, obj)
        
    def save_dict(self, obj):
        self.dict_checkpoint(obj, self._save_dict_impl)
        
    def _save_dict_impl(self, obj):
        ## Stackless addition BEGIN
        modict_saver = self._pickle_moduledict(self, obj)
        if modict_saver is not None:
            mod = modict_saver[1][0]
            if not self.mustSerialize(mod):
                return self.save_reduce(*modict_saver, obj=obj)
        ## Stackless addition END
        write = self.write
        parent = self.object_dict_ids.get(id(obj))
        if parent is not None:
            del self.object_dict_ids[id(obj)]
            self.save_reduce(getattr, (parent, "__dict__"))
        elif self.bin:
            write(pickle.EMPTY_DICT)
        else:   # proto 0 -- can't use EMPTY_DICT
            write(pickle.MARK + pickle.DICT)

        self.memoize(obj)
        self._batch_setitems(obj.iteritems())

    def save_global(self, obj, name=None, pack=struct.pack):
        write = self.write
        memo = self.memo
        t = type(obj)

        if name is None:
            name = obj.__name__

        module = None
        if t is type:
            for k in dir(types):
                if obj is getattr(types, k, None):
                    module = types.__name__
                    name = k
                    break

        if module is None:
            module = getattr(obj, "__module__", None)
            if module is None:
                module = pickle.whichmodule(obj, name)

        try:
            try:
                __import__(module)
                mod = sys.modules[module]
                klass = getattr(mod, name)
            except (ImportError, KeyError, AttributeError):
                raise pickle.PicklingError(
                    "Can't pickle %r: it's not found as %s.%s" %
                    (obj, module, name))
            else:
                if klass is not obj:
                    raise pickle.PicklingError(
                        "Can't pickle %r: it's not the same object as %s.%s" %
                        (obj, module, name))
        except pickle.PicklingError:
            if t in (type, types.ClassType):
                return self.saveClass(obj)
            raise

        if self.proto >= 2:
            code = pickle._extension_registry.get((module, name))
            if code:
                assert code > 0
                if code <= 0xff:
                    write(pickle.EXT1 + chr(code))
                elif code <= 0xffff:
                    write("%c%c%c" % (pickle.EXT2, code&0xff, code>>8))
                else:
                    write(pickle.EXT4 + pack("<i", code))
                return

        ## Fg2 specific start
        if self.mustSerialize(mod):
            self.save(mod)
            self.write(pickle.POP)
            x = memo.get(id(obj))
            if x is not None:
                return self.write(self.get(x[0], pack))
            # stange: obj is not contained in the dictionary of its module
            # probably the type of mod is a strange subclass of module
            return self.save_reduce(getattr, (mod, name), obj=obj)
        
        write(pickle.GLOBAL + module + '\n' + name + '\n')
        self.memoize(obj)

    def saveClass(self, obj):
        f = type(obj)
        name = obj.__name__
        d1 = {}; d2 = {}
        for (k, v) in obj.__dict__.iteritems():
            if k in ('__doc__', '__module__'):
                d1[k] = v
                continue
            if type(v) in (types.DictProxyType, types.GetSetDescriptorType, types.MemberDescriptorType):
                continue
            if k in ('__dict__', '__class__' ):
                continue
            d2[k] = v
        self.save_reduce(f,(name, obj.__bases__, d1), (None,d2), obj=obj)
    
    
    def saveModule(self, obj, reload=False):
        write = self.write
        
        self.module_dict_ids[id(obj.__dict__)] = obj
        
        if not self.mustSerialize(obj):
            # help pickling unimported modules
            if obj.__name__ not in sys.modules:
                try:
                    self.save_global(obj)
                    return True
                except pickle.PicklingError:
                    pass
            # continue with the default procedure
            return False  
        # do it ourself
        
        # save the current implementation of the module
        doDel = obj.__name__ not in sys.modules
        if doDel or not reload:
            self.save(restore_modules_entry)
            self.write(pickle.TRUE if doDel else pickle.FALSE)
            self.save_reduce(save_modules_entry, (obj.__name__,))
            
        savedModule = save_modules_entry(obj.__name__)
        try:            
            # create the module
            self.save_reduce(create_module, (type(obj), obj.__name__, getattr(obj, "__doc__", None)), obj.__dict__, obj=obj)
        finally:
            restore_modules_entry(True, savedModule, obj)
            
        if doDel or not reload:
            write(pickle.TUPLE3+pickle.REDUCE)
        return True

    def saveLock(self, obj):
        return self.save_reduce(create_thread_lock, (obj.locked(), ), obj=obj)

    def saveFile(self, obj):
        sysname = None
        if obj is sys.stdout:
            sysname = "stdout"
        elif obj is sys.stderr:
            sysname = "stderr"
        elif obj is sys.stdin:
            sysname = "stdin"
        elif obj is sys.__stdout__:
            sysname = "__stdout__"
        elif obj is sys.__stderr__:
            sysname = "__stderr__"
        elif obj is sys.__stdin__:
            sysname = "__stdin__"
        if sysname:
            LOGGER().info("Pickling a reference to sys.%s", sysname)
            self.write(pickle.GLOBAL + "sys" + '\n' + sysname + '\n')
            self.memoize(obj)
            return
        LOGGER().warn("Pickling file %r as null-file", obj)
        mode = getattr(obj, "mode", "rwb")
        closed = getattr(obj, "closed", False)
        return self.save_reduce(create_null_file, (mode, closed), obj=obj)
            
    def saveSocket(self, obj):
        LOGGER().warn("Pickling socket %r as closed socket", obj)
        return self.save_reduce(create_closed_socket, (), obj=obj)
    
    def saveSocketPairSocket(self, obj):
        LOGGER().warn("Pickling socket-pair socket %r as closed socket", obj)
        return self.save_reduce(create_closed_socketpair_socket, (), obj=obj)

class WfPickler(object):
    
    def save(self, fileish, obj, *more):
        fileish.write(self.dumps(obj, *more))
        
    def dumps(self, obj, persistent_id = None):
        l = []
        pickler = Pickler(l, 2)
        if persistent_id is not None:
            pickler.persistent_id = persistent_id
        pickler.dump(obj)
        p = pickletools.optimize("".join(l))
        c = compress(p)
        if len(c) < len(p):
            return c
        return p
        
    def dumps_with_external_ids(self, obj, idmap, matchResources=False, matchNetref=False):
        def persistent_id(obj):
            oid = id(obj)
            if oid in idmap:
                return oid 
            if (( matchResources and isinstance(obj, RESOURCE_TYPES) ) or
                ( matchNetref and isRpycProxy(obj) )):
                idmap[oid] = obj
                return oid
            return None

        pickle = self.dumps(obj, persistent_id)
        return pickle
    
    def loads_with_external_ids(self, str, idmap):
        def persistent_load(oid):
            try:
                return idmap[oid]
            except KeyError:
                raise cPickle.UnpicklingError("Invalid id %r" % (oid,))
        return self.loads(str, persistent_load)
    
    #def load(file):
    #    return Unpickler(file).load()

    def loads(self, str, persistent_load=None):
        if str.startswith("BZh9"):
            str = decompress(str)
        file = StringIO(str)
        unpickler = cPickle.Unpickler(file)
        if persistent_load is not None:
            unpickler.persistent_load = persistent_load
        return unpickler.load()

    def dis(self, str, out=None, memo=None, indentlevel=4):
        if str.startswith("BZh9"):
            str = decompress(str)
        pickletools.dis(str, out, memo, indentlevel)
        
    def getImportList(self, str):   
        """returns a list containing all imported modules from the pickled 
        data.
        """
        if str.startswith("BZh9"):
            str = decompress(str)
        importModules = []   
        opcodesIt = pickletools.genops(str)
        for opcodes in opcodesIt:
            if opcodes[0].name == "GLOBAL": 
                importModules.append(opcodes[1])
        return importModules        
                
