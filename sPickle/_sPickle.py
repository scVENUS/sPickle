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
import threading
import os.path
import collections
import operator
import functools
import inspect

#saved_dispatch = pickle.Pickler.dispatch.copy()
#saved_dispatch_table = pickle.dispatch_table.copy()

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

MODULE_TO_BE_PICKLED_FLAG_NAME="__module_must_be_pickled__"
    

WRAPPER_DESCRIPTOR_TYPE = type(object.__getattribute__)
METHOD_DESCRIPTOR_TYPE = type(object.__format__)
METHOD_WRAPPER_TYPE = type((1).__add__)

EMPTY_LIST_ITERATOR = iter([])
LISTITERATOR_TYPE = type(EMPTY_LIST_ITERATOR)

EMPTY_TUPLE_ITERATOR = iter(())
TUPLEITERATOR_TYPE = type(EMPTY_TUPLE_ITERATOR)

EMPTY_RANGE_ITERATOR = iter(xrange(0))
RANGEITERATOR_TYPE = type(EMPTY_RANGE_ITERATOR)

EMPTY_SET_ITERATOR = iter(set())
SETITERATOR_TYPE = type(EMPTY_SET_ITERATOR)

class _DelayedLogger(object):
    from imp import lock_held
    
    def __init__(self, name):
        self.__logger = None
        self.__name = name
        self.__tl = threading.local()
    
    def __getLogger(self):
        if self.__logger is None:
            import logging
            self.__logger = logging.getLogger(self.__name)
        return self.__logger
        
    def __getQueue(self):
        tl = self.__tl
        try:
            return tl.queue
        except AttributeError:
            tl.queue = collections.deque()
        return tl.queue
        
    def __call__(self):
        if self.lock_held():
            return self
        self.flush()
        return self.__getLogger()
    
    def __getattr__(self, name):
        f = functools.partial(self.__log, name)
        setattr(self, name, f)
        return f
    
    def __log(self, method, *args, **kw):
        if method is not None:
            queue = self.__getQueue()
            queue.append((method, args, kw))
        
        if not self.lock_held():
            queue = self.__getQueue()
            while True:
                try:
                    nextCall = queue.popleft()
                except IndexError:
                    break
                getattr(self.__getLogger(), nextCall[0])(*nextCall[1], **nextCall[2])
                
    def flush(self):
        self.__log(None)

LOGGER = _DelayedLogger(__name__)
            
            

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
# The next 8 functions are used to unpickle various types
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

def restore_modules_entry(doDel, old, new, preserveReferenceToNew=True):
    """Restore the content of sys.modules."""
    try:
        if preserveReferenceToNew:
            preservedModules = getattr(sys, "sPicklePreservedModules", None)
            if preservedModules is None:
                preservedModules = {}
                sys.sPicklePreservedModules = preservedModules
            preservedModules[id(new)] = new
        if doDel and sys.modules.has_key(new.__name__) and old == ():
            del sys.modules[new.__name__]
        if old != ():
            sys.modules[new.__name__] = old
    finally:
        from imp import release_lock
        release_lock()
    return new

#def import_module(name):
#    global sys
#    if sys is None:
#        sys = __import__('sys')
#    __import__(name)
#    return sys.modules[name]

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
    
if True:
    #
    # Recreate the functions with __GLOBALS_DICT as their global 
    # namesspace and a different function name. This way 
    # their definitions get pickled "by value" and not as a reference 
    # to this module
    #
    __GLOBALS_DICT = {'sys': sys,
                      'thread': thread,
                      'pickle': pickle,
                      'os': os,
                      'socket': socket,
                      '__builtins__': __builtins__}   
    __func = type(create_module)
#    import_module=__func(import_module.func_code, {'sys': None, '__import__': __import__}, 'import_module_')
    create_module=__func(create_module.func_code, __GLOBALS_DICT, 'create_module_')
    save_modules_entry=__func(save_modules_entry.func_code, __GLOBALS_DICT, 'save_modules_entry_')
    restore_modules_entry=__func(restore_modules_entry.func_code, 
                                 __GLOBALS_DICT, 
                                 'restore_modules_entry_',
                                 restore_modules_entry.func_defaults)
    create_thread_lock = __func(create_thread_lock.func_code, __GLOBALS_DICT, 'create_thread_lock_')
    create_null_file = __func(create_null_file.func_code, __GLOBALS_DICT, 'create_null_file_')
    create_closed_socket = __func(create_closed_socket.func_code, __GLOBALS_DICT, 'create_closed_socket_')
    create_closed_socketpair_socket = __func(create_closed_socketpair_socket.func_code,
                                             __GLOBALS_DICT,
                                            'create_closed_socketpair_socket_')
    del __func
    del __GLOBALS_DICT
        
class DictAlreadyExistError(pickle.PickleError):
    """The dictionary of an object has been pickled prior to the object itself.
    
    This exception is used for backtracking. Its 'obj'-attribute
    is the object, whose dictionary had already been pickled 
    """
    def __init__(self, msg, obj, *args, **kw):
        super(DictAlreadyExistError, self).__init__(msg, *args, **kw)
        self.obj = obj

class UnpicklingWillFailError(pickle.PicklingError):
    """This object can be pickled, but unpickling will probably fail.
    
    This usually caused by an incomplete implementation of the pickling 
    protocol or by a hostile __getattr__ or __getattribute__ method.
    """
    pass
        
class List2Writable(object):
    """A simple list to file adapter.
    
    Only write is supported.
    """
    def __init__(self, listish):
        self.write = listish.append
                
class Pickler(pickle.Pickler):
    """The sPickle Pickler.
    
    This Pickler is a subclass of :class:`pickle.Pickler` that adds the ability 
    to pickle modules, most classes and program state. It is intended to be 
    API-compatible with :class:`pickle.Pickler` so you can use it as a plug in 
    replacement. However its constructor has more optional arguments.
    """
    
    def __init__(self, file, protocol=pickle.HIGHEST_PROTOCOL, serializeableModules=None):
        """
        The file argument must be either an instance of :class:`collections.MutableSequence`
        or have a `write(str)` - method that accepts a single
        string argument.  It can thus be an open file object, a StringIO
        object, or any other custom object that meets this interface.
        As an alternative you can use a list or any other instance of 
        collections.MutableSequence.

        The optional protocol argument tells the pickler to use the
        given protocol; For this implementation, 
        the only supported protocol is 2 or `pickle.HIGHEST_PROTOCOL`.
        Specifying a negative protocol version selects the highest
        protocol version supported.  The higher the protocol used, the
        more recent the version of Python needed to read the pickle
        produced.

        The optional argument serializeableModules must be an iterable 
        collection of modules and strings. If the pickler needs to serialize
        a module, it checks this collection to decide, if the module needs to 
        be pickled by value or by name. The module gets pickled by value, 
        if at least one of the following conditions is true. Otherwise it 
        gets pickled by reference:
        
        * The module object is contained in serializeableModules.
        * The the name of the module starts with a string contained 
          in serializeableModules.
        * The module has an attribute `__file__` and module contains 
          a string, that is a substring of `__file__` after applying 
          a path and case normalization as appropriate for the 
          current system. 

        .. note::
        
            In order to be able to unpickle a module pickled by name, 
            the module must be importable. If this is not the case or if 
            the content of the module might change, you should tell the pickler 
            to pickle the module by value.
        
        """
        if protocol < 0:
            protocol = pickle.HIGHEST_PROTOCOL
        if protocol != pickle.HIGHEST_PROTOCOL:
            raise pickle.PickleError("The sPickle Pickler supports protocol %d only. Requested protocol was %d" %(pickle.HIGHEST_PROTOCOL, protocol))
        
        if serializeableModules is None:
            serializeableModules = []
        self.serializeableModules = serializeableModules

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
        self.dispatch[WRAPPER_DESCRIPTOR_TYPE] = self.saveWrapperOrMethodDescriptor.__func__
        self.dispatch[METHOD_DESCRIPTOR_TYPE] = self.saveWrapperOrMethodDescriptor.__func__
        self.dispatch[staticmethod] = self.saveStaticOrClassmethod.__func__
        self.dispatch[classmethod] = self.saveStaticOrClassmethod.__func__
        self.dispatch[property] = self.saveProperty.__func__
        self.dispatch[operator.itemgetter] = self.saveOperatorItemgetter.__func__
        self.dispatch[operator.attrgetter] = self.saveOperatorAttrgetter.__func__
                # initiallize the module_dict_ids module dict lookup table
        # this stackless specific call creates the dict self.module_dict_ids
        self._pickle_moduledict(self, {})
        self.object_dict_ids = {}
        
    def mustSerialize(self, obj):
        """test, if a module must be serialised"""
        # Legacy check for flowGuide2 workflow modules
        try:
            return bool(getattr(obj, MODULE_TO_BE_PICKLED_FLAG_NAME))
        except Exception:
            pass # Attribute is not present
        
        for item in self.serializeableModules:
            if obj is item:
                break
            elif isinstance(item, basestring):
                if obj.__name__ and obj.__name__.startswith(item):
                    break
                else:
                    f = getattr(obj, "__file__", None)
                    if ( f and 
                         os.path.normcase(os.path.normpath(item)) in os.path.normcase(os.path.normpath(f)) ):
                        break
        else:
            return False
        try:
            # Mark the module. The mark will be pickled and therefore 
            # the unpickled module will be considered beeing "mustSerialize" too 
            setattr(obj, MODULE_TO_BE_PICKLED_FLAG_NAME, True)
        except Exception, e:
            LOGGER().debug("Ohh, module %r is not writable: %r", obj.__name__, e)
        return True
        

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
                # Check the memo again, in case of a backtrack, that created obj
                x = self.memo.get(id(obj))
                if x:
                    self.write(self.get(x[0]))
                    return
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

        try:
            objDict = getattr(obj, "__dict__", None)
        except Exception:
            objDict = None
        if isinstance(objDict, types.DictType):
            dictId = id(objDict)
            x = self.memo.get(dictId)
            if x is not None:
                raise DictAlreadyExistError("__dict__ already pickled (memo %s) for %r" % (x[0], obj), obj)


        # special cases 
        if isinstance(obj, types.ModuleType):
            if self.saveModule(obj):
                return

        super_save = self.__class__.__bases__[0].save
        if isinstance(obj, types.FrameType):
            trace_func = obj.f_trace
            if trace_func is not None:
                obj.f_trace = self.TraceFunctionSurrogate()
            try:
                return super_save(self, obj)
            finally:
                if trace_func is not None:
                    obj.f_trace = trace_func
                    
        if obj is WRAPPER_DESCRIPTOR_TYPE:
            return self.save_reduce(type, (object.__getattribute__,), obj=obj)
        if obj is METHOD_DESCRIPTOR_TYPE:
            return self.save_reduce(type, (object.__format__,), obj=obj)
        if obj is METHOD_WRAPPER_TYPE:
            return self.save_reduce(type, ((1).__add__, ), obj=obj)
        if obj is LISTITERATOR_TYPE:
            return self.save_reduce(type, (EMPTY_LIST_ITERATOR,), obj=obj)
        if obj is TUPLEITERATOR_TYPE:
            return self.save_reduce(type, (EMPTY_TUPLE_ITERATOR,), obj=obj)
        if obj is RANGEITERATOR_TYPE:
            return self.save_reduce(type, (EMPTY_RANGE_ITERATOR,), obj=obj)
        if obj is SETITERATOR_TYPE:
            return self.save_reduce(type, (EMPTY_SET_ITERATOR,), obj=obj)
        if obj is EMPTY_LIST_ITERATOR:
            return self.save_reduce(iter, ([],), obj=obj)
        if obj is EMPTY_TUPLE_ITERATOR:
            return self.save_reduce(iter, ((),), obj=obj)
        if obj is EMPTY_RANGE_ITERATOR:
            return self.save_reduce(iter, (xrange(0),), obj=obj)
        if obj is EMPTY_SET_ITERATOR:
            return self.save_reduce(iter, (set(),), obj=obj)
                                
        # handle __new__ anbd similar methods of built-in types        
        if (isinstance(obj, type(object.__new__)) and 
            getattr(obj,"__name__", None) in ('__new__', '__subclasshook__')):
            return self.saveBuiltinNew(obj)

        if ( isinstance(obj, types.BuiltinMethodType) and 
             hasattr(obj, "__self__") and 
             obj.__self__ is not None ):
            return self.saveBuiltinMethod(obj)

        # avoid problems with some classes, that implement 
        # __getattribute__ or __getattr__ in a way, 
        # that getattr(obj, "__setstate__", None) raises an exception.
        try:
            getattr(obj, "__setstate__", None)
        except Exception, e:
            raise UnpicklingWillFailError("Object %r has hostile attribute access: %r" % (obj, e))
                
        return super_save(self, obj)
    def memoize(self, obj):
        """Store an object in the memo."""
        try:
            objDict = getattr(obj, "__dict__", None)
        except Exception:
            objDict = None
        if isinstance(objDict, types.DictType):
            dictId = id(objDict)
            # x = self.memo.get(dictId)
            
            self.object_dict_ids[dictId] = obj

        if id(obj) in self.memo:
            m = self.memo[id(obj)]
            LOGGER().error("Object already in memo! Id: %d, Obj: %r of type %r, Memo: %d=%r of type %r", 
                         id(obj), obj, type(obj), 
                         m[0], m[1], type(m[1]))
        return self.__class__.__bases__[0].memoize(self, obj)
        
    def save_dict(self, obj):
        if obj is sys.modules:
            self.write(pickle.GLOBAL + 'sys' + '\n' + 'modules' + '\n')
            self.memoize(obj)
            return
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
            __import__(module)
            mod = sys.modules[module]
            klass = getattr(mod, name)
        except (ImportError, KeyError, AttributeError):
            if t in (type, types.ClassType):
                return self.saveClass(obj)
            raise pickle.PicklingError(
                "Can't pickle %r: it's not found as %s.%s" %
                (obj, module, name))
        else:
            if klass is not obj:
                if mod is sys and hasattr(mod, "__" + name + "__"):
                    # special case for some functions from sys. 
                    # Sys contains two copies of these functions, one 
                    # named <name> and the other named "__<name>__".  
                    return self.save_global(obj, "__"+name+"__", pack)
                if t in (type, types.ClassType):
                    return self.saveClass(obj)
                raise pickle.PicklingError(
                    "Can't pickle %r: it's not the same object as %s.%s" %
                    (obj, module, name))

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

    def save_function(self, obj):
        memo = self.memo
        writePos = len(self.writeList) 
        memoPos = len(memo)
        try:
            return self.save_global(obj)
        except pickle.PicklingError, e:
            LOGGER().debug("Going to pickle function %r by value, because it can't be pickled as global: %r", obj, e)
            del self.writeList[writePos:]
            for k in memo.keys():
                v= memo[k]
                if isinstance(v, types.TupleType):
                    if v[0] >= memoPos:
                        del memo[k]
        # Check copy_reg.dispatch_table
        reduce = pickle.dispatch_table.get(type(obj))
        if reduce:
            rv = reduce(obj)
        else:
            # Check for a __reduce_ex__ method, fall back to __reduce__
            reduce = getattr(obj, "__reduce_ex__", None)
            if reduce:
                rv = reduce(self.proto)
            else:
                reduce = getattr(obj, "__reduce__", None)
                if reduce:
                    rv = reduce()
                else:
                    raise e
        return self.save_reduce(obj=obj, *rv)

    def saveClass(self, obj):
        write = self.write
        f = type(obj)
        name = obj.__name__
        d1 = {}; d2 = {}
        for (k, v) in obj.__dict__.iteritems():
            if k in ('__doc__', '__module__', '__slots__'):
                d1[k] = v
                continue
            if type(v) in (types.DictProxyType, types.GetSetDescriptorType, types.MemberDescriptorType):
                continue
            if k in ('__dict__', '__class__' ):
                continue
            d2[k] = v
        self.save_reduce(f,(name, obj.__bases__, d1), obj=obj)
        
        # Use setattr to set the remaining class members. 
        # unfortunately, we can't use the state parameter of the 
        # save_reduce method (pickle BUILD-opcode), because BUILD 
        # invokes a __setstate__ method eventually inherited from 
        # a super class.
        keys = d2.keys()
        keys.sort()
        for k in keys:
            v = d2[k]
            self.save_reduce(setattr, (obj, k, v))
            write(pickle.POP)

    def saveModule(self, obj, reload=False):
        write = self.write
        
        self.module_dict_ids[id(obj.__dict__)] = obj
        
        if not self.mustSerialize(obj):
            # help pickling unimported modules
            if obj is not sys.modules.get(obj.__name__):
            # if obj.__name__ not in sys.modules:
                try:
                    self.save_global(obj)
                    return True
                except pickle.PicklingError:
                    LOGGER().exception("Can't pickle anonymous module: %r", obj)
                    raise
                
            # self.save_reduce(import_module, (obj.__name__,), obj=obj)
            if __import__(obj.__name__) is obj:
                self.save_reduce(__import__, (obj.__name__,), obj=obj)
            else:
                self.save_reduce(__import__, (obj.__name__,))
                write(pickle.POP)
                self.save_reduce(operator.getitem, (sys.modules, obj.__name__), obj=obj)
            return True
                
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
            restore_modules_entry(True, savedModule, obj, preserveReferenceToNew=False)
            # Flush log messages
            LOGGER.flush()
            
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

    def saveBuiltinNew(self, obj):
        t = obj.__self__
        if obj is not getattr(t, obj.__name__) and  obj.__name__ != '__subclasshook__':
            raise pickle.PicklingError("Can't pickle %r: it's not the same object as %s.%s" %
                        (obj, t, obj.__name__))
        return self.save_reduce(getattr, (t, obj.__name__), obj=obj)
    
    def saveBuiltinMethod(self, obj):
        objSelf = obj.__self__
        objName = obj.__name__
        if getattr(objSelf, objName, None) != obj:
            raise pickle.PicklingError("Can't pickle %r: it's not the same object as %s.%s" %
                        (obj, objSelf, objName))
        return self.save_reduce(getattr, (objSelf, objName), obj=obj)
        
    def saveWrapperOrMethodDescriptor(self, obj):
        t = obj.__objclass__
        if obj is not getattr(t, obj.__name__):
            raise pickle.PicklingError("Can't pickle %r: it's not the same object as %s.%s" %
                        (obj, t, obj.__name__))
        return self.save_reduce(getattr, (t, obj.__name__), obj=obj)
    
    def saveStaticOrClassmethod(self, obj):
        return self.save_reduce(type(obj), (obj.__func__, ), obj=obj)
    
    def saveProperty(self,obj):
        return self.save_reduce(type(obj), (obj.fget, obj.fset, obj.fdel, obj.__doc__), obj=obj)
    
    class OperatorItemgetterProbe(object):
        def __init__(self):
            self.items = []
        def __getitem__(self, key):
            self.items.append(key)
            return None
        
    def saveOperatorItemgetter(self, obj):
        probe = self.OperatorItemgetterProbe()
        obj(probe)
        return self.save_reduce(type(obj), tuple(probe.items), obj=obj)
    
    class OperatorAttrgetterProbe(object):
        def __init__(self, record, index=None):
            self.record = record
            self.index = index
        def __getattribute__(self, name):
            record = object.__getattribute__(self, "record")
            index = object.__getattribute__(self, "index")
            if index is None:
                index = len(record)
                record.append(name)
            else:
                record[index] = record[index] + "." + name
            return type(self)(record, index)
    
    def saveOperatorAttrgetter(self, obj):
        record = []
        probe = self.OperatorAttrgetterProbe(record)
        obj(probe)
        return self.save_reduce(type(obj), tuple(record), obj=obj)
    
        
class SPickleTools(object):
    """A collection of simple utility methods.
    
    .. warning::
       This class is still under development. Don't rely on its 
       methods. If you need a stable API use the class :class:`Pickler` directly 
       or copy the code.
       
    """
    
    def __init__(self, serializeableModules=None):
        """
        The optional argument serializeableModules is passed 
        on to the class :class:`Pickler`.
        """
        if serializeableModules is None:
            serializeableModules = []
        self.serializeableModules = serializeableModules
        
    def dumps(self, obj, persistent_id = None, doCompress=True):
        """Pickle an object and return the pickle
        
        This method works similar to the regular dumps method, but
        also optimizes and optionally compresses the pickle.
        
        :param object obj: object to be pickled
        :param persistent_id: the persistent_id function for the pickler.
                              See the section "Pickling and unpickling external objects" of the 
                              documentation of module :mod:`Pickle`.
        :param doCompress: If doCompress yields `True` in a boolean context, the 
                           pickle will be compressed, if the compression actually 
                           reduces the size of the pickle. The compression method depends
                           on the exact value of doCompress. If doCompress is callable,
                           it is called to perform the compression. doCompress
                           must be a function (or method), that takes a single string parameter
                           and returns a compressed version. Otherwise, if doCompress is not
                           callable the function :func:`bz2.compress` is used.
        :return: the pickle, optionally compressed
        :rtype: :class:`str`
        """
        l = []
        pickler = Pickler(l, 2, serializeableModules=self.serializeableModules)
        if persistent_id is not None:
            pickler.persistent_id = persistent_id
        pickler.dump(obj)
        p = pickletools.optimize("".join(l))
        if doCompress:
            if callable(doCompress):
                c = doCompress(p)
            else:
                # use the bz2 compression
                c = compress(p)
            if len(c) < len(p):
                return c
        return p
        
    def dumps_with_external_ids(self, obj, idmap, matchResources=False, matchNetref=False):
        """
        Pickle an object, that references objects that can't be pickled.
        
        If you want to pickle an object, that references a resource (files,
        sockets, etc) or references a RPyC-proxy for an object on a remote system
        you can't pickle the referenced object. But if you are going to transfer
        the pickle to a remote system using the package RPyC, you can replace the 
        resources by an RPyC proxy objects and replace RPyC proxy objects by the 
        real objects. 
        
        This method creates an :class:`Pickler` object with a `persistent_id` method
        that optionally replaces resources and proxy objects by their object id. It stores 
        the mapping between ids and objects in the idmap dictionary (or any other mutable mapping).
        
        :param object obj: the object to be pickled
        :param idmap: receives the id to object mapping
        :type idmap: :class:`dict`
        :param object matchResources: if true in a boolean context, replace resource objects. 
        :param object matchNetref: if true in a boolean context, replace RPyC proxies (technically 
                            objects of class :class:`rpyc.core.netref.BaseNetref`).
        """
        def persistent_id(obj):
            oid = id(obj)
            if oid in idmap:
                return oid
            isResource = matchResources and isinstance(obj, RESOURCE_TYPES)
            isNetRef = matchNetref and isRpycProxy(obj)
            if isResource or isNetRef:
                if isNetRef:
                    objrepr = "RPyC Netref"
                try:
                    objrepr = repr(obj)
                except Exception:
                    objrepr = "-- repr failed --"
                LOGGER().debug("Pickling object %s of type %r using persistent id %d", objrepr, type(obj), oid)
                idmap[oid] = obj
                return oid
            return None

        pickle = self.dumps(obj, persistent_id)
        return pickle
    
    @classmethod
    def loads_with_external_ids(cls, str, idmap):
        """
        Unpickle an object from a string.
        
        Replace ids for external objects with 
        the objects provided in idmap.
        
        :param str: the pickle
        :type str: :class:`str`
        :param idmap: the mapping, that contains the objects for the id values used in the pickle
        :type idmap: dict
        :return: the reconstructed object
        :rtype: object
        """ 
        def persistent_load(oid):
            try:
                return idmap[oid]
            except KeyError:
                raise cPickle.UnpicklingError("Invalid id %r" % (oid,))
        return cls.loads(str, persistent_load)
    
    @classmethod
    def loads(cls, str, persistent_load=None, useCPickle=True):
        """
        Unpickle an object from a string.

        :param str: the pickle
        :type str: :class:`str`
        :param persistent_load: The `persistent_load` method for the 
                                unpickler.  
                                See the section "Pickling and unpickling external objects" of the 
                                documentation of module :mod:`Pickle`.
        :param object useCPickle: if True in a boolean context, use the Unpickler from the 
                           module :mod:`cPickle`. Otherwise use the much slower Unpickler from 
                           the module :mod:`pickle`.
        :return: the reconstructed object
        :rtype: object        
        """
        if str.startswith("BZh9"):
            str = decompress(str)
        file = StringIO(str)
        p = cPickle if useCPickle else pickle
        unpickler = p.Unpickler(file)
        if persistent_load is not None:
            unpickler.persistent_load = persistent_load
        return unpickler.load()

    @classmethod
    def dis(cls, str, out=None, memo=None, indentlevel=4):
        """
        Disassemble an optionally compressed pickle.
        
        See function :func:`pickletools.dis` for details.
        """
        if str.startswith("BZh9"):
            str = decompress(str)
        pickletools.dis(str, out, memo, indentlevel)
        
    @classmethod
    def getImportList(cls, str):   
        """
        Return a list containing all imported modules from the pickle `str`.
        
        Somtimes useful for debuging.
        """
        if str.startswith("BZh9"):
            str = decompress(str)
        importModules = []   
        opcodesIt = pickletools.genops(str)
        for opcodes in opcodesIt:
            if opcodes[0].name == "GLOBAL": 
                importModules.append(opcodes[1])
        return importModules        
          
    CREATE_IMMEDIATELY = 'immedately'
    """
    Constant to be given to the `create_only_once` argument of method :meth:`remotemethod`. 
    Create the function on the remote side during the invocation of :meth:`remotemethod`.
    """

    CREATE_LAZY = True
    """
    Constant to be given to the `create_only_once` argument of method :meth:`remotemethod`. 
    Create the function on the remote side on the first invocation of the function returned by :meth:`remotemethod`.
    The actual value is `True`.
    """

    CREATE_EVERYTIME = False
    """
    Constant to be given to the `create_only_once` argument of method :meth:`remotemethod`. 
    Create the function on the remote side on every invocation of the function returned by :meth:`remotemethod`.
    The actual value is `False`.
    """

    def remotemethod(self, rpycconnection, method=None, create_only_once=None):
        """Create a remote function.
        
        This method takes an active RPyC connection and 
        a locally defined function (or method) and returns 
        a proxy for an equivalent function on the remote side.
        If you invoke the proxy, it will create a pickle containing the 
        function, transfer this pickle to the remote side, 
        unpickle it and invoke the function. It then pickles the result 
        and transfers the result back to the local side. It will not pickle the 
        function arguments. If you need to transfer the function arguments by 
        value, use :func:`functools.partial` to apply them to your 
        function prior to the call of remotemethod.
        
        :param rpycconnection: an active RPyC connection. If set to `None`, execute
                               method localy.
        :type rpycconnection: :class:`rpyc.core.protocol.Connection`
        :param object method: a callable object. If you do not give this argument, 
            you can use remotemethod as a decorator.
        :param create_only_once: controlls the creation of the function on the 
            remote side. If you want to create the function during the execution 
            of :meth:`remotemethod`, pass :attr:`CREATE_IMMEDIATELY`. Otherwise,
            if you want to create the remote function on its first invokation,
            set create_only_once to a value that is `True` in a boolean context.
            Otherwise, if you set create_only_once evaluates to `False`, 
            the local proxy creates the create the remote 
            function on every invocation.

        :return: the proxy for the remote function
        
        .. note::
           If you use `remotemethod` as a decorator, do not apply it on regular
           methods of a class. It does not work in the desired way, because decorators 
           work on the underlying function object, not on the method object. Therefore 
           you will end up with a remote function, that recives a RPyC proxy for `self`.
           
        """
        if method is None:
            return functools.partial(self.remotemethod, rpycconnection, create_only_once=create_only_once)

        if create_only_once == self.CREATE_IMMEDIATELY:
            rmethod_list = (self._build_remotemethod(rpycconnection, method),)
        else:
            rmethod_list = [bool(create_only_once)]

        def wrapper(*args, **keywords):
            if rpycconnection is None:
                # the local case.
                return method(*args, **keywords)

            if callable(rmethod_list[0]):
                rmethod = rmethod_list[0]
            else:
                rmethod = self._build_remotemethod(rpycconnection, method)
                if rmethod_list[0] is True:
                    rmethod_list[0] = rmethod

            r0, r1 = rmethod(*args, **keywords)
            if r0 is None:
                return r1
            idmap = {}
            idmap.update(r1)
            return self.loads_with_external_ids(r0, idmap)
        functools.update_wrapper(wrapper, method)
        return wrapper
                
    def _build_remotemethod(self, rpycconnection, method):
        """return a remote method"""
        idmap = {}
        pickle = self.dumps_with_external_ids(method, idmap, matchResources=True)
        try:
            rcls = rpycconnection.root.getmodule(self.__class__.__module__)
            rcls = getattr(rcls, self.__class__.__name__)
        except ImportError, e:
            LOGGER().debug("Remote side lacks the module, going to pickle it")
            import sPickle._sPickle
            pt = SPickleTools(serializeableModules=[sPickle, sPickle._sPickle])
            # no compression to be compatible with the plain unpickler
            rcls_pickled = pt.dumps((sPickle, self.__class__), doCompress=False)
            rcls = rpycconnection.root.getmodule("cPickle").loads(rcls_pickled)[1]
        remotemethod = rcls()._build_remotemethod_remote(pickle, idmap)
        return remotemethod
    
    def _build_remotemethod_remote(self, pickle, idmap):
        m = self.loads_with_external_ids(pickle, idmap)
        def returnWrapper(*args, **keywords):
            r = m(*args, **keywords)
            if isRpycProxy(r):
                return (None, r)
            idmap = {}
            pickle = self.dumps_with_external_ids(r, idmap, matchNetref=True)
            return (pickle, idmap)
        return returnWrapper

    @classmethod
    def module_for_globals(cls, callable_or_moduledict, withDefiningModules=False):
        """
        Get the module associated with a callable or a module dictionary.

        If you pickle a module, make sure to keep a reference to unpickled module.
        Otherwise the destruction of the module will clear the modules 
        dictionary. Usually, the sPickle code for serializing modules, preserves 
        a reference to modules created from a pickle but not imported into 
        sys.modules. However, there might be cases, where you need to identify 
        relevant modules yourself. This method can be used, to find the relevant module(s).

        :param callable_or_moduledict: a function or a method or a module dictionary
        :param object withDefiningModules: if True and callable_or_moduledict is a callable, 
                                           return also the module defining the callable.
        :return: `None`, or a single module or a set of modules
        """
        g = None
        modules = []
        result = set()
        if isinstance(callable_or_moduledict, dict):
            g = callable_or_moduledict
        else:
            func = callable_or_moduledict
            if withDefiningModules:
                try:
                    modules.append(func.__module__)
                except Exception:
                    pass
            if inspect.ismethod(func):
                func = func.im_func
                if withDefiningModules:
                    try:
                        modules.append(func.__module__)
                    except Exception:
                        pass
            if inspect.isfunction(func):
                g = func.func_globals
        for v in modules:
            if v:
                m = sys.modules.get(v)
                if m is not None:
                    result.add(m)
        if g is not None:
            for m in sys.modules.itervalues():
                if g is getattr(m, '__dict__', None):
                    result.add(m)
        if len(result) == 0:
            return None
        if len(result) == 1:
            return result.pop()
        return result


class StacklessTaskletReturnValueException(BaseException):
    """This exception can be used to return a value from a Stackless Python tasklet.
    
    Usually a tasklet has no documented way to return a value at the end of its live. But it can 
    raise an exception. You can use this exception to encapsulate a return value.
    Because this exception is not a real error and you usually do not want 
    to catch this exception in normal "error" handling, it 
    is derived from :exc:`BaseException` instead of :exc:`Exception`.
    """
    def __init__(self, value):
        self.value = value
