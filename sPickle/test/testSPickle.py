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

from unittest import TestCase, skipIf

import pickletools
from StringIO import StringIO
import pickle
import types
import sys
import imp
import traceback
import thread
import functools
# a pure python module used for testing
import tabnanny
import os
import socket

from .. import _sPickle as pickler
from . import wf_module 


# Various test objects

def aFunction():
    """A plain global function """
    return True

def functionWithArg(arg):
    """A function that takes an argument"""
    return bool(arg)

decoratedFunction = functools.partial(functionWithArg, 4711)
functools.update_wrapper(decoratedFunction, functionWithArg)
del functionWithArg

class PlainClass(object):
    def __init__(self, state = None):
        self.state = state 
    
    def isOk(self):
        return self.state == "OK"
    
class PlainClassicClass:
    def __init__(self, state = None):
        self.state = state 
    
    def isOk(self):
        return self.state == "OK"
    
        
def ClassWithDictCreatingReduce_reducer(cls, dict):
    obj = cls()
    obj.__dict__ = dict
    return obj
    
class ClassWithDictCreatingReduce(object):
    
    def __reduce_ex__(self, proto):
        return (ClassWithDictCreatingReduce_reducer, (self.__class__, self.__dict__))
    
    def __init__(self, state=None):
        self.state = state
            
    def isOk(self):
        return self.state == "OK"
        
class P:
    
    # this class is not in the module namespace and 
    # therefore the pickler has to serialize the class
    class RecursiveClass(object):
        selfCls = None
        count = 0
        def __init__(self):
            self.count = self.count + 1 
        
        def run(self, test):
            test.assertTrue(self.selfCls is self.__class__)
            test.assertEqual(self.count, 1)
            return True
    RecursiveClass.selfCls = RecursiveClass


def buildModule(name):
    """Builder for anonymous modules"""
    m = imp.new_module(name)
    # function from another module
    m.foreignFunction = aFunction
    # function of the anonymous module
    exec """
def isOk():
    return foreignFunction()
""" in m.__dict__
    return m

anonymousModule = buildModule("anonymousModule")
anonymousWfModule = buildModule("anonymousWfModule")
anonymousWfModule.__wf_module__ = True

class StrangeModuleType(types.ModuleType):
    """Modules of this type have an 'isOk()' function, that is 
    not referenced in the modules dictionary. Therefore this function does not
    get pickled together with its module."""
    
    syntheticFunctions = {}
    
    """A special module type"""
    def __getattr__(self, name):
        if name == "isOk":
            f = self.syntheticFunctions.get(id(self))
            if f is None:
                # create the function, but avoid a reference to the 
                # modules namespace. Just copy the __module__ attribute,
                # to make the synthetic function part of self
                f = eval("lambda : True", {'__name__': self.__module__})
                f.func_name = name
                self.syntheticFunctions[id(self)] = f
            return f
        raise AttributeError(name)
    __wf_module__ = True

strangeModule = StrangeModuleType("strangeModule")
sys.modules[strangeModule.__name__] = strangeModule
                
        
class TestImportFunctor(object):
    def __init__(self, *modulesToUnimport):
        self.names = []
        self.modulesToUnimport = modulesToUnimport
        self.unimported = set()
            
    def __call__(self, name, *args, **kw):
        self.names.append(name)
        return self.saved_import(name, *args, **kw)
    
    def __enter__(self):
        import __builtin__
        self.saved_import = __builtin__.__import__
        __builtin__.__import__ = self
        self.saved_modules = sys.modules.copy()
        assert self.saved_modules == sys.modules
        
        for m in self.modulesToUnimport:
            if isinstance(m, types.ModuleType):
                m = m.__name__
            if sys.modules.has_key(m):
                self.unimported.add(sys.modules[m])
                del sys.modules[m]
                
        self.pre_modules = sys.modules.copy()
        assert self.pre_modules == sys.modules
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        import __builtin__
        __builtin__.__import__ = self.saved_import
        self.post_modules = sys.modules.copy()
        assert self.post_modules == sys.modules
        sys.modules.clear()
        sys.modules.update(self.saved_modules)
        
        return False
        

        
class PickelingTest(TestCase):
    """
    This is the description of the class WfBaseTest. 
    
    Here comes the description of the class. The sole purpose
    of this class is to act as an example for documenting a class.
    """
    def run(self, result=None):
        self.dis = getattr(result, "showAll", False)
        self.dis = False
        return TestCase.run(self, result)
    
    def setUp(self):
        self.pickler = pickler.WfPickler()
        
    def tearDown(self):
        pass

    def dumpWithPreobjects(self, preObjects, *obj, **kw):
        """Dump one or more objects.
        
        Returns the dump of the 2-tuple (preObjects, obj[0] if len(obj) == 1 else obj )
        Writes the opcodes to sys.stdout, if kw['dis'] evaluates to True.
        Writes the opcodes to sys.stderr, if an error occurs.
    
        """
        
        dis = kw.get("dis")
        try:
            toBeDumped = (preObjects, obj[0] if len(obj) == 1 else obj )
            p = self.pickler.dumps(toBeDumped)
            self.pickler.dis(p, out=StringIO())
        except:
            exinfo = sys.exc_info()
            l = []
            try:
                pickler.FgPickler(l, 2).dump(toBeDumped)
            except:
                try:
                    l.append(pickle.STOP)
                    pickletools.dis("".join(l), out=sys.stderr)
                except:
                    traceback.print_exc(limit=1, file=sys.stderr)
            raise exinfo[0], exinfo[1], exinfo[2]
            
        if dis is None:
            dis = self.dis
        if dis:
            self.pickler.dis(p)
            print "len(pickle): ", len(p)
        return p
        
    # Test the object graph isomorphism for objects and their 
    # __dict__ attributes. (These tests fail with pickle.Pickler). 

    # using objects of a plain normal class
    def testObjDictConsistency(self):
        self.objDictConsistency(PlainClass, False, False)
    def testObjDictConsistencyHard(self):
        self.objDictConsistency(PlainClass, True)

    # using objects of a classic class
    def testObjDictConsistencyClassic(self):
        self.objDictConsistency(PlainClassicClass, False, False)
    def testObjDictConsistencyClassicHard(self):
        self.objDictConsistency(PlainClassicClass, True)

    # using objects of a class, that uses a
    # reducer, that uses a precreated __dict__ object
    def testObjDictConsistencyDCR(self):
        self.objDictConsistency(ClassWithDictCreatingReduce, False, False)
    def testObjDictConsistencyDCRHard(self):
        self.objDictConsistency(ClassWithDictCreatingReduce, True, False)

    def objDictConsistency(self, cls, hard, dis=False):
        """Is the relation between an object and its dictionary preserved"""
        orig = cls("OK")
        # the hard way 
        if hard:
            p = self.dumpWithPreobjects(None, orig.__dict__, orig, dis=dis)
            d, obj = self.pickler.loads(p)[-1]
        else:
            p = self.dumpWithPreobjects(None, orig, orig.__dict__, dis=dis)
            obj, d = self.pickler.loads(p)[-1]            
        self.assertTrue(type(obj)is type(orig))
        self.assertTrue(type(obj.__dict__) is type(orig.__dict__))
        self.assertEquals(set(obj.__dict__.keys()), set(orig.__dict__.keys()))
        self.assertTrue(obj.__dict__ is d)
        self.assertTrue(obj.isOk() is True)
    
    
    
    # Tests for modules
    #
    # - Contains the pickle the module content or just an import instruction?
    # - How is sys.modules changed on unpickling?
    # - Is the content of the modules equal?
    
    def testModule(self):
        orig = tabnanny
        self.assertTrue(orig.__name__ in sys.modules)    
        obj, tif = self.moduleTest(orig)
        self.assertTrue(obj is tif.post_modules[orig.__name__])
    def testModuleU(self):
        orig = tabnanny
        self.assertTrue(orig.__name__ in sys.modules)
        obj, tif = self.moduleTest(orig, unimport=True, dis=False)
        self.assertTrue(obj is tif.post_modules[orig.__name__])
        self.assertTrue(orig.__name__ in tif.names, "name %r not in %r" % (orig.__name__, tif.names))
        
    def testWfModule(self):
        orig = wf_module
        self.assertTrue(orig.__name__ in sys.modules)
        obj, tif = self.wfModuleTest(orig)
        self.assertFalse(obj is tif.post_modules[orig.__name__])

    def testWfModuleU(self):
        orig = wf_module
        self.assertTrue(orig.__name__ in sys.modules)
        obj, tif = self.wfModuleTest(orig, unimport=True)
        self.assertEqual(obj, tif.post_modules.get(orig.__name__))
        self.assertTrue(obj is tif.post_modules[orig.__name__])

    def testStrangeWfModuleTest(self):
        orig = strangeModule
        self.assertTrue(orig.__name__ in sys.modules)
        obj, tif = self.wfModuleTest(orig)
        self.assertFalse(obj is tif.post_modules[orig.__name__])

    def testStrangeWfModuleTestU(self):
        orig = strangeModule
        self.assertTrue(orig.__name__ in sys.modules)
        obj, tif = self.wfModuleTest(orig, unimport=True)
        self.assertEqual(obj, tif.post_modules.get(orig.__name__))
        self.assertTrue(obj is tif.post_modules[orig.__name__])

    def testAnonymousModule(self):
        self.assertFalse(anonymousModule.__name__ in sys.modules)
        obj, tif = self.moduleTest(anonymousModule)
        self.assertFalse(obj.__name__ in tif.post_modules)
    def testAnonymousModule2(self):
        self.assertFalse(anonymousModule.__name__ in sys.modules)
        obj, tif = self.moduleTest(anonymousModule, anonymousModule.__dict__)
        self.assertFalse(obj.__name__ in tif.post_modules)

    def testAnonymousWfModule(self):
        self.assertFalse(anonymousWfModule.__name__ in sys.modules)
        obj, tif = self.wfModuleTest(anonymousWfModule)
        self.assertFalse(obj.__name__ in tif.post_modules)
    def testAnonymousWfModule2(self):
        self.assertFalse(anonymousWfModule.__name__ in sys.modules)
        obj, tif = self.wfModuleTest(anonymousWfModule, anonymousWfModule.__dict__)
        self.assertFalse(obj.__name__ in tif.post_modules)


    def moduleTest(self, module, preObjects=None, **kw):
        orig = module
        obj, tif = self._moduleTestCommon(orig, preObjects, **kw)
        if tif.pre_modules.has_key(orig.__name__):
            self.assertEquals(obj, orig)
            self.assertTrue(obj is orig)
            self.assertTrue(orig.__name__ in tif.names, "name %r not in %r" % (orig.__name__, tif.names))
        return (obj, tif)

    def wfModuleTest(self, wf_module, preObjects=None, **kw):
        orig = wf_module
        obj, tif = self._moduleTestCommon(orig, preObjects, **kw)
        # the object must be a real clone and must not be imported
        self.assertFalse(obj is orig)
        self.assertFalse(orig.__name__ in tif.names, "Import of forbidden module %r" % (orig.__name__,))
        return (obj, tif)
        
    def _moduleTestCommon(self, module, preObjects=None, unimport=(), dis=False):
        orig = module
        p = self.dumpWithPreobjects(preObjects, orig, dis=dis)
        
        if unimport:
            if unimport is True:
                unimport = (orig,)
        
        with TestImportFunctor(*unimport) as tif:    
            obj = self.pickler.loads(p)[-1]

        # test obj        
        self.assertEquals(type(obj), type(orig))
        self.assertTrue(type(obj) is type(orig))
        self.assertEquals(obj.__name__, orig.__name__)
        self.assertEqual(set(obj.__dict__.iterkeys()), set(orig.__dict__.iterkeys()))
        if callable(getattr(orig, "isOk", None)):
            self.assertTrue(obj.isOk() is True)
        
        if tif.pre_modules.has_key(orig.__name__):
            # the import of the module must not change anything 
            self.assertEqual(tif.pre_modules, tif.post_modules)
            
        return (obj, tif)


    
    # Tests for module dictionaries
    #
    
    def testModuleDict(self):
        orig = tabnanny
        self.moduleDictTest(orig)        
    def testWfModuleDict(self):
        self.wfModuleDictTest(wf_module)
            

    def testAnonymousModuleDict(self):
        self.assertFalse(anonymousModule.__name__ in sys.modules)
        self.moduleDictTest(anonymousModule)
    def testAnonymousModuleDict2(self):
        self.assertFalse(anonymousModule.__name__ in sys.modules)
        self.moduleDictTest(anonymousModule, anonymousModule)
        

    def testAnonymousWfModuleDict(self):
        self.assertFalse(anonymousWfModule.__name__ in sys.modules)
        self.wfModuleDictTest(anonymousWfModule)
    def testAnonymousWfModuleDict2(self):
        self.assertFalse(anonymousWfModule.__name__ in sys.modules)
        self.wfModuleDictTest(anonymousWfModule, anonymousWfModule)
        
    def moduleDictTest(self, module, preObjects=None, dis=False):
        g = module.__dict__
        p = self.dumpWithPreobjects(preObjects, g, module)
        obj, mod = self.pickler.loads(p)[-1]
        self.assertTrue(obj is g)

    def wfModuleDictTest(self, wf_module, preObjects = None, dis=False):
        orig = getattr(wf_module, "__dict__")
        p = self.dumpWithPreobjects(preObjects, orig, wf_module)
        obj, mod = self.pickler.loads(p)[-1]
        self.assertTrue(type(obj) is type(orig))
        self.assertFalse(obj is orig)
        self.assertEqual(set(obj.keys()), set(orig.keys()))
        self.assertTrue(type(wf_module) is type(mod))
        self.assertTrue(mod.__dict__ is obj)

    # Tests for function and code objects
    
    def testWfFunction(self):
        self.wfFunctionTest(wf_module.isOk)
        self.assertTrue(wf_module is sys.modules[wf_module.__name__])
        
    def testStrangeWfModuleFunctionTest(self):
        f = strangeModule.isOk
        self.assertTrue(f())
        self.wfFunctionTest(f, strangeModule, dis=False)
        self.wfFunctionTest(f, dis=False)

    @skipIf(sys.hexversion < 0x02070000, "requires python 2.7")
    def testDecoratedFunctionTest(self):
        f = decoratedFunction
        self.assertTrue(f())
        obj = self.wfFunctionTest(f, dis=False)
        self.assertTrue(obj())
    
    def wfFunctionTest(self, function, preObjects=None, dis=False):
        orig = function
        p = self.dumpWithPreobjects(preObjects, orig, dis=dis)
        obj = self.pickler.loads(p)[-1]
        self.assertTrue(type(obj) is type(orig))
        self.assertFalse(obj is orig)
        self.assertEqual(obj.__name__, orig.__name__)
        self.assertEqual(obj.__doc__, orig.__doc__)
        if hasattr(orig, "func_globals"):
            self.assertTrue(type(obj.func_globals) is type(orig.func_globals))
        if hasattr(orig, "func_code"):
            self.assertFalse(obj.func_code is orig.func_code)
            self.assertEquals(obj.func_code, orig.func_code)
        # Todo: compare the other attributes of a function
        return obj


    # Tests for special objects and types

    def testTypeType(self):
        p = self.pickler.dumps(type)
        obj = self.pickler.loads(p)
        self.assertTrue(obj is type)

    def testTypeClass(self):
        p = self.pickler.dumps(types.ClassType)
        obj = self.pickler.loads(p)
        self.assertTrue(obj is types.ClassType)

    def testThreadLock(self):
        lock = thread.allocate_lock()
        self.assertIsInstance(lock, thread.LockType)
        p = self.pickler.dumps(lock)
        obj = self.pickler.loads(p)
        self.assertIsInstance(obj, thread.LockType)
        self.assertIsNot(obj,lock)
        self.assertFalse(obj.locked())
        
        self.assertTrue(lock.acquire(0))
        p = self.pickler.dumps(lock)
        obj = self.pickler.loads(p)
        self.assertIsInstance(obj, thread.LockType)
        self.assertTrue(obj.locked())
        

    # Tests for pickling classes
    
    def testClassicClass(self):
        class ClassicClass:
            anAttribute = None
        self.classCopyTest(ClassicClass)
    
    def testNewStyleClass(self):
        class NewStyleClass(object):
            anAttribute = None
        self.classCopyTest(NewStyleClass)

    def testRecursiveClass(self):
        cls = self.classCopyTest(P.RecursiveClass)
        cls().run(self)
        
    def testNormalClass(self):
        cls = self.classCopyTest(pickle.Pickler)
        self.assertTrue(cls is pickle.Pickler)
        
    def classCopyTest(self, origCls, dis=False):
        p = self.pickler.dumps(origCls)
        cls = self.pickler.loads(p)
        self.assertTrue(type(cls) is type(origCls))
        self.assertEqual(cls.__name__, origCls.__name__)
        self.assertEqual(cls.__module__, origCls.__module__)
        self.assertEqual(cls.__bases__, origCls.__bases__)
        self.assertEqual(set(dir(cls)), set(dir(origCls)))
        for k in dir(cls):
            if k in ('__dict__', '__subclasshook__', '__weakref__'):
                continue
            v1 = getattr(cls, k)
            v2 = getattr(origCls, k)
            self.assertEqual(type(v1), type(v2), "key: %r type: %r != %r" % (k, v1, v2))
            self.assertEqual(repr(v1), repr(v2), "key: %r repr: %r != %r" % (k, v1, v2))
        return cls
        
    # Import tests
        
    def testWfImports(self):
        orig = wf_module
        p = self.pickler.dumps(orig)
        importList = self.pickler.getImportList(p)
        for module in importList:
            print module
        obj = self.pickler.loads(p)

    # Handling of resource objects (files, socket, socketpair)

    def testResources(self):
        openFile = open(os.devnull)
        closedFile = open(os.devnull, "wb")
        closedFile.close()
        socket_ = socket.socket()
        if hasattr(socket, "socketpair"):
            sp = socket.socketpair()
            sp[1].close()
        else:
            sp = [socket.socket()]
            sp.append(sp[0]._sock)
            
        try:
            orig = (openFile, closedFile, sys.__stdout__, socket_, sp )
            p = self.pickler.dumps(orig)
            restored = self.pickler.loads(p)
            
            self.assertIsInstance(restored, tuple)
            self.assertEqual(len(orig), len(restored))
            for i in range(len(orig)-1):
                self.assertIs(type(orig[i]), type(restored[i]))
            self.assertIsInstance(restored[-1][1], pickler.SOCKET_PAIR_TYPE)
            self.assertIs(orig[2], restored[2])
            
            self.assertEqual(orig[0].mode, restored[0].mode)
            self.assertFalse(restored[0].closed)
            restored[0].close()
            self.assertEqual(orig[1].mode, restored[1].mode)
            self.assertTrue(restored[1].closed)
        finally:
            openFile.close()
            socket_.close()
            sp[0].close()
        
        

if __name__ == "__main__":
    import unittest
    unittest.main()
