"""

    File: cache_internal.py
    Description: 
    
        This module uses a Python dictionary to implement a simple in-memory Twice cache.

    Author: Kyle Vogt
    Copyright (c) 2008, Justin.tv, Inc.
    
"""

from twisted.python import log
from twisted.protocols.memcache import MemCacheProtocol
from twisted.internet import protocol, reactor
import random, time

class TwiceCache:
    """ Base class for implementing a Twice Cache"""
    
    def __init__(self, config):
        self.config = config
        
    def ready(self):
        "Call when the cache is online"
        pass
        
    def set(self, dictionary, time = None):
        "Store value(s) supplied as a python dict for a certain time"
        pass
        
    def get(self, keylist):
        "Retreive a list of values as a python dict"
        return {}
        
    def flush(self):
        "Delete all keys"
        pass
        
class InternalCache(TwiceCache):
    "Implements a Twice Cache using a Python dictionary"
    
    def __init__(self, config):
        Cache.__init__(self, config)
        self.cache = {}
        self.ready()
        
    def ready(self):
        limit = self.config.get('memory_limit')
        if not limit:
            log.msg('WARNING: memory_limit not specified, using 100MB as default')
            self.config['memory_limit'] = 100000
        log.msg("CACHE_BACKEND: Using %s MB in-memory cache" % limit)
        
    def set(self, dictionary, time = None):
        for key, val in dict(dictionary.items()):
            element = {
                'expires_on' : time.time() + (time or 0),
                'element' : val
            }
            dictionary[key] = element
        self.cache.update(element)
            
    def get(self, keylist):
        if not isinstance(keylist, list): keylist = [keylist]
        output = {}
        for key in keylist:
            element = self.cache.get(key)
            if element:
                if time.time() > element['expires_on']:
                    output[key] = None
                else:
                    output[key] = element['element']
        return output
        
    def delete(self, keylist):
        for key in keylist:
            try:
                del self.cache[key]
            except:
                pass
        
    def flush(self):
        self.cache = {}
        
class MemcacheCache(TwiceCache):
    "Implements a Twice Cache using a memcache server"

    def __init__(self, config):
        Cache.__init__(self, config)
        server = config['cache_server']
        connection_pool_size = int(config.get('cache_pool', 1))
        log.msg('Creating memcache connection pool to server %s...' % server)
        self.pool = []
        # Import pickling library
        try:
            import cPickle as pickle
        except ImportError:
            log.msg('cPickle not available, using slower pickle library.')
            import pickle
        # Parse server string
        try:
            self.host, self.port = server.split(':')
        except:
            self.host = server
            self.port = 11211
        # Make connections
        defers = []
        for i in xrange(connection_pool_size):
            d = protocol.ClientCreator(reactor, MemCacheProtocol).connectTCP(self.host, int(self.port))
            d.addCallback(self.add_connection)
            defers.append(d)
        defer.DeferredList(defers).addCallback(self.ready)        
        
    def add_connection(self, result=None):
        log.msg('CACHE_BACKEND: Connected to memcache server at %s:%s' % (self.host, self.port))
        self.pool.append(result)

    def ready(self, result=None):
        log.msg('CACHE_BACKEND: Memcache pool complete')
        
    def cache_pool(self):
        "Random load balancing across connection pool"
        return random.choice(self.pool)

    def set(self, dictionary, time = None):
        pickled_dict = dict([(key, pickle.dumps(val)) for key, val in dictionary.items() if val is not None])
        connection = self.cache_pool()
        #log.msg('SET on cache %s' % cache)
        if len(pickled_dict):
            return connection.set_multi(pickled_dict, expireTime = time)
        else:
            return {}

    def get(self, keylist):
        if not isinstance(keylist, list): keylist = [keylist]
        #log.msg('keylist: %s' % keylist)
        connection = self.cache_pool()
        #log.msg('GET on cache %s' % cache)
        return connection.get_multi(keylist).addCallback(self._format, keylist)
        
    def delete(self, keylist):
        for key in keylist:
            self.cache_pool().delete(key)
        
    def _format(self, results, keylist):
        "Return a dictionary containing all keys in keylist, with cache misses as None"
        output = dict([(key, results[1].get(key, None) and pickle.loads(results[1][key])) for key in keylist])
        #log.msg('Memcache results:\n%s' % repr(output))
        return output
        
    def flush(self):
        self.cache_pool().flushAll()
        