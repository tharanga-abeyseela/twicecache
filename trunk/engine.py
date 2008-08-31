"""

    File: engine.py
    Description: 
    
        Rendering engine.

    Author: Kyle Vogt
    Copyright (c) 2008, Justin.tv, Inc.
    
"""

from twisted.internet import reactor, protocol, defer
from twisted.python import log
import traceback, urllib, time
import cache, http

class DataStore:
    
    # Element types to fetch on every request
    prefetch_types = ['session']
    # Element types that depend on results of a prefetch element
    prefetch_dependent_elements = []
    
    # Status codes
    uncacheable_status = [500, 502, 503, 504, 307]
    short_status = [404, 304]
    
    def __init__(self, config):
        self.config = config   

        # Mecache Backend
        from twisted.protocols.memcache import MemCacheProtocol, DEFAULT_PORT
        try:
            server, port = config.get('backend_memcache').split(':')
        except:
            server = config.get('backend_memcache')
            port = DEFAULT_PORT
        d = protocol.ClientCreator(reactor, MemCacheProtocol).connectTCP(server, int(port))
        d.addCallback(self.memcacheConnected)
        
        # Database Backend
        from twisted.enterprise import adbapi
        log.msg('Conneting to db...')
        try:
            self.db = adbapi.ConnectionPool("pyPgSQL.PgSQL", 
                database=config.get('backend_db_name', ''), 
                host=config.get('backend_db_host', '127.0.0.1'), 
                user=config.get('backend_db_user', ''), 
                password=config.get('backend_db_pass', ''),
                cp_noisy=True,
                cp_reconnect=True,
                cp_min=config.get('backend_db_pool_min', 1),
                cp_max=config.get('backend_db_pool_max', 1),
            )
            log.msg("Connected to db.")
        except ImportError:
            log.msg("Could not import database library")
            traceback.print_exc()
        except:
            log.msg("Unable to connect to database")
            traceback.print_exc()
            
         # HTTP Backend
        try:
            self.backend_host, self.backend_port = self.config['backend_appserver'].split(':')
            self.backend_port = int(self.backend_port)
        except:
            self.backend_host = self.config['backend_appserver']
            self.backend_port = 80
            
        # Cache Backend
        log.msg('Initializing cache...')
        cache_type = config['cache_type'].capitalize() + 'Cache'
        self.cache = getattr(cache, cache_type)(config)     
        
        # Memorize variants of a uri
        self.uri_lookup = {}
        
    # Init status

    def memcacheConnected(self, proto):
        log.msg('Memcache connection success.')
        self.proto = proto

    def dbConnected(self, db):
        log.msg('Database connection success.')
        self.db = db

    def viewdbConnected(self, viewdb):
        log.msg("Viewdb connection success.")
        self.viewdb = viewdb

    def get(self, keys, request):
        "Get, cache, and return elements"
        d = defer.maybeDeferred(self.cache.get, keys)
        d.addCallback(self.handleMisses, request)
        d.addErrback(self.getError)
        return d
        
    def delete(self, keys):
        "Delete elements from cache"
        if not isinstance(keys, list): keys = [keys]
        self.cache.delete(keys)
        
    def flush(self):
        "Flush entire cache"
        self.cache.flush()

    def handleMisses(self, dictionary, request):
        "Process hits, check for validity, and fetch misses or invalids"
        missing_deferreds = []
        missing_elements = []
        for key, value in dictionary.items():
            if value is None:
                log.msg('MISS [%s]' % key)
                d = getattr(self, 'fetch_' + self.elementType(key))(request, self.elementId(key))
                missing_deferreds.append(d)
                missing_elements.append(key)
            elif not getattr(self, 'valid_' + self.elementType(key))(request, self.elementId(key), value):
                log.msg('INVALID [%s]' % key)
                d = getattr(self, 'fetch_' + self.elementType(key))(request, self.elementId(key))
                missing_deferreds.append(d)
                missing_elements.append(key)
            else:
                log.msg('HIT [%s]' % key)
        # Wait for all items to be fetched
        if missing_deferreds:
            deferredList = defer.DeferredList(missing_deferreds)
            deferredList.addCallback(self.returnElements, dictionary, missing_elements)
            return deferredList
        else:
            return defer.succeed(dictionary)
        
    def returnElements(self, results, dictionary, missing_elements):
        if not isinstance(results, list): results = [results]
        uncached_elements = dict([(key, results.pop(0)[1]) for key in missing_elements])
        dictionary.update(uncached_elements)
        return dictionary

    def getError(self, dictionary):
        log.msg('uh oh! %s' % dictionary)
        traceback.print_exc()
        
    # Hashing
            
    def elementHash(self, request, element_type, element_id = None):
        "Hash function for elements"
        return getattr(self, 'hash_' + element_type.lower())(request, element_id)
        
    def elementType(self, key):
        return key.split('_')[0]
        
    def elementId(self, key):
        return '_'.join(key.split('_')[1:])
        
    # Page
    
    def hash_page(self, request, id=None, cookies = []):
        # Hash the request key
        key = 'page_' + (request.getHeader('x-real-host') or request.getHeader('host')) + request.uri
        # Internationalization salt
        if self.config.get('hash_lang_header'):
            header = request.getHeader('accept-language') or self.config.get('hash_lang_default', 'en-us')
            if header:
                try:
                    lang = header.replace(' ', '').split(';')[0].split(',')[0].lower()
                    key += '//' + lang
                except:
                    traceback.print_exc()
        if cookies:
            # Grab the cookies we care about from the request
            found_cookies = []
            for cookie in cookies:
                val = request.getCookie(cookie)
                if val:
                    found_cookies.append('%s=%s' % (cookie, val))
            # Update key based on cookies we care about
            if found_cookies:
                key += '//' + ','.join(found_cookies)
        return key
    
    def fetch_page(self, request, id):
        # Tell backend that we are Twice and strip cache-control headers
        request.setHeader(self.config.get('twice_header'), 'true')
        request.removeHeader('cache-control')
        # Make the request
        sender = http.HTTPRequestSender(request)
        sender.noisy = False
        reactor.connectTCP(self.backend_host, self.backend_port, sender)
        return sender.deferred.addCallback(self.extract_page, request).addErrback(self.page_failed, request)
        
    def valid_page(self, request, id, value):
        "Determine whether the page can be served stale"
        now = time.time()
        # Force refetch of very stale (3x cache_control value) pages
        if now > value['expires_on'] + value['cache_control'] * 3:
            log.msg('STALE-HARD [%s]' % id)
            return False
        # Sever semi-stale pages but refresh in the background
        elif now > value['expires_on']:
            log.msg('STALE-SOFT [%s]' % id)
            self.fetch_page(request, id)
            return True
        # Valid page
        else:
            return True
        
    def page_failed(self, response, request):
        log.msg('ERROR: Could not retrieve [%s]' % request.uri)
        response.printBriefTraceback()
        # TODO: Return something meaningful!
        return ''
        
    def extract_page(self, response, request):

        # Extract uniqueness info
        cookies = sorted((response.getHeader(self.config.get('cookies_header')) or '').split(','))
        key = self.hash_page(request, cookies = cookies)

        # Store uri variant
        if key not in self.uri_lookup.setdefault(request.uri, []):
            log.msg('Added new varient for %s: %s' % (request.uri, key))
            self.uri_lookup[request.uri].append(key)

        # Override for non GET's
        if request.method.upper() not in ['GET']:
            log.msg('NO-CACHE (Method is %s) [%s]' % (request.method, key))
            cache = False
            cache_control = 0
        else:    
            # Cache logic
            cache_control = response.getCacheControlHeader(self.config.get('cache_header')) or 0
            if response.status in self.uncacheable_status:
                log.msg('NO-CACHE (Status is %s) [%s]' % (response.status, key))
                cache = False
            elif response.status in self.short_status:
                log.msg('SHORT-CACHE (Status is %s) [%s]' % (response.status, key))
                cache = True
                cache_control = 30
            elif cache_control and cache_control > 0:
                log.msg('CACHE [%s] (for %ss)' % (key, cache_control))
                cache = True
            else:
                log.msg('NO-CACHE (No cache data) [%s]' % key)
                cache = False

        # Actual return value  
        value =  {
            'dependencies' : [],
            'response' : response,
            'expires_on' : time.time() + cache_control,
            'cache_control' : cache_control
        }
        if cache:
            response.cookies = []
            self.cache.set({key : value}, cache_control + 86400) # Keep pages for up to 24 hours
        return value
        
    # Memcache
    
    def hash_memcache(self, request, id):
        return 'memcache_' + id
    
    def fetch_memcache(self, request, id):
        log.msg('Looking up memcache %s' % id)
        return self.proto.get(id).addCallback(self.extract_memcache, request, id)  
        
    def extract_memcache(self, result, request, id):
        value = result and result[1]
        self.cache.set({self.hash_memcache(request, id) : value}, 30) # 30 seconds
        return value
        
    def valid_memcache(self, request, id, value):
        return True
                
    def incr_memcache(self, key):
        log.msg('Incrementing memcache %s' % key)
        return self.proto.increment(key)
        
    def set_memcache(self, key, val):
        log.msg('Setting memcache %s' % key)
        return self.proto.set(key, val)
                
    # Session    
    
    def hash_session(self, request, id):
        id = self._read_session(request)
        if id:
            return 'session_' + id
        else:
            return ''
      
    def fetch_session(self, request, id):
        id = self._read_session(request)
        return self.db.runInteraction(self._session, id).addCallback(self.extract_session, request, id)
    
    def extract_session(self, result, request, id):
        if len(result[0]):
            output = result[0][0]
        else:
            output = {}
        self.cache.set({self.hash_session(request, id) : output}, 86400) # 24 hours
        return output
    
    def valid_session(self, request, id, value):
        return True
    
    def _read_session(self, request):
        "Extract session id from the HTTP request"
        return urllib.unquote(request.getCookie('session_cookie') or '')

    def _session(self, txn, id):
        log.msg('Looking up session %s' % id)
        users_query = "select * from users where session_cookie = '%s'" % id
        log.msg('Running query: %s' % users_query)
        txn.execute(users_query)
        return txn.fetchall()
