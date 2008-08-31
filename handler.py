"""

    File: handler.py
    Description: 
    
        Main HTTP request handler.

    Author: Kyle Vogt
    Copyright (c) 2008, Justin.tv, Inc.
    
"""

from twisted.internet import reactor, defer
from twisted.python import log
import sys, urllib, time, re, traceback
import parser, engine, http, cache

class RequestHandler(http.HTTPRequestDispatcher):
    
    def __init__(self, config):

        # Caches and config
        self.config = config
        
        # Template format
        self.specialization_re = re.compile(self.config['template_regex'])
        
        # Data Store
        log.msg('Initializing data store...')
        self.store = storage.DataStore(config)
                            
    def objectReceived(self, connection, request):
        "Main request handler"
        # Handle mark dirty requests
        if request.getHeader(self.config.get('purge_header')) is not None:
            self.markDirty(connection, request)
            return
        # Handle time requests
        if 'live/time' in request.uri:
            connection.sendCode(200, str(time.time()))
            return
        # Check cache
        else:
            # Overwrite host field
            real_host = self.config.get('rewrite_host', request.getHeader('x-real-host'))
            if real_host:
                request.setHeader('host', real_host)
            
            # Add in prefetch keys
            keys = [self.store.elementHash(request, 'page')]
            session_key = self.store.elementHash(request, 'session')
            if session_key:
                keys.append(session_key)

            # Retrieve keys
            log.msg('PREFETCH: %s' % keys)
            self.store.get(keys, request).addCallback(self.checkPage, connection, request)
                        
# ---------- CACHE EXPIRATION -----------
            
    def markDirty(self, connection, request):
        "Mark a uri as dirty"
        uri = request.uri
        try:
            kind = request.getHeader(self.config.get('purge_header')).lower()
        except:
            log.msg('Could not read expiration type: %s' % repr(request.getHeader(self.config.get('purge_header'))))
            return
        log.msg("Expire type: %s, arg: %s" % (kind, uri))
        # Parse request
        if kind == '*':
            self.store.flush()
            log.msg('Cleared entire cache')
        elif kind == 'url':
            try:
                keys = self.store.uri_lookup[uri]
                self.store.delete(keys)
                del self.store.uri_lookup[uri]
                log.msg('Deleted all variants of %s' % uri)
            except:
                pass
        elif kind == 'session':
            try:
                types = ['favorite', 'subscription', 'session']
                keys = ['%s_%s' % (t, uri[1:]) for t in types]
                self.store.delete(keys)
                log.msg('Deleted session-related keys: %s' % keys)
            except:
                pass
        else:
            try:
                key = kind + '_' + uri[1:]
                self.store.delete(key)
                log.msg('Deleted %s_%s' % (kind, uri[1:]))
            except:
                pass
        # Write response
        connection.sendCode(200, "Expired %s_%s" % (kind, uri))
        return True       
                
# ---------- CLIENT RESPONSE -----------  

    def checkPage(self, elements, connection, request, extra = {}):
        "See if we have the correct version of the page"        
        # Process cookies
        response = [val for key, val in elements.items() if key.startswith('page_')][0]['response']
        cookies = sorted((response.getHeader(self.config.get('cookies_header')) or '').split(','))
        key = self.store.hash_page(request, cookies = cookies)
            
        # If the page we fetched doesn't have the right cookies, try again!
        if key != self.store.hash_page(request):
            self.store.get(key, request).addCallback(self.scanPage, connection, request, extra = elements)
        else:
            self.scanPage(elements, connection, request)

    def scanPage(self, elements, connection, request, extra = {}):
        "Scan for missing elements"
        elements.update(extra)
        logged_in = [True for key, value in elements.items() if key.startswith('session_') and value is not None]
        data = [val for key, val in elements.items() if key.startswith('page_')][0]['response'].body
        matches = self.specialization_re.findall(data)
        missing_keys = []
        for match in matches:
            # Parse element
            try:
                parts = match.strip().split()
                command = parts[0].lower()
                element_type = parts[1].lower()
                element_id = parts[2]
            except:
                traceback.print_exc()
                continue
            if element_type not in ['page', 'session']:
                if element_type in ['memcache', 'viewdb'] or logged_in:
                    key = self.store.elementHash(request, element_type, element_id)
                    if key and key not in missing_keys:
                        missing_keys.append(key)
        if missing_keys:
            d = self.store.get(missing_keys, request)
            d.addCallback(self.renderPage, connection, request, elements)
        else:
            self.renderPage({}, connection, request, elements)

    def renderPage(self, new_elements, connection, request, elements):
        "Write the page out to the request's connection"
        elements.update(new_elements)
        for etype in ['page', 'session', 'favorite', 'subscription']:
            eitems = [val for key, val in elements.items() if key.startswith(etype)]
            if eitems:
                eitems = eitems[0]
            else:
                eitems = {}
            setattr(self, 'current_' + etype, eitems)
            #log.msg('Current %s: %s' % (etype, eitems))

        for etype in ['memcache', 'viewdb']:
            #log.msg('elements: %s' % elements.items())
            eitems = dict([(self.store.elementId(key), val) for key, val in elements.items() if key.startswith(etype)])
            setattr(self, 'current_' + etype, eitems)
            #log.msg('Current %s: %s' % (etype, eitems))

        response = self.current_page['response']
        # Do Templating
        data = self.specialization_re.sub(self.specialize, response.body)
        # Remove current stuff
        for etype in ['session', 'favorite', 'subscription']:
            setattr(self, 'current_' + etype, {})
        # Overwrite headers
        response.setHeader('connection', 'close')
        response.setHeader('content-length', len(data))
        response.setHeader('via', 'Twice 0.1')
        # Delete twice/cache headers
        response.removeHeader(self.config.get('cache_header'))
        response.removeHeader(self.config.get('twice_header'))
        response.removeHeader(self.config.get('cookies_header'))
        # Write response
        connection.transport.write(response.writeResponse(body = data))
        connection.shutdown()
        log.msg('RENDER [%s] (%.3fs after request received)' % (request.uri, (time.time() - request.received_on)))

# ---------- TEMPLATING -----------

    def specialize(self, expression):
        "Parse an expression and return the result"
        try:
            expression = expression.groups()[0].strip()
            parts = expression.split()
            # Syntax is: command target arg1 arg2 argn
            #   command - one of 'get', 'if', 'unless', 'incr', 'decr'
            #   target - one of 'memcache', 'session'
            #   arg[n] - usually the name of a key
            command, target, args = parts[0].lower(), parts[1], parts[2:]
            #log.msg('command: %s target: %s args: %s' % (command, target, repr(args)))
        except:
            log.msg('Could not parse expression: [%s]' % expression)
            return expression
        # Grab dictionary
        try:
            dictionary = getattr(self, 'current_' + target)
        except:
            dictionary = {}
        #log.msg('dictionary: %s' % dictionary)
        # Handle commands
        if command == 'get' and len(args) >= 1:
            if len(args) >= 2:
                default = args[1]
            else:
                default = ''
            val = dictionary.get(args[0])
            if not val:
                val = default
            #log.msg('arg: %s val: %s (default %s)' % (args[0], val, default))
            return str(val)
        elif command == 'if' and len(args) >= 2:
            if dictionary.get(args[0]):
                return str(args[1])
            elif len(args) >= 3:
                return str(args[2])
            else:
                return ''
        elif command == 'unless' and len(args) >= 2:
            if not dictionary.get(args[0]):
                return str(args[1])
            elif len(args) >= 3:
                return str(args[2])
            else:
                return ''
        elif (command == 'incr' or command == 'decr') and len(args) >= 1:
            try:
                func = getattr(self.store, command + '_' + target)
                set_func = getattr(self.store, 'set_' + target)
            except:
                log.msg('Data store is missing %s_%s or set_%s' % (command, target, target))
                return ''
            val = dictionary.get(args[0])
            if val:
                try:
                    func(args[0])
                    if command == 'incr':
                        dictionary[args[0]] = int(val) + 1
                    else:
                        dictionary[args[0]] = int(val) - 1
                except:
                    pass
            elif len(args) >= 2:
                set_func(args[0], args[1])
                dictionary[args[0]] = args[1]
            return ''
        else:
            log.msg('Invalid command: %s' % command)
            return expression    
