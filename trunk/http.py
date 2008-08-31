"""

    File: http.py
    Description: 
    
        Twisted HTTP 1.0 library.

    Author: Kyle Vogt
    Copyright (c) 2008, Justin.tv, Inc.
    
"""

from twisted.python import log
from twisted.protocols import basic
from twisted.internet import protocol, defer
import traceback, urllib, time

messages = {
    200 : 'OK',
    400 : 'Bad Request',
    500 : 'Internal Server Error',
    501 : 'Not Implemented',
    502 : 'Bad Gateway',
    503 : 'Service Unavailable',
    504 : 'Gateway Timeout',
    505 : 'HTTP Version Not Supported',
}

class HTTPObject:    
    
    def __init__(self, id=None):
        self.id = id
        self.headers = {}
        self.cookies = []
        self.body = ''
        self.method = 'GET'
        self.mode = 'status'
        self.uri = ''
        self.protocol = 'HTTP/1.0'
        self.status = 200
        self.message = None
        self.element_cache = []
        self.response = None
        self.cacheable = False
        self.dependencies = []
        self.elements = {}
        self.received_on = None
        
    def setHeader(self, key, value=''):
        self.removeHeader(key)
        self.headers[key.lower()] = value
        
    def getHeader(self, key):
        for hkey, hval in self.headers.items():
            if key.lower() == hkey.lower():
                return hval
        return None
        
    def getCacheControlHeader(self, header='x-twice-control'):
        "Parse headers looking like 'x-twice-control: max-age=23423'"
        header = self.getHeader(header)
        if header:
            for element in header.split('; '):
                if '=' in element:
                    key, val = element.split('=')[0:2]
                    if key == 'max-age':
                        return int(val)
        return None
        
    def removeHeader(self, key):
        for k in list(self.headers.keys()):
            if key.lower() == k.lower():
                del self.headers[k]
        
    def addCookie(self, key, value, path='/'):
        self.cookies.append((key.lower(), value, path))
    
    def removeCookie(self, key):
        for cookie in list(self.cookies):
            k, v, path = cookie
            if key.lower() == k:
                cookies.remove(cookie)
                
    def getCookie(self, key):
        for cookie in self.cookies:
            parts = cookie.split('; ')[0].split('=')
            ckey, cval = parts[0], parts[1:]
            if ckey.lower() == key.lower():
                return '='.join(cval)
        return None
                
    def writeStatus(self):
        status_data = '%s %s %s\r\n' % (self.protocol, self.status, self.message or messages.get(self.status, 'ERROR'))
        return status_data
        
    def writeCommand(self):
        command_data = '%s %s %s\r\n' % (self.method, self.uri, self.protocol)
        return command_data

    def writeHeaders(self):
        header_data = ''.join(['%s: %s\r\n' % (k,v) for k,v in self.headers.items()])
        return header_data

    def writeCookies(self, key='set-cookie'):
        if not self.cookies: return ''
        if key.lower() == 'set-cookie':
            cookie_data = ''.join(['set-cookie: %s\r\n' % cookie for cookie in self.cookies])
        elif key.lower() == 'cookie':
            cookie_data = 'cookie: %s\r\n' % ('; '.join(self.cookies))
        return cookie_data
        
    def writeBody(self, body = None):
        self.setHeader('content-length', len(body or self.body))

    def writeResponse(self, body = None):
        self.writeBody(body or self.body)
        return ''.join([self.writeStatus(), self.writeHeaders(), self.writeCookies('set-cookie'), '\r\n', body or self.body])

    def writeRequest(self, body = None):
        self.writeBody()
        return ''.join([self.writeCommand(), self.writeHeaders(), self.writeCookies('cookie'), '\r\n', body or self.body])

        
class HTTPHandler(basic.LineReceiver):

    def __init__(self):
        self.max_headers = 100
        self.object_count = 0
        self.object = None 
        self.received_on = None
        
    def connectionMade(self):
        self.received_on = time.time()       
        
    def lineReceived(self, line):
        if not self.object:
            self.object = HTTPObject(self.object_count)
            self.object.received_on = self.received_on
            self.object_count += 1      
        if self.object.mode == 'status':
            try:
                parts = line.split()
                if parts[0].upper() in ['GET', 'PUT', 'POST', 'DELETE', 'HEAD']:
                    self.object.method, self.object.uri, self.object.protocol = parts
                    self.object.uri = self.object.uri
                else:
                    self.object.protocol = parts[0]
                    self.object.status = int(parts[1])
                    self.object.message = ' '.join(parts[2:])
                self.object.mode = 'headers'
                return
            except:
                log.msg("Bad line was: %s" % line)
                traceback.print_exc()
                try:
                    self.sendCode(400)
                except:
                    pass
                self.shutdown()
                return          
        elif self.object.mode == 'headers':
            if line != '':
                try:
                    key, value = line.split(': ')
                    if key.lower() == 'cookie':
                        new_cookies = value.split('; ')
                        self.object.cookies.extend(new_cookies)
                    elif key.lower() == 'set-cookie':
                        new_cookie = value
                        self.object.cookies.append(new_cookie)
                    else:
                        self.object.setHeader(key, value)
                except:
                    self.sendCode(400)
                    self.shutdown()
                    return
            else:
                length = self.object.getHeader('content-length')
                if length and int(length) > 0:
                    self.mode = 'body'
                    self.setRawMode()
                else:
                    self.factory.objectReceived(self, self.object)
                    self.object = None

    def rawDataReceived(self, data):
        "Process HTTP body data"
        self.object.body += data
        if len(self.object.body) == int(self.object.getHeader('content-length')):
            self.factory.objectReceived(self, self.object)
            self.object = None
            self.setLineMode()
            
    def shutdown(self):
        self.transport.loseConnection()
        
class HTTPServer(HTTPHandler):
    
    def __init__(self):
        HTTPHandler.__init__(self)
        
    def sendCode(self, code, body = ''):
        response = HTTPObject()
        response.status = int(code)
        response.body = body
        self.transport.write(response.writeResponse())
        self.shutdown()
        
class HTTPClient(HTTPHandler):
    
    def __init__(self):
        HTTPHandler.__init__(self)
    
    def connectionMade(self):
        data = self.factory.request.writeRequest()
        self.transport.write(data)
        
class HTTPRequestDispatcher(protocol.ServerFactory):
    
    protocol = HTTPServer
        
    def objectReceived(self, connection, request):
        "Override me"
        
class HTTPRequestSender(protocol.ClientFactory):
    
    protocol = HTTPClient
    
    def __init__(self, request):
        self.request = request
        self.deferred = defer.Deferred()
        
    def __repr__(self):
        return '<HTTPRequestSender (%s)>' % self.request.uri
    
    def objectReceived(self, connection, response):
        "Send the page!"
        self.deferred.callback(response)
