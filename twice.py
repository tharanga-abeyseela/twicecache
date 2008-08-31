import sys, os, signal, traceback, resource

__author__    = "Kyle Vogt <kyleavogt@gmail.com> and Emmett Shear <emmett.shear@gmail.com>"
__version__   = "0.2"
__copyright__ = "Copyright (c) 2008, Justin.tv, Inc."
__license__   = "MIT"        

def check_memory(limit):
    try:
        cpu, mem = [i.replace('\n', '') for i in os.popen('ps -p %s -o pcpu,rss' % os.getpid()).readlines()[1].split(' ') if i]
        real = int(mem) / 1000.0
        if real > limit:
            log.msg('Using too much memory (%.2fMB out of %.2fMB)' % (real, limit))
            os.kill(os.getpid(), signal.SIGTERM)
    except:
        log.msg('Unable to read memory or cpu usage!')
        traceback.print_exc()
    reactor.callLater(15.0, check_memory, limit)
    
if __name__ == '__main__':

    # Read config
    import parser
    config = parser.parse()
    
    # Log
    from twisted.python import log
    f = config['log']
    if f != 'stdout':
        log.startLogging(open(f, 'w'))
    else:
        log.startLogging(sys.stdout)

    # Set up reactor
    try:
        from twisted.internet import epollreactor
        epollreactor.install()
        log.msg('Using epoll')
    except:
        pass
    from twisted.internet import reactor  
    
    # Step up to maximum file descriptor limit
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        for i in xrange(16):
            test = 2 ** i
            if test > hard: break
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (test, hard))        
                val = test
            except ValueError:
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                val = soft
                break
        log.msg('%s file descriptors available (system max is %s)' % (val, hard))
    except:
        log.msg('Error setting fd limit!')
        traceback.print_exc()
        
    # Check memory usage
    check_memory(int(config.get('memory_limit', 100)))
        
    # Start request handler event loop
    import handler
    factory = handler.RequestHandler(config)
    reactor.listenTCP(int(config['port']), factory)
    
    #from twisted.manhole import telnet
    #shell = telnet.ShellFactory()
    #shell.username = 'admin'
    #shell.password = 'changeme'
    #try:
    #    reactor.listenTCP(4040, shell)
    #    log.msg('Telnet server running on port 4040.')
    #except:
    #    log.msg('Telnet server not running.')
        
    reactor.run()        
