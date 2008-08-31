"""

    File: parser.py
    Description: 
    
        Config file parser.

    Author: Kyle Vogt
    Copyright (c) 2008, Justin.tv, Inc.
    
"""

from twisted.python import usage, log
import sys, traceback

def parse():
    "Parse conf file into a dict"
    try:
        options = Options()
        options.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.stdout.flush()
        sys.exit(1)
    try:
        # Read config file
        settings = {}
        data = file(options['config']).readlines()
        for line in data:
            args = line.split()
            if len(args) >= 2 and not line.startswith('#'):
                if args[0] not in settings:
                    val = args[1].strip()
                else:
                    val = settings[args[0]] + ',' + args[1].strip()
                if val.lower() in ['yes', 'true', 'on']: val = True
                elif val.lower() in ['no', 'false', 'off']: val = False
                settings[args[0]] = val
        for option, val in options.items():
            if val:
                settings[option] = val
        log.msg(settings)
        return settings
    except:
        print 'Unable to parse config file %s:' % options['config']
        traceback.print_exc()
        sys.stdout.flush()
        sys.exit(1)
        
class Options(usage.Options):
    optFlags = [
        ['verbose', 'v', 'Verbose mode'],
        ['daemon', 'd', 'Daemonize'],
    ]
    optParameters = [
        ['config', 'c', 'twice.conf', 'Config file'],
        ['log', 'l', '', 'Log file'],
        ['port', 'p', '', 'Port to listen on'],
        ['interface', 'i', '', 'Interface to bind to']
    ]
