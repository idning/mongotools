#!/usr/bin/env python
#coding: utf-8
#file   : killslow.py
#author : ning
#date   : 2014-08-26 10:51:25

import os
import re
import sys
import time
import copy
import thread
import logging
import argparse

import bson
import pymongo

class MException(Exception):
    def __init__(self, msg=None):
        Exception.__init__(self, msg)

class Mongo(pymongo.Connection):
    '''
    a sub class of pymongo Connection, add auth support.
    '''
    def __init__(self, host, port, user=None, passwd=None, replset=None, timeout=1500):
        host_port = '%s:%s' % (host, port)
        logging.debug("init Mongo: host_port: %(host_port)s user: %(user)s passwd: %(passwd)s replset: %(replset)s" % locals())

        try:
            if replset:
                pymongo.Connection.__init__(self, host_port, replicaSet=replset, connectTimeoutMS=timeout, network_timeout=timeout)
            else:
                pymongo.Connection.__init__(self, host_port, connectTimeoutMS=timeout, network_timeout=timeout)
        except pymongo.errors.AutoReconnect, e:
            msg = '[MException] Connect to %(host_port)s Timeout ' % locals()
            logging.warning(msg)
            raise MException(msg)

        if not user:
            return

        db = self['admin']
        db.authenticate(user, passwd)

def main():
    parser= argparse.ArgumentParser()

    parser.add_argument('--host', required = True)
    parser.add_argument('--port', required = True)
    parser.add_argument('--user', default = '')
    parser.add_argument('--passwd', default = '')
    parser.add_argument('--kill', action='store_true')

    args = parser.parse_args()

    conn = Mongo(args.host, args.port, args.user, args.passwd)

    currentOP = conn.admin["$cmd.sys.inprog"].find_one({'$all': True })

    currentOP = currentOP['inprog']
    for op in currentOP:
        if not op['active']:
            continue

        if op['op'] not in ['query', 'getmore']:
            continue

        if 'secs_running' not in op:
            continue

        if op['secs_running'] < 10:
            continue

        if args.kill:
            print 'kill: ', op
            conn['admin']['$cmd.sys.killop'].find_one({'op': op['opid']})
        else:
            print op

if __name__ == "__main__":
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

