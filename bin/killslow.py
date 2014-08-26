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

    parser.add_argument('-h', '--host')
    parser.add_argument('-p', '--port')
    parser.add_argument('-u', '--user', default = '')
    parser.add_argument('-P', '--passwd', default = '')

    args = parser.parse_args()

    conn = Mongo(args.host, args.port, args.user, args.passwd)

    currentOP = conn.admin["$cmd.sys.inprog"].find_one({'$all': True })
    currentOP = currentOP['inprog']

    for op in currentOP:
        print op

def main():
    """docstring for main"""
    logging.debug(PWD)

if __name__ == "__main__":
    common.parse_args2(LOGPATH)
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

