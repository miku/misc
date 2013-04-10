#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
vufmdump.py - dump binary MARC21 records from VuFind

(c) 2013, Leander Seige, seige@ub.uni-leipzig.de
released under the terms of GNU the General Public License (GPL) Version 3

Leipzig University Library

http://www.ub.uni-leipzig.de

"""

import argparse
import datetime
import math
import random
import re
import solr
import sys
import time


def dump(args):

    s = solr.SolrConnection(args.url)

    total_recs = 0
    file_recs = args.jsize
    file_num = 0
    file = sys.stdout

    time_start = time.time()

    while True:

        message = ""

        if args.delay:
            if args.paranoid:
                hfactor = datetime.datetime.now()
                random.seed(hfactor.second)
                hfactor = hfactor.hour
                hfactor = 2 + math.fabs(12 - hfactor)
                hfactor *= random.random()
                secs = hfactor + args.delay
            else:
                secs = args.delay
            message = '[sleeping %.2f sec]' % secs
            s.close()
            time.sleep(secs)
            s = solr.SolrConnection(args.url)

        response = s.query(args.query, start=total_recs, rows=args.qsize)

        junk_recs = 0

        for record in response:

            if args.base and file_recs == args.jsize:
                if file != sys.stdout:
                    file.close()
                file = open('%s.%08d.mrc' % (args.base, file_num), 'wb')
                file_num += 1
                file_recs = 0

            junk_recs += 1
            total_recs += 1
            file_recs += 1

            o = record['fullrecord'].strip()
            o = o.replace('#31;', '\x1f')
            o = o.replace('#30;', '\x1e')
            o = o.replace('#29;', '\x1d')
            o = o.encode('utf-8')
            file.write(o)

            if total_recs == args.limit:
                junk_recs = 0
                break

        time_end = time.time()
        time_lapse = time_end - time_start

        sys.stderr.write('\rdumping %s (%d) at %d rec/sec %s ' % (
            record['id'], total_recs, (total_recs/time_lapse), message))

        if junk_recs == 0:
            sys.stderr.write('\n')
            if file != sys.stdout:
                file.close()
            return total_recs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--url", "-u",
        default="http://localhost:8983/solr/biblio", help="Solr server URL")
    parser.add_argument("--query", "-q", default="title:lucene",
        help="Solr query, default is 'title:lucene'")
    parser.add_argument("--limit", "-l", default=100, type=int,
        help="max records, 0 = unlimited, default = 100")
    parser.add_argument("--qsize", "-qs", default=20, type=int,
        help="number of records per query, default = 20")
    parser.add_argument("--jsize", "-js", default=10000, type=int,
        help="number of records per file, default = 10000")
    parser.add_argument("--base", "-b",
        help="file output basename, number and .mrc extension will be appended automatically")
    parser.add_argument("--delay", "-d", type=float,
        help="delay in seconds between queries")
    parser.add_argument("--paranoid", "-p", action='store_true',
        help="use jittered delays")

    args = parser.parse_args()
    recs = dump(args)
