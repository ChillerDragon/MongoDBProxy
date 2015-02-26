import argparse
import pymongo
from base64 import b64decode
from bson import BSON
import sys
from qexec.db import (
    setup_fin_db, setup_live_db, FinDbConnection, LiveDbConnection)


def parse_args():
    parser = argparse.ArgumentParser(description='Mirror data from findb and '
                                     'tickplant into local database')
    parser.add_argument('--db-host', help='host[:port] for local database',
                        default='localhost')
    parser.add_argument('--db-name', help='Local database name',
                        default='findb')
    parser.add_argument('--wipe-collections', action='store_true',
                        default=False, help='Drop all documents in each '
                        'collection before saving any data into it')
    return parser.parse_args()

args = parse_args()

setup_fin_db()
setup_live_db()

fin = FinDbConnection.get()
tickplant = LiveDbConnection.get()
local = pymongo.MongoClient(args.db_host)[args.db_name]

done = set()

doit = True
collections_accessed = set()

if args.wipe_collections:
    for collection in local.collection_names():
        if collection.startswith('system.'):
            continue
        local[collection].remove()

for line in open('/tmp/queries', 'r'):
    doit = not doit
    if not doit:
        continue
    if line in done:
        continue
    done.add(line)
    try:
        query = BSON(b64decode(line)).decode()
    except:
        print line
        raise
    if query['collection'].startswith('system.'):
        continue
    kwargs = {}
    if query['method'] == 'find_one':
        query['limit'] = 1
    if query['sort']:
        kwargs['sort'] = query['sort']
    kwargs['spec'] = query['spec'] or {}
    if query['limit']:
        kwargs['limit'] = query['limit']

    if query['collection'][0:26] == 'equity.trades.minute.live.':
        remote = tickplant
    else:
        remote = fin

    cursor = remote[query['collection']].find(**kwargs)
    sys.stderr.write('%s %d: ' % (query['collection'], cursor.count()))
    for doc in cursor:
        local[query['collection']].save(doc)
        sys.stderr.write('.')
    sys.stderr.write('\n')

    # MongoDBProxy objects don't like being compared with ==. Rather
    # than waste time figuring out why that is, just compare their
    # id's instead.
    if id(remote) == id(tickplant):
        local[query['collection']].ensure_index([
            ('dt', pymongo.ASCENDING),
            ('sid', pymongo.DESCENDING)])
