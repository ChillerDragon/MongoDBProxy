import pymongo
from base64 import b64decode
from bson import BSON
import sys
from qexec.db import (
    setup_fin_db, setup_live_db, FinDbConnection, LiveDbConnection)

setup_fin_db()
setup_live_db()

fin = FinDbConnection.get()
tickplant = LiveDbConnection.get()
local = pymongo.MongoClient()['findb']

done = set()

doit = True
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
    if query['collection'][0:7] == 'system.':
        continue
    args = {}
    if query['method'] == 'find_one':
        query['limit'] = 1
    if query['sort']:
        args['sort'] = query['sort']
    args['spec'] = query['spec'] or {}
    if query['limit']:
        args['limit'] = query['limit'] 
    
    if query['collection'][0:26] == 'equity.trades.minute.live.':
        remote = tickplant
    else:
        remote = fin

    cursor = remote[query['collection']].find(**args)
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
