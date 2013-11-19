import ctr
import pymongo
from base64 import b64decode
from bson import BSON
import sys

ctr.production()
fin = ctr.fin()
tickplant = ctr.live()
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
    else:
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

    if remote == tickplant:
        local[query['collection']].ensure_index([
            ('dt', pymongo.ASCENDING),
            ('sid', pymongo.DESCENDING)])
