"""
Copyright 2013 Gustav Arngarden

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import time
import pymongo

from bson import BSON
from base64 import b64encode
import os
query_pid = None
query_file = None
query_pattern = '/tmp/query-log.%d'

def get_methods(*objs):
    return set(
        attr
        for obj in objs
        for attr in dir(obj)
        if not attr.startswith('_')
           and hasattr(getattr(obj, attr), '__call__')
    )

EXECUTABLE_MONGO_METHODS = get_methods(pymongo.collection.Collection,
                                       pymongo.Connection,
                                       pymongo)


class Executable:
    """ Wrap a MongoDB-method and handle AutoReconnect-exceptions
    using the safe_mongocall decorator.
    """

    def __init__(self, method, logger, wait_time=None):
        self.method = method
        self.logger = logger
        self.wait_time = wait_time or 60

    def __call__(self, *args, **kwargs):
        """ Automatic handling of AutoReconnect-exceptions.
        """
        start = time.time()
        i = 0
        while True:
            try:
                if self.method.__name__ in ('find', 'find_one') and \
                        self.method.im_self.database.name in ('tickplant', 'quanto_production_phys'):
                    global query_pid
                    global query_file
                    if os.getpid() != query_pid:
                        query_pid = os.getpid()
                        query_file = open(query_pattern % query_pid, 'w')
                    query = {'method': self.method.__name__,
                             'collection': self.method.im_self.name,
                             'spec': kwargs.get('spec', {}),
                             'limit': kwargs.get('limit', None),
                             'sort': kwargs.get('sort', None)}
                    query_file.write('# %s\n%s\n' %
                                     (query, b64encode(BSON.encode(query))))
                    query_file.flush()
                return self.method(*args, **kwargs)
            except pymongo.errors.AutoReconnect:
                end = time.time()
                delta = end - start
                if delta >= self.wait_time:
                    break
                self.logger.warning('AutoReconnecting, try %d (%.1f seconds)'
                                    % (i, delta))
                time.sleep(pow(2, i))
                i += 1
        # Try one more time, but this time, if it fails, let the
        # exception bubble up to the caller.
        return self.method(*args, **kwargs)

    def __dir__(self):
        return dir(self.method)

    def __str__(self):
        return self.method.__str__()

    def __repr__(self):
        return self.method.__repr__()

class MongoProxy:
    """ Proxy for MongoDB connection.
    Methods that are executable, i.e find, insert etc, get wrapped in an
    Executable-instance that handles AutoReconnect-exceptions transparently.

    """
    def __init__(self, conn, logger=None, wait_time=None):
        """ conn is an ordinary MongoDB-connection.

        """
        if logger is None:
            import logging
            logger = logging.getLogger(__name__)

        self.conn = conn
        self.logger = logger
        self.wait_time = wait_time


    def __getitem__(self, key):
        """ Create and return proxy around the method in the connection
        named "key".

        """
        item = self.conn[key]
        if hasattr(item, '__call__'):
            return MongoProxy(item, self.logger, self.wait_time)
        return item

    def __getattr__(self, key):
        """ If key is the name of an executable method in the MongoDB connection,
        for instance find or insert, wrap this method in Executable-class that
        handles AutoReconnect-Exception.

        """

        attr = getattr(self.conn, key)
        if hasattr(attr, '__call__'):
            if key in EXECUTABLE_MONGO_METHODS:
                return Executable(attr, self.logger, self.wait_time)
            else:
                return MongoProxy(attr, self.logger, self.wait_time)
        return attr

    def __call__(self, *args, **kwargs):
        return self.conn(*args, **kwargs)

    def __dir__(self):
        return dir(self.conn)

    def __str__(self):
        return self.conn.__str__()

    def __repr__(self):
        return self.conn.__repr__()

    def __nonzero__(self):
        return True
