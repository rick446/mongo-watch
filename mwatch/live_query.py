import logging
from pprint import pformat
from collections import namedtuple

from mongoquery import Query

log = logging.getLogger(__name__)
Change = namedtuple('Change', 'op lq obj')


class LiveQuery(object):

    def __init__(self, collection, qspec, callback=None):
        if callback is None:
            callback = self.log_entry
        self._collection = collection
        self._qspec = qspec
        self._callback = callback
        self.ns = '{}.{}'.format(collection.database.name, collection.name)
        self._query = Query(qspec)
        if '_id' in qspec:
            self._query_by_id = Query({'_id': qspec['_id']})
        else:
            self._query_by_id = None
        self._result_ids = set()

    def log_entry(self, entry):
        log.info('CHANGE %s %s:\n%s', entry.op, entry.ns, pformat(entry.obj))

    def refresh(self):
        old_result_ids = list(self._result_ids)
        cursor = self._collection.find(self._qspec)
        results = {obj['_id']: obj for obj in cursor}
        for oid in old_result_ids:
            if oid not in results:
                self._callback(Change('d', self, oid))
        for obj in results.values():
            self._callback(Change('a', self, obj))
        self._result_ids = set(results)

    def add(self, obj):
        self._result_ids.add(obj['_id'])
        self._callback(Change('a', self, obj))

    def discard(self, oid):
        if oid in self._result_ids:
            self._result_ids.discard(oid)
            self._callback(Change('d', self, oid))

    def handle(self, entry):
        ns, op, o2, o, obj = (
            entry['ns'], entry['op'],
            entry.get('o2'), entry['o'], entry.get('obj'))
        if ns != self.ns:
            return
        if op == 'i':
            if self._query.match(o):
                return self.add(o)
        elif op == 'd':
            return self.discard(o['_id'])
        elif entry['op'] == 'u':
            if self._query.match(obj):
                return self.add(obj)
            else:
                return self.discard(o2['_id'])
