import logging
from pprint import pformat
from collections import namedtuple

from mongoquery import Query

log = logging.getLogger(__name__)
Change = namedtuple('Change', 'op lq ts obj')


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
        self._result_set = {}

    def log_entry(self, entry):
        log.info('CHANGE %s %s:\n%s', entry.op, entry.ns, pformat(entry.obj))

    def refresh(self, emit=False):
        old_result_ids = list(self._result_set)
        cursor = self._collection.find(self._qspec)
        results = {obj['_id']: obj for obj in cursor}
        if emit:
            for oid in old_result_ids:
                if oid not in results:
                    self._callback(Change('d', self, oid))
            for obj in results.values():
                self._callback(Change('a', self, obj))
        self._result_set = results

    def add(self, ts, obj):
        self._result_set[obj['_id']] = obj
        self._callback(Change('a', self, ts, obj))

    def discard(self, ts, oid):
        obj = self._result_set.pop(oid, None)
        if obj:
            self._callback(Change('d', self, ts, obj))

    def handle(self, entry):
        ts, ns, op, o2, o, obj = (
            entry['ts'], entry['ns'], entry['op'],
            entry.get('o2'), entry['o'], entry.get('obj'))
        if ns != self.ns:
            return
        if op == 'i':
            if self._query.match(o):
                return self.add(ts, o)
        elif op == 'd':
            log.info('DISCARD because DELETE')
            return self.discard(ts, o['_id'])
        elif entry['op'] == 'u':
            if self._query.match(obj):
                return self.add(ts, obj)
            else:
                log.debug(
                    'DISCARD because NOMATCH\nentry: %r\nqspec: %r',
                    entry, self._qspec)
                return self.discard(ts, o2['_id'])
